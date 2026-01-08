"""
Confluence Content Copier - Copy Engine

Orchestrates the copying of content from source to destination Confluence.
"""

import json
import logging
import re
import csv
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, field, asdict

from .client import ConfluenceClient, ConfluenceAPIError
from .models import (
    Page, Space, PageNode, ChildRef, Attachment,
    CreatePageRequest, UpdatePageRequest,
)


logger = logging.getLogger(__name__)


@dataclass
class CopyStats:
    """Statistics for a copy operation."""
    pages_found: int = 0
    pages_created: int = 0
    pages_updated: int = 0
    pages_skipped: int = 0
    pages_unchanged: int = 0  # Skipped due to identical content
    pages_failed: int = 0
    
    # Attachment statistics
    attachments_found: int = 0
    attachments_downloaded: int = 0
    attachments_uploaded: int = 0
    attachments_skipped: int = 0
    attachments_unchanged: int = 0  # Skipped due to same file
    attachments_failed: int = 0
    
    @property
    def pages_processed(self) -> int:
        return self.pages_created + self.pages_updated + self.pages_skipped
    
    def summary(self) -> str:
        result = (
            f"Found: {self.pages_found}, "
            f"Created: {self.pages_created}, "
            f"Updated: {self.pages_updated}, "
            f"Skipped: {self.pages_skipped}, "
            f"Unchanged: {self.pages_unchanged}, "
            f"Failed: {self.pages_failed}"
        )
        if self.attachments_found > 0:
            result += (
                f" | Attachments - "
                f"Found: {self.attachments_found}, "
                f"Uploaded: {self.attachments_uploaded}, "
                f"Skipped: {self.attachments_skipped}, "
                f"Unchanged: {self.attachments_unchanged}, "
                f"Failed: {self.attachments_failed}"
            )
        return result


@dataclass
class CopyState:
    """
    Persistent state for idempotent copy operations.
    
    Stores mapping of source page IDs to destination page IDs.
    """
    last_run: Optional[str] = None
    source_space_key: Optional[str] = None
    dest_space_key: Optional[str] = None
    page_mapping: Dict[str, str] = field(default_factory=dict)
    attachment_mapping: Dict[str, str] = field(default_factory=dict)  # NEW
    
    @classmethod
    def load(cls, path: str = ".confluence_copy_state.json") -> "CopyState":
        """Load state from file."""
        try:
            with open(path, "r") as f:
                data = json.load(f)
                return cls(
                    last_run=data.get("last_run"),
                    source_space_key=data.get("source_space_key"),
                    dest_space_key=data.get("dest_space_key"),
                    page_mapping=data.get("page_mapping", {}),
                    attachment_mapping=data.get("attachment_mapping", {}),  # NEW
                )
        except FileNotFoundError:
            logger.info(f"No existing state file at {path}")
            return cls()
        except json.JSONDecodeError as e:
            logger.warning(f"Failed to parse state file: {e}")
            return cls()
    
    def save(self, path: str = ".confluence_copy_state.json") -> None:
        """Save state to file."""
        self.last_run = datetime.utcnow().isoformat() + "Z"
        with open(path, "w") as f:
            json.dump(asdict(self), f, indent=2)
        logger.info(f"Saved state to {path}")
    
    def get_dest_id(self, source_id: str) -> Optional[str]:
        """Get destination page ID for a source page ID."""
        return self.page_mapping.get(source_id)
    
    def set_mapping(self, source_id: str, dest_id: str) -> None:
        """Record a source -> dest page mapping."""
        self.page_mapping[source_id] = dest_id
    
    def get_dest_attachment_id(self, source_att_id: str) -> Optional[str]:
        """Get destination attachment ID for a source attachment ID."""
        return self.attachment_mapping.get(source_att_id)
    
    def set_attachment_mapping(self, source_att_id: str, dest_att_id: str) -> None:
        """Record a source -> dest attachment mapping."""
        self.attachment_mapping[source_att_id] = dest_att_id


class CopyEngine:
    """
    Engine for copying content from source to destination Confluence.
    
    Supports:
    - Copying entire spaces
    - Copying page trees from a root page
    - Preserving page hierarchy
    - Dry-run mode
    - Idempotent re-runs via state tracking
    """
    
    def __init__(
        self,
        source_client: ConfluenceClient,
        dest_client: ConfluenceClient,
        dry_run: bool = True,
        conflict_handling: str = "skip",
        state_file: str = ".confluence_copy_state.json",
        copy_attachments: bool = False,
        max_attachment_size: int = 52428800,  # 50 MB
        skip_large_attachments: bool = True,
        max_tree_depth: int = 0,  # 0 = unlimited
        create_space_if_missing: bool = False,
    ):
        """
        Initialize the copy engine.
        
        Args:
            source_client: ConfluenceClient for source (should be read-only)
            dest_client: ConfluenceClient for destination
            dry_run: If True, don't perform actual writes
            conflict_handling: How to handle existing pages ("skip", "update", "error")
            state_file: Path to state file for idempotency
            copy_attachments: If True, copy attachments with pages
            max_attachment_size: Maximum attachment size in bytes (0 = unlimited)
            skip_large_attachments: Skip large attachments vs error
            max_tree_depth: Maximum depth for tree copy (0 = unlimited)
        """
        self.source = source_client
        self.dest = dest_client
        self.dry_run = dry_run
        self.conflict_handling = conflict_handling
        self.state_file = state_file
        self.copy_attachments = copy_attachments
        self.max_attachment_size = max_attachment_size
        self.skip_large_attachments = skip_large_attachments
        
        # Link logging
        self.external_links_log = "external_links.csv"
        self._init_link_log()
        
        self.max_tree_depth = max_tree_depth
        self.create_space_if_missing = create_space_if_missing
        
        self.state = CopyState.load(state_file)
        self.stats = CopyStats()
        
        # Cache for destination page lookups
        self._dest_pages_cache: Dict[str, Page] = {}
        
        if dry_run:
            logger.info("Running in DRY-RUN mode - no changes will be made")
        if copy_attachments:
            logger.info("Attachment copying enabled")
    
    def copy_space(
        self,
        source_space_key: str,
        dest_space_key: str,
        max_pages: int = 0,
        force: bool = False,
    ) -> CopyStats:
        """
        Copy all pages from a source space to a destination space.
        
        Args:
            source_space_key: Source space key (e.g., "DOCS")
            dest_space_key: Destination space key (e.g., "DOCS-COPY")
            max_pages: Maximum pages to copy (0 = unlimited)
            force: If True, process all pages regardless of existence in destination
            
        Returns:
            CopyStats with operation results
        """
        logger.info(f"Starting space copy: {source_space_key} -> {dest_space_key}")
        
        # Reset stats
        self.stats = CopyStats()
        
        # Resolve spaces
        source_space = self.source.get_space_by_key(source_space_key)
        if not source_space:
            raise ValueError(f"Source space not found: {source_space_key}")
        
        dest_space = self.dest.get_space_by_key(dest_space_key)
        if not dest_space:
            if self.create_space_if_missing:
                logger.info(f"Destination space '{dest_space_key}' not found. Creating...")
                
                if self.dry_run:
                    logger.info("  [DRY-RUN] Would create space")
                    # Fake space object for subsequent operations
                    dest_space = Space(id="dry-run-id", key=dest_space_key, name=f"{source_space.name} (Copy)", type="global")
                else:
                    try:
                        dest_space = self.dest.create_space(dest_space_key, source_space.name)
                        logger.info(f"  Created space: {dest_space.name} (ID: {dest_space.id})")
                    except Exception as e:
                        raise ConfluenceAPIError(f"Failed to create space: {e}")
            else:
                raise ValueError(f"Destination space '{dest_space_key}' not found (use --create-space to create it)")
        
        logger.info(f"Source space: {source_space.name} (ID: {source_space.id})")
        logger.info(f"Dest space: {dest_space.name} (ID: {dest_space.id})")
        
        # Update state
        self.state.source_space_key = source_space_key
        self.state.dest_space_key = dest_space_key
        
        # Fetch all pages from source
        logger.info("Fetching pages from source space...")
        pages = self._fetch_space_pages(source_space.id, dest_space.id, max_pages, force)
        self.stats.pages_found = len(pages)
        logger.info(f"Found {len(pages)} pages in source space")
        
        if not pages:
            logger.warning("No pages found in source space")
            return self.stats
        
        # Build page tree
        logger.info("Building page tree...")
        root_nodes, page_index = self._build_page_tree(pages)
        logger.info(f"Built tree with {len(root_nodes)} root pages")
        
        # Fetch attachments if enabled
        if self.copy_attachments:
            self._fetch_attachments_for_pages(page_index)
        
        # Copy pages in dependency order
        logger.info("Copying pages...")
        self._copy_tree(root_nodes, dest_space.id, page_index)
        
        # Save state
        if not self.dry_run:
            self.state.save(self.state_file)
        
        logger.info(f"Copy complete. {self.stats.summary()}")
        return self.stats
    
    def copy_page_tree(
        self,
        source_page_id: str,
        dest_space_key: str,
        dest_parent_id: Optional[str] = None,
    ) -> CopyStats:
        """
        Copy a page and all its descendants to a destination space.
        
        Args:
            source_page_id: Source root page ID
            dest_space_key: Destination space key
            dest_parent_id: Optional parent page ID in destination
            
        Returns:
            CopyStats with operation results
        """
        logger.info(f"Starting page tree copy: page {source_page_id} -> space {dest_space_key}")
        
        # Reset stats
        self.stats = CopyStats()
        
        # Get source page
        source_page = self.source.get_page_by_id(source_page_id, include_body=True)
        logger.info(f"Source page: {source_page.title}")
        
        # Resolve destination space
        dest_space = self.dest.get_space_by_key(dest_space_key)
        if not dest_space:
            if self.create_space_if_missing:
                logger.info(f"Destination space '{dest_space_key}' not found. Creating...")
                
                if self.dry_run:
                    logger.info("  [DRY-RUN] Would create space")
                    # Fake space object for subsequent operations
                    dest_space = Space(id="dry-run-id", key=dest_space_key, name=f"{source_space.name} (Copy)", type="global")
                else:
                    try:
                        dest_space = self.dest.create_space(dest_space_key, source_space.name)
                        logger.info(f"  Created space: {dest_space.name} (ID: {dest_space.id})")
                    except Exception as e:
                        raise ConfluenceAPIError(f"Failed to create space: {e}")
            else:
                raise ValueError(f"Destination space '{dest_space_key}' not found (use --create-space to create it)")
        
        # Build tree from source page
        logger.info("Fetching page tree from source...")
        pages = self._fetch_page_tree(source_page_id)
        self.stats.pages_found = len(pages)
        logger.info(f"Found {len(pages)} pages in tree")
        
        # Build page tree
        root_nodes, page_index = self._build_page_tree(pages)
        
        # Set custom parent for root if specified
        if dest_parent_id and root_nodes:
            # The root node's parent will be the specified dest_parent_id
            pass  # Handled in _copy_tree
        
        # Fetch attachments if enabled
        if self.copy_attachments:
            self._fetch_attachments_for_pages(page_index)
        
        # Copy pages
        logger.info("Copying pages...")
        self._copy_tree(root_nodes, dest_space.id, page_index, custom_parent=dest_parent_id)
        
        # Save state
        if not self.dry_run:
            self.state.save(self.state_file)
        
        logger.info(f"Copy complete. {self.stats.summary()}")
        return self.stats
    
    def _fetch_space_pages(self, source_space_id: str, dest_space_id: str, max_pages: int = 0, force: bool = False) -> List[Page]:
        """
        Fetch pages that don't exist in destination yet (Diff Sync).
        
        Args:
            source_space_id: Source Space ID
            dest_space_id: Destination Space ID
            max_pages: Max pages to fetch body for (0 = unlimited)
            force: If True, fetch all pages regardless of existence in destination
        """
        logger.info("Fetching source page list (metadata)...")
        source_pages = list(self.source.list_pages_in_space(source_space_id))
        
        if force:
            # Force mode: process all source pages, but skip ones already in state file
            # This allows batch looping to make progress
            already_copied_ids = set(self.state.page_mapping.keys())
            candidates = [p for p in source_pages if p.id not in already_copied_ids]
            logger.info(f"Force mode: Processing all {len(source_pages)} source pages ({len(already_copied_ids)} already copied, {len(candidates)} remaining)")
        else:
            # Diff mode: only process missing pages
            logger.info("Fetching destination page list (metadata)...")
            dest_pages = list(self.dest.list_pages_in_space(dest_space_id))
            dest_titles = {p.title for p in dest_pages}
            
            # Filter for missing pages
            candidates = [p for p in source_pages if p.title not in dest_titles]
            logger.info(f"Found {len(source_pages)} source pages, {len(dest_pages)} dest pages. Missing: {len(candidates)}")
        
        if not candidates:
            return []
            
        # Apply limit
        if max_pages > 0:
            batch = candidates[:max_pages]
            logger.info(f"Processing batch of {len(batch)} pages (limit: {max_pages})")
        else:
            batch = candidates
            logger.info(f"Processing all {len(batch)} missing pages")
            
        # Hydrate bodies
        results = []
        for i, page in enumerate(batch):
            try:
                logger.debug(f"Fetching body for {page.title} ({i+1}/{len(batch)})")
                full_page = self.source.get_page_by_id(page.id, include_body=True)
                results.append(full_page)
            except Exception as e:
                logger.error(f"Failed to fetch body for {page.title}: {e}")
                
        return results
    
    def _fetch_page_tree(self, root_page_id: str) -> List[Page]:
        """Recursively fetch a page and its descendants up to max_tree_depth."""
        pages = []
        max_depth = self.max_tree_depth
        
        def fetch_recursive(page_id: str, current_depth: int):
            page = self.source.get_page_by_id(page_id, include_body=True)
            pages.append(page)
            
            # Check depth limit (0 means unlimited)
            if max_depth > 0 and current_depth >= max_depth:
                logger.debug(f"Reached max depth {max_depth} at page {page.title}")
                return
            
            for child_ref in self.source.get_page_children(page_id):
                if child_ref.type == "page":
                    fetch_recursive(child_ref.id, current_depth + 1)
        
        fetch_recursive(root_page_id, current_depth=0)
        return pages
    
    def _build_page_tree(
        self, 
        pages: List[Page]
    ) -> Tuple[List[PageNode], Dict[str, PageNode]]:
        """
        Build an in-memory tree structure from a list of pages.
        
        Returns:
            Tuple of (root_nodes, page_index)
        """
        # Create nodes for all pages
        page_index: Dict[str, PageNode] = {}
        for page in pages:
            node = PageNode(
                source_id=page.id,
                title=page.title,
                body_storage=page.body_storage or "",
                parent_source_id=page.parent_id,
                labels=[label.name for label in page.labels],
            )
            
            # Extract external links
            self._extract_external_links(page.id, page.title, page.body_storage)
            
            page_index[page.id] = node
        
        # Build parent-child relationships
        root_nodes: List[PageNode] = []
        for node in page_index.values():
            if node.parent_source_id and node.parent_source_id in page_index:
                parent = page_index[node.parent_source_id]
                parent.add_child(node)
            else:
                root_nodes.append(node)
        
        return root_nodes, page_index
    
    def _fetch_attachments_for_pages(self, page_index: Dict[str, PageNode]) -> None:
        """
        Fetch attachments for all pages in the tree.
        
        Populates the 'attachments' field of each PageNode.
        """
        if not self.copy_attachments:
            return
        
        logger.info(f"Fetching attachments for {len(page_index)} pages...")
        
        for page_id, node in page_index.items():
            try:
                attachments = list(self.source.list_page_attachments(page_id))
                node.attachments = attachments
                self.stats.attachments_found += len(attachments)
                
                if attachments:
                    logger.debug(f"  {node.title}: {len(attachments)} attachments")
            except Exception as e:
                logger.warning(f"Failed to fetch attachments for {node.title}: {e}")
    
    def _copy_attachments_for_page(self, node: PageNode, dest_page_id: str) -> None:
        """
        Copy all attachments from source page to destination page.
        """
        if not node.attachments:
            return
        
        logger.info(f"  Copying {len(node.attachments)} attachments for {node.title}")
        
        for attachment in node.attachments:
            try:
                # Check size limit
                if self.max_attachment_size > 0 and attachment.file_size > self.max_attachment_size:
                    size_mb = attachment.file_size / (1024 * 1024)
                    limit_mb = self.max_attachment_size / (1024 * 1024)
                    
                    if self.skip_large_attachments:
                        logger.warning(
                            f"    Skipping large attachment: {attachment.title} "
                            f"({size_mb:.1f} MB > {limit_mb:.1f} MB limit)"
                        )
                        node.attachments_skipped += 1
                        self.stats.attachments_skipped += 1
                        continue
                    else:
                        raise ValueError(
                            f"Attachment {attachment.title} exceeds size limit"
                        )
                
                # Check if already copied (idempotency)
                existing_dest_id = self.state.get_dest_attachment_id(attachment.id)
                if existing_dest_id:
                    logger.debug(f"    Already copied: {attachment.title}")
                    node.attachments_skipped += 1
                    self.stats.attachments_skipped += 1
                    continue
                
                # Smart sync: Check if attachment already exists in destination
                if not self.dry_run:
                    try:
                        dest_attachments = list(self.dest.list_page_attachments(dest_page_id))
                        existing_att = next((a for a in dest_attachments if a.title == attachment.title), None)
                        
                        if existing_att:
                            # Attachment exists - compare file size
                            if existing_att.file_size == attachment.file_size:
                                logger.info(f"    Unchanged: {attachment.title} (same size, skipping)")
                                node.attachments_skipped += 1
                                self.stats.attachments_unchanged += 1
                                # Track in state to avoid re-checking
                                self.state.set_attachment_mapping(attachment.id, existing_att.id)
                                continue
                            else:
                                # Different size - need to update
                                logger.info(f"    Updating: {attachment.title} (size changed: {existing_att.file_size} -> {attachment.file_size})")
                                # Note: Confluence API doesn't support updating attachments directly
                                # We would need to delete and re-upload, but that's risky
                                # For now, skip and log as failed
                                logger.warning(f"    Cannot update existing attachment (API limitation)")
                                node.attachments_failed += 1
                                self.stats.attachments_failed += 1
                                continue
                    except Exception as e:
                        logger.debug(f"    Could not check existing attachments: {e}")
                        # Continue with upload attempt
                
                # Dry-run mode
                if self.dry_run:
                    logger.info(f"    [DRY-RUN] Would copy: {attachment.title} ({attachment.file_size} bytes)")
                    node.attachments_copied += 1
                    self.stats.attachments_uploaded += 1
                    continue
                
                # Download from source
                logger.debug(f"    Downloading: {attachment.title}")
                file_content = self.source.download_attachment(attachment)
                self.stats.attachments_downloaded += 1
                
                # Upload to destination
                logger.debug(f"    Uploading: {attachment.title}")
                uploaded_att = self.dest.upload_attachment(
                    page_id=dest_page_id,
                    filename=attachment.title,
                    file_content=file_content,
                    comment=f"Copied from source (original ID: {attachment.id})",
                )
                
                # Track mapping
                self.state.set_attachment_mapping(attachment.id, uploaded_att.id)
                
                node.attachments_copied += 1
                self.stats.attachments_uploaded += 1
                
                logger.info(f"    ✓ Copied: {attachment.title}")
                
            except Exception as e:
                node.attachments_failed += 1
                self.stats.attachments_failed += 1
                logger.error(f"    ✗ Failed to copy {attachment.title}: {e}")
    
    def _copy_tree(
        self,
        root_nodes: List[PageNode],
        dest_space_id: str,
        page_index: Dict[str, PageNode],
        custom_parent: Optional[str] = None,
    ) -> None:
        """
        Copy a tree of pages to the destination.
        
        Processes pages in dependency order (parents before children).
        """
        def copy_node(
            node: PageNode, 
            dest_parent_id: Optional[str] = None
        ) -> Optional[str]:
            """Copy a single node and return the destination page ID."""
            
            # Check if already copied in previous run
            existing_dest_id = self.state.get_dest_id(node.source_id)
            if existing_dest_id:
                logger.info(f"  Already copied: {node.title} -> {existing_dest_id}")
                node.dest_id = existing_dest_id
                node.copied = True
                self.stats.pages_skipped += 1
                return existing_dest_id
            
            # Check if page with same title exists in destination
            existing_page = self._find_dest_page(dest_space_id, node.title, dest_parent_id)
            
            if existing_page:
                if self.conflict_handling == "skip":
                    logger.info(f"  Skipping (exists): {node.title}")
                    node.dest_id = existing_page.id
                    node.copied = True
                    self.state.set_mapping(node.source_id, existing_page.id)
                    self.stats.pages_skipped += 1
                    return existing_page.id
                    
                elif self.conflict_handling == "update":
                    # Smart sync: Check if content is actually different
                    # Need to fetch full page content if not already loaded
                    if not existing_page.body_storage:
                        existing_page = self.dest.get_page_by_id(existing_page.id, include_body=True)
                    
                    if self._content_matches(node.body_storage, existing_page.body_storage):
                        logger.info(f"  Unchanged: {node.title} (content identical, skipping update)")
                        node.dest_id = existing_page.id
                        node.copied = True
                        self.state.set_mapping(node.source_id, existing_page.id)
                        self.stats.pages_unchanged += 1
                        
                        # Still copy attachments if enabled, even if page content is unchanged
                        if self.copy_attachments and node.attachments:
                            self._copy_attachments_for_page(node, existing_page.id)
                        
                        return existing_page.id
                    else:
                        return self._update_page(node, existing_page, dest_space_id)
                    
                else:  # error
                    raise ValueError(
                        f"Page already exists: {node.title} (ID: {existing_page.id})"
                    )
            else:
                return self._create_page(node, dest_space_id, dest_parent_id)
        
        def copy_recursive(node: PageNode, parent_dest_id: Optional[str] = None):
            """Recursively copy a node and its children."""
            dest_id = copy_node(node, parent_dest_id)
            
            for child in node.children:
                copy_recursive(child, dest_id)
        
        # Process all root nodes
        for root_node in root_nodes:
            # Handle "Batch Roots" - nodes that have a parent in source, but parent wasn't in this batch
            current_parent_id = custom_parent
            
            if current_parent_id is None and root_node.parent_source_id:
                # 1. Check state mapping first (fast)
                mapped_parent = self.state.get_dest_id(root_node.parent_source_id)
                if mapped_parent:
                    current_parent_id = mapped_parent
                else:
                    # 2. Lookup parent in destination by title (slow but robust)
                    try:
                        src_parent = self.source.get_page_by_id(root_node.parent_source_id)
                        dest_parent_page = self._find_dest_page(dest_space_id, src_parent.title)
                        if dest_parent_page:
                            current_parent_id = dest_parent_page.id
                    except Exception as e:
                        logger.warning(f"Could not resolve parent for {root_node.title}: {e}")
            
            copy_recursive(root_node, current_parent_id)
    
    def _content_matches(self, source_content: str, dest_content: str) -> bool:
        """
        Compare two page contents to determine if they are identical.
        Uses simple string comparison for now.
        """
        # Handle None values
        if source_content is None or dest_content is None:
            return False
        # Strip whitespace for comparison to handle minor formatting differences
        return source_content.strip() == dest_content.strip()
    
    def _find_dest_page(
        self, 
        space_id: str, 
        title: str,
        parent_id: Optional[str] = None,
    ) -> Optional[Page]:
        """Find a page in the destination by title."""
        # Skip lookup if this is a dry-run created space
        if space_id == "dry-run-id":
            return None

        # Simple linear search - could be optimized with caching
        for page in self.dest.list_pages_in_space(space_id):
            if page.title == title:
                if parent_id is None or page.parent_id == parent_id:
                    return page
        return None
    
    def _create_page(
        self,
        node: PageNode,
        dest_space_id: str,
        dest_parent_id: Optional[str] = None,
    ) -> Optional[str]:
        """Create a new page on the destination."""
        if self.dry_run:
            logger.info(f"  [DRY-RUN] Would create: {node.title}")
            self.stats.pages_created += 1
            return None
        
        try:
            request = CreatePageRequest(
                space_id=dest_space_id,
                title=node.title,
                body_value=node.body_storage,
                parent_id=dest_parent_id,
            )
            
            created_page = self.dest.create_page(request)
            
            node.dest_id = created_page.id
            node.copied = True
            self.state.set_mapping(node.source_id, created_page.id)
            self.stats.pages_created += 1
            
            logger.info(f"  Created: {node.title} -> {created_page.id}")
            
            # Copy attachments after page is created
            if self.copy_attachments and node.attachments:
                self._copy_attachments_for_page(node, created_page.id)
            
            return created_page.id
            
        except ConfluenceAPIError as e:
            node.error = str(e)
            self.stats.pages_failed += 1
            logger.error(f"  Failed to create {node.title}: {e}")
            return None
    
    def _update_page(
        self,
        node: PageNode,
        existing_page: Page,
        dest_space_id: str,
    ) -> Optional[str]:
        """Update an existing page on the destination."""
        if self.dry_run:
            logger.info(f"  [DRY-RUN] Would update: {node.title}")
            self.stats.pages_updated += 1
            return existing_page.id
        
        try:
            # Get current version
            current_page = self.dest.get_page_by_id(existing_page.id)
            current_version = current_page.version.number if current_page.version else 1
            
            request = UpdatePageRequest(
                page_id=existing_page.id,
                title=node.title,
                body_value=node.body_storage,
                version_number=current_version + 1,
                version_message="Updated via Confluence Copier",
            )
            
            updated_page = self.dest.update_page(request)
            
            node.dest_id = updated_page.id
            node.copied = True
            self.state.set_mapping(node.source_id, updated_page.id)
            self.stats.pages_updated += 1
            
            logger.info(f"  Updated: {node.title} -> {updated_page.id}")
            
            # Copy attachments after page is updated
            if self.copy_attachments and node.attachments:
                self._copy_attachments_for_page(node, updated_page.id)
            
            return updated_page.id
            
        except ConfluenceAPIError as e:
            node.error = str(e)
            self.stats.pages_failed += 1
            logger.error(f"  Failed to update {node.title}: {e}")
            return None
    def _init_link_log(self) -> None:
        """Initialize the external links log file."""
        if not self.dry_run:
            try:
                with open(self.external_links_log, "w", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    writer.writerow(["Page ID", "Page Title", "External URL"])
            except Exception as e:
                logger.warning(f"Failed to initialize link log: {e}")

    def _extract_external_links(self, page_id: str, title: str, content: str) -> None:
        """
        Extract external links from page content and log them.
        """
        if self.dry_run or not content:
            return

        try:
            # simple regex for href="..."
            # This is not HTML-parser perfect but sufficient for Confluence storage format
            # Matches href="url" or href='url'
            links = re.findall(r'href=["\']([^"\']+)["\']', content)
            
            external_links = set()
            source_domain = self.source.base_url.split("/wiki")[0]
            
            for link in links:
                # Filter out internal/relative links
                if link.startswith("/"): 
                    continue
                if link.startswith("#"):
                    continue
                if source_domain in link:
                    continue
                if "atlassian.net/wiki" in link and link.startswith(source_domain):
                    continue
                    
                # Must be http/https
                if link.startswith("http://") or link.startswith("https://"):
                    external_links.add(link)
            
            if external_links:
                with open(self.external_links_log, "a", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    for link in external_links:
                        writer.writerow([page_id, title, link])
                        
        except Exception as e:
            logger.warning(f"Failed to extract links for {title}: {e}")
