#!/usr/bin/env python3
"""
Confluence Content Copier

A tool for copying content from one Confluence Cloud instance to another.

Usage:
    # Copy a space (dry-run by default)
    python copy_confluence.py copy-space --source-key DOCS --dest-key DOCS-COPY
    
    # Copy with actual execution
    python copy_confluence.py copy-space --source-key DOCS --dest-key DOCS-COPY --execute
    
    # Copy a page tree
    python copy_confluence.py copy-tree --page-id 12345 --dest-key DOCS-COPY --execute
    
    # Test connections
    python copy_confluence.py test-connection
    
    # List spaces
    python copy_confluence.py list-spaces --target source
"""

import argparse
import logging
import sys
import time
from typing import Optional

from src.client import ConfluenceClient, ConfluenceAPIError, ReadOnlyViolationError
from src.copier import CopyEngine
from src.utils import (
    load_app_config, 
    setup_logging, 
    validate_config,
    AppConfig,
)


logger = logging.getLogger(__name__)


def create_clients(config: AppConfig) -> tuple[ConfluenceClient, ConfluenceClient]:
    """Create source and destination clients from config."""
    source_client = ConfluenceClient(
        base_url=config.source.base_url,
        user_email=config.source.user_email,
        api_token=config.source.api_token,
        read_only=True,  # Source is always read-only
        api_delay=config.api_delay,
    )
    
    dest_client = ConfluenceClient(
        base_url=config.destination.base_url,
        user_email=config.destination.user_email,
        api_token=config.destination.api_token,
        read_only=False,  # Destination allows writes
        api_delay=config.api_delay,
    )
    
    return source_client, dest_client


def cmd_test_connection(args: argparse.Namespace, config: AppConfig) -> int:
    """Test connections to source and/or destination."""
    errors = []
    
    # Test source
    if args.target in ("source", "both"):
        print(f"Testing source connection: {config.source.base_url}")
        try:
            with ConfluenceClient(
                base_url=config.source.base_url,
                user_email=config.source.user_email,
                api_token=config.source.api_token,
                read_only=True,
            ) as client:
                client.test_connection()
                print("  ✓ Source connection successful")
        except ConfluenceAPIError as e:
            print(f"  ✗ Source connection failed: {e}")
            errors.append("source")
    
    # Test destination
    if args.target in ("destination", "both"):
        print(f"Testing destination connection: {config.destination.base_url}")
        try:
            with ConfluenceClient(
                base_url=config.destination.base_url,
                user_email=config.destination.user_email,
                api_token=config.destination.api_token,
                read_only=True,
            ) as client:
                client.test_connection()
                print("  ✓ Destination connection successful")
        except ConfluenceAPIError as e:
            print(f"  ✗ Destination connection failed: {e}")
            errors.append("destination")
    
    if errors:
        print(f"\nConnection test failed for: {', '.join(errors)}")
        return 1
    
    print("\nAll connection tests passed!")
    return 0


def cmd_list_spaces(args: argparse.Namespace, config: AppConfig) -> int:
    """List spaces on source or destination."""
    if args.target == "source":
        client_config = config.source
    else:
        client_config = config.destination
    
    print(f"Listing spaces from {args.target}: {client_config.base_url}")
    print()
    
    try:
        with ConfluenceClient(
            base_url=client_config.base_url,
            user_email=client_config.user_email,
            api_token=client_config.api_token,
            read_only=True,
        ) as client:
            spaces = list(client.list_spaces(limit=args.limit))
            
            if not spaces:
                print("No spaces found.")
                return 0
            
            # Print table header
            print(f"{'ID':<15} {'Key':<15} {'Name':<50} {'Type':<10}")
            print("-" * 90)
            
            for space in spaces:
                name = space.name[:47] + "..." if len(space.name) > 50 else space.name
                print(f"{space.id:<15} {space.key:<15} {name:<50} {space.type:<10}")
            
            print()
            print(f"Total: {len(spaces)} spaces")
            
    except ConfluenceAPIError as e:
        print(f"Error: {e}")
        return 1
    
    return 0


def cmd_list_pages(args: argparse.Namespace, config: AppConfig) -> int:
    """List pages in a space."""
    if args.target == "source":
        client_config = config.source
    else:
        client_config = config.destination
    
    print(f"Listing pages in space '{args.space_key}' from {args.target}")
    print()
    
    try:
        with ConfluenceClient(
            base_url=client_config.base_url,
            user_email=client_config.user_email,
            api_token=client_config.api_token,
            read_only=True,
        ) as client:
            # Find space by key
            space = client.get_space_by_key(args.space_key)
            if not space:
                print(f"Space not found: {args.space_key}")
                return 1
            
            print(f"Space: {space.name} (ID: {space.id})")
            print()
            
            # List pages
            pages = []
            for page in client.list_pages_in_space(space.id, limit=args.limit):
                pages.append(page)
            
            if not pages:
                print("No pages found.")
                return 0
            
            # Print table header
            print(f"{'ID':<15} {'Title':<60} {'Parent ID':<15}")
            print("-" * 90)
            
            for page in pages:
                title = page.title[:57] + "..." if len(page.title) > 60 else page.title
                parent = page.parent_id or "(root)"
                print(f"{page.id:<15} {title:<60} {parent:<15}")
            
            print()
            print(f"Total: {len(pages)} pages")
            
    except ConfluenceAPIError as e:
        print(f"Error: {e}")
        return 1
    
    return 0


def cmd_copy_space(args: argparse.Namespace, config: AppConfig) -> int:
    """Copy all pages from a source space to a destination space."""
    dry_run = not args.execute
    
    if dry_run:
        print("=" * 60)
        print("DRY-RUN MODE - No changes will be made")
        print("Use --execute to perform actual copy")
        print("=" * 60)
        print()
    
    print(f"Copying space: {args.source_key} -> {args.dest_key}")
    print()
    
    # Validate config
    errors = validate_config(config)
    if errors:
        print("Configuration errors:")
        for error in errors:
            print(f"  - {error}")
        return 1
        
    # Check allowlists
    if config.allowed_source_spaces and args.source_key not in config.allowed_source_spaces:
        print(f"Error: Source space '{args.source_key}' is not in allowed_source_spaces list.")
        return 1
        
    if config.allowed_destination_spaces and args.dest_key not in config.allowed_destination_spaces:
        print(f"Error: Destination space '{args.dest_key}' is not in allowed_destination_spaces list.")
        return 1
    
    try:
        source_client, dest_client = create_clients(config)
        
        with source_client, dest_client:
            engine = CopyEngine(
                source_client=source_client,
                dest_client=dest_client,
                dry_run=dry_run,
                conflict_handling=args.conflict or config.conflict_handling,
                copy_attachments=args.with_attachments or config.copy_attachments,
                max_tree_depth=args.max_depth if args.max_depth > 0 else config.max_tree_depth,
                create_space_if_missing=args.create_space or config.create_space_if_missing,
                max_attachment_size=config.max_attachment_size_mb * 1024 * 1024,
                skip_large_attachments=config.skip_large_attachments,
            )
            
            # Determine batch limit
            limit = args.max_pages if args.max_pages > 0 else config.max_pages
            
            # Loop support
            loop_count = 0
            while True:
                loop_count += 1
                if args.loop:
                    print(f"\n>>> Starting Batch {loop_count} (Limit: {limit})")
                
                stats = engine.copy_space(
                    source_space_key=args.source_key,
                    dest_space_key=args.dest_key,
                    max_pages=limit,
                    force=args.force,
                )
                
                print()
                print("=" * 60)
                print("Copy Summary:")
                print(f"  Pages found:    {stats.pages_found}")
                print(f"  Pages created:  {stats.pages_created}")
                print(f"  Pages updated:  {stats.pages_updated}")
                print(f"  Pages skipped:  {stats.pages_skipped}")
                print(f"  Pages unchanged:{stats.pages_unchanged}")
                print(f"  Pages failed:   {stats.pages_failed}")
                
                if args.with_attachments and stats.attachments_found > 0:
                    print()
                    print("  Attachment Summary:")
                    print(f"    Found:      {stats.attachments_found}")
                    print(f"    Uploaded:   {stats.attachments_uploaded}")
                    print(f"    Skipped:    {stats.attachments_skipped}")
                    print(f"    Unchanged:  {stats.attachments_unchanged}")
                    print(f"    Failed:     {stats.attachments_failed}")
                
                print("=" * 60)
                
                # Check for loop termination
                if not args.loop:
                    break
                    
                # 1. No pages found means no missing pages left to copy
                if stats.pages_found == 0:
                    print("\n>>> No more missing pages found. Migration complete!")
                    break
                    
                # 2. If we found fewer pages than limit, we are done
                if limit > 0 and stats.pages_found < limit:
                    print(f"\n>>> Fetched {stats.pages_found} pages (less than limit {limit}). Migration complete!")
                    break
                    
                # 3. Safety: If we made no progress at all (no pages created/updated, 
                # no attachments uploaded), we might be stuck.
                # Note: We continue if pages failed (need to report them) or if attachments were uploaded
                if stats.pages_created == 0 and stats.pages_updated == 0 and stats.attachments_uploaded == 0:
                    # Only stop if there were also no failures (truly no progress)
                    if stats.pages_failed == 0:
                        print("\n>>> Batch made no progress (0 created/updated/uploaded). Stopping to prevent infinite loop.")
                        break
                    else:
                        print(f"\n>>> Batch had {stats.pages_failed} failed page(s) but made no other progress.")
                        # Continue to next batch to process remaining pages
                
                print("\n>>> Waiting 2 seconds before next batch...")
                time.sleep(2)
            
            if stats.pages_failed > 0:
                return 1
            
    except (ConfluenceAPIError, ReadOnlyViolationError, ValueError) as e:
        print(f"Error: {e}")
        return 1
    
    return 0


def cmd_copy_tree(args: argparse.Namespace, config: AppConfig) -> int:
    """Copy a page tree from source to destination."""
    dry_run = not args.execute
    
    if dry_run:
        print("=" * 60)
        print("DRY-RUN MODE - No changes will be made")
        print("Use --execute to perform actual copy")
        print("=" * 60)
        print()
    
    print(f"Copying page tree: {args.page_id} -> space {args.dest_key}")
    print()
    
    # Validate config
    errors = validate_config(config)
    if errors:
        print("Configuration errors:")
        for error in errors:
            print(f"  - {error}")
        return 1
    
    try:
        source_client, dest_client = create_clients(config)
        
        with source_client, dest_client:
            engine = CopyEngine(
                source_client=source_client,
                dest_client=dest_client,
                dry_run=dry_run,
                conflict_handling=args.conflict or config.conflict_handling,
                copy_attachments=args.with_attachments or config.copy_attachments,
                max_tree_depth=args.max_depth if args.max_depth > 0 else config.max_tree_depth,
                create_space_if_missing=args.create_space or config.create_space_if_missing,
                max_attachment_size=config.max_attachment_size_mb * 1024 * 1024,
                skip_large_attachments=config.skip_large_attachments,
            )
            
            stats = engine.copy_page_tree(
                source_page_id=args.page_id,
                dest_space_key=args.dest_key,
                dest_parent_id=args.parent_id,
            )
            
            print()
            print("=" * 60)
            print("Copy Summary:")
            print(f"  Pages found:    {stats.pages_found}")
            print(f"  Pages created:  {stats.pages_created}")
            print(f"  Pages updated:  {stats.pages_updated}")
            print(f"  Pages skipped:  {stats.pages_skipped}")
            print(f"  Pages failed:   {stats.pages_failed}")
            
            if args.with_attachments and stats.attachments_found > 0:
                print()
                print("  Attachment Summary:")
                print(f"    Found:      {stats.attachments_found}")
                print(f"    Uploaded:   {stats.attachments_uploaded}")
                print(f"    Skipped:    {stats.attachments_skipped}")
                print(f"    Failed:     {stats.attachments_failed}")
            
            print("=" * 60)
            
            if stats.pages_failed > 0:
                return 1
            
    except (ConfluenceAPIError, ReadOnlyViolationError, ValueError) as e:
        print(f"Error: {e}")
        return 1
    
    return 0


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Confluence Content Copier - Copy content between Confluence Cloud instances",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Test connections
  python copy_confluence.py test-connection
  
  # List spaces on source
  python copy_confluence.py list-spaces --target source
  
  # Copy a space (dry-run)
  python copy_confluence.py copy-space --source-key DOCS --dest-key DOCS-COPY
  
  # Copy a space (execute)
  python copy_confluence.py copy-space --source-key DOCS --dest-key DOCS-COPY --execute
  
  # Copy a page tree
  python copy_confluence.py copy-tree --page-id 12345 --dest-key DOCS-COPY --execute
"""
    )
    
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose logging"
    )
    parser.add_argument(
        "--env-file",
        default=".env",
        help="Path to .env file (default: .env)"
    )
    parser.add_argument(
        "--config-file",
        default="config.yaml",
        help="Path to config.yaml file (default: config.yaml)"
    )
    
    subparsers = parser.add_subparsers(dest="command", help="Command to run")
    
    # test-connection command
    test_parser = subparsers.add_parser(
        "test-connection",
        help="Test API connections"
    )
    test_parser.add_argument(
        "--target",
        choices=["source", "destination", "both"],
        default="both",
        help="Which connection(s) to test"
    )
    
    # list-spaces command
    spaces_parser = subparsers.add_parser(
        "list-spaces",
        help="List spaces on source or destination"
    )
    spaces_parser.add_argument(
        "--target",
        choices=["source", "destination"],
        default="source",
        help="Which instance to list spaces from"
    )
    spaces_parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Maximum number of spaces to list"
    )
    
    # list-pages command
    pages_parser = subparsers.add_parser(
        "list-pages",
        help="List pages in a space"
    )
    pages_parser.add_argument(
        "--space-key",
        required=True,
        help="Space key to list pages from"
    )
    pages_parser.add_argument(
        "--target",
        choices=["source", "destination"],
        default="source",
        help="Which instance to list pages from"
    )
    pages_parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Maximum number of pages to list"
    )
    
    # copy-space command
    copy_space_parser = subparsers.add_parser(
        "copy-space",
        help="Copy all pages from a source space to destination"
    )
    copy_space_parser.add_argument(
        "--source-key",
        required=True,
        help="Source space key"
    )
    copy_space_parser.add_argument(
        "--dest-key",
        required=True,
        help="Destination space key"
    )
    copy_space_parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually perform the copy (default is dry-run)"
    )
    copy_space_parser.add_argument(
        "--conflict",
        choices=["skip", "update", "error"],
        help="How to handle existing pages"
    )
    copy_space_parser.add_argument(
        "--max-pages",
        type=int,
        default=0,
        help="Maximum pages to copy (0 = unlimited)"
    )
    copy_space_parser.add_argument(
        "--create-space",
        action="store_true",
        help="Create destination space if it is missing"
    )
    copy_space_parser.add_argument(
        "--with-attachments",
        action="store_true",
        help="Copy page attachments (opt-in)"
    )
    copy_space_parser.add_argument(
        "--max-depth",
        type=int,
        default=0,
        help="Maximum tree depth (0 = unlimited)"
    )
    
    copy_space_parser.add_argument(
        "--loop",
        action="store_true",
        help="Automatically loop batches until all pages are copied"
    )
    copy_space_parser.add_argument(
        "--force",
        action="store_true",
        help="Force re-process all pages (use with --conflict update to overwrite existing pages)"
    )
    
    # delete-space command
    delete_space_parser = subparsers.add_parser(
        "delete-space",
        help="Delete a destination space"
    )
    delete_space_parser.add_argument(
        "--dest-key",
        required=True,
        help="Destination space key to delete"
    )
    delete_space_parser.add_argument(
        "--confirm",
        action="store_true",
        help="Require confirmation before deletion"
    )
    
    # copy-tree command
    copy_tree_parser = subparsers.add_parser(
        "copy-tree",
        help="Copy a page and all its descendants"
    )
    copy_tree_parser.add_argument(
        "--page-id",
        required=True,
        help="Source page ID to copy"
    )
    copy_tree_parser.add_argument(
        "--dest-key",
        required=True,
        help="Destination space key"
    )
    copy_tree_parser.add_argument(
        "--parent-id",
        help="Optional parent page ID in destination"
    )
    copy_tree_parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually perform the copy (default is dry-run)"
    )
    copy_tree_parser.add_argument(
        "--conflict",
        choices=["skip", "update", "error"],
        help="How to handle existing pages"
    )
    copy_tree_parser.add_argument(
        "--with-attachments",
        action="store_true",
        help="Copy page attachments (opt-in)"
    )
    copy_tree_parser.add_argument(
        "--max-depth",
        type=int,
        default=0,
        help="Maximum tree depth (0 = unlimited, 1 = root + children, etc.)"
    )

    # Define command function
    def cmd_delete_space(args: argparse.Namespace, config: AppConfig) -> int:
        """Delete a destination Confluence space."""
        errors = validate_config(config)
        if errors:
            print("Configuration errors:")
            for error in errors:
                print(f"  - {error}")
            return 1
        try:
            source_client, dest_client = create_clients(config)
            with source_client, dest_client:
                space = dest_client.get_space_by_key(args.dest_key)
                if not space:
                    print(f"Space '{args.dest_key}' not found in destination.")
                    return 1
                if not args.confirm:
                    resp = input(f"Are you sure you want to delete space '{args.dest_key}' (ID {space.id})? Type 'yes' to confirm: ")
                    if resp.lower() != "yes":
                        print("Deletion aborted by user.")
                        return 0
                dest_client.delete_space(space.id)
                print(f"Deleted space '{args.dest_key}' (ID {space.id}).")
                return 0
        except (ConfluenceAPIError, ReadOnlyViolationError, ValueError) as e:
            print(f"Error: {e}")
            return 1

    # Add routing for delete-space
    # (Will be handled later in the main routing block)

    
    args = parser.parse_args()
    
    # Load configuration
    config = load_app_config(args.env_file, args.config_file)
    
    # Setup logging
    setup_logging(verbose=args.verbose or config.verbose_logging)
    
    # Route to command
    if args.command == "test-connection":
        return cmd_test_connection(args, config)
    elif args.command == "list-spaces":
        return cmd_list_spaces(args, config)
    elif args.command == "list-pages":
        return cmd_list_pages(args, config)
    elif args.command == "copy-space":
        return cmd_copy_space(args, config)
    elif args.command == "copy-tree":
        return cmd_copy_tree(args, config)
    elif args.command == "delete-space":
        return cmd_delete_space(args, config)
    else:
        parser.print_help()
        return 0


if __name__ == "__main__":
    sys.exit(main())
