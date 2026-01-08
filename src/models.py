"""
Confluence Content Copier - Data Models

Dataclasses representing Confluence API v2 entities.
"""

from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any
from datetime import datetime


@dataclass
class Version:
    """Page version information."""
    number: int
    message: Optional[str] = None
    created_at: Optional[str] = None
    author_id: Optional[str] = None
    minor_edit: bool = False

    @classmethod
    def from_api(cls, data: Dict[str, Any]) -> "Version":
        """Create Version from API response."""
        return cls(
            number=data.get("number", 1),
            message=data.get("message"),
            created_at=data.get("createdAt"),
            author_id=data.get("authorId"),
            minor_edit=data.get("minorEdit", False),
        )

    def to_api(self) -> Dict[str, Any]:
        """Convert to API request format."""
        result = {"number": self.number}
        if self.message:
            result["message"] = self.message
        return result


@dataclass
class Label:
    """Page label."""
    id: str
    name: str
    prefix: str = "global"

    @classmethod
    def from_api(cls, data: Dict[str, Any]) -> "Label":
        """Create Label from API response."""
        return cls(
            id=data.get("id", ""),
            name=data.get("name", ""),
            prefix=data.get("prefix", "global"),
        )


@dataclass
class Attachment:
    """Confluence attachment."""
    id: str
    title: str  # Filename
    file_id: str
    file_size: int
    media_type: str
    comment: Optional[str] = None
    version: Optional[Version] = None
    download_url: Optional[str] = None  # Relative URL from _links.download
    
    @classmethod
    def from_api(cls, data: Dict[str, Any]) -> "Attachment":
        """Create Attachment from API response."""
        download_url = None
        if "_links" in data and "download" in data["_links"]:
            download_url = data["_links"]["download"]
        
        version = None
        if "version" in data:
            version = Version.from_api(data["version"])
        
        return cls(
            id=data.get("id", ""),
            title=data.get("title", ""),
            file_id=data.get("fileId", ""),
            file_size=data.get("fileSize", 0),
            media_type=data.get("mediaType", "application/octet-stream"),
            comment=data.get("comment"),
            version=version,
            download_url=download_url,
        )


@dataclass
class Space:
    """Confluence space."""
    id: str
    key: str
    name: str
    type: str = "global"
    status: str = "current"
    
    @classmethod
    def from_api(cls, data: Dict[str, Any]) -> "Space":
        """Create Space from API response."""
        return cls(
            id=data.get("id", ""),
            key=data.get("key", ""),
            name=data.get("name", ""),
            type=data.get("type", "global"),
            status=data.get("status", "current"),
        )


@dataclass
class Page:
    """Confluence page with content."""
    id: str
    title: str
    space_id: str
    status: str = "current"
    parent_id: Optional[str] = None
    parent_type: Optional[str] = None
    body_storage: Optional[str] = None
    version: Optional[Version] = None
    labels: List[Label] = field(default_factory=list)
    position: Optional[int] = None
    author_id: Optional[str] = None
    created_at: Optional[str] = None
    
    @classmethod
    def from_api(cls, data: Dict[str, Any]) -> "Page":
        """Create Page from API response."""
        # Extract body content
        body_storage = None
        if "body" in data:
            body = data["body"]
            if "storage" in body and isinstance(body["storage"], dict):
                body_storage = body["storage"].get("value")
            elif "storage" in body and isinstance(body["storage"], str):
                body_storage = body["storage"]
        
        # Extract version
        version = None
        if "version" in data:
            version = Version.from_api(data["version"])
        
        # Extract labels
        labels = []
        if "labels" in data and "results" in data["labels"]:
            labels = [Label.from_api(l) for l in data["labels"]["results"]]
        
        return cls(
            id=data.get("id", ""),
            title=data.get("title", ""),
            space_id=data.get("spaceId", ""),
            status=data.get("status", "current"),
            parent_id=data.get("parentId"),
            parent_type=data.get("parentType"),
            body_storage=body_storage,
            version=version,
            labels=labels,
            position=data.get("position"),
            author_id=data.get("authorId"),
            created_at=data.get("createdAt"),
        )


@dataclass
class Ancestor:
    """Page ancestor (minimal info)."""
    id: str
    type: str = "page"
    
    @classmethod
    def from_api(cls, data: Dict[str, Any]) -> "Ancestor":
        """Create Ancestor from API response."""
        return cls(
            id=data.get("id", ""),
            type=data.get("type", "page"),
        )


@dataclass
class ChildRef:
    """Reference to a child content item."""
    id: str
    title: str
    type: str
    status: str = "current"
    space_id: Optional[str] = None
    child_position: Optional[int] = None
    
    @classmethod
    def from_api(cls, data: Dict[str, Any]) -> "ChildRef":
        """Create ChildRef from API response."""
        return cls(
            id=data.get("id", ""),
            title=data.get("title", ""),
            type=data.get("type", "page"),
            status=data.get("status", "current"),
            space_id=data.get("spaceId"),
            child_position=data.get("childPosition"),
        )


@dataclass
class CreatePageRequest:
    """Request body for creating a new page."""
    space_id: str
    title: str
    body_value: str
    body_representation: str = "storage"
    status: str = "current"
    parent_id: Optional[str] = None
    
    def to_api(self) -> Dict[str, Any]:
        """Convert to API request format."""
        result = {
            "spaceId": self.space_id,
            "status": self.status,
            "title": self.title,
            "body": {
                "representation": self.body_representation,
                "value": self.body_value,
            },
        }
        if self.parent_id:
            result["parentId"] = self.parent_id
        return result


@dataclass
class UpdatePageRequest:
    """Request body for updating an existing page."""
    page_id: str
    title: str
    body_value: str
    version_number: int
    body_representation: str = "storage"
    status: str = "current"
    version_message: Optional[str] = None
    
    def to_api(self) -> Dict[str, Any]:
        """Convert to API request format."""
        version = {"number": self.version_number}
        if self.version_message:
            version["message"] = self.version_message
            
        return {
            "id": self.page_id,
            "status": self.status,
            "title": self.title,
            "body": {
                "representation": self.body_representation,
                "value": self.body_value,
            },
            "version": version,
        }


@dataclass
class PageNode:
    """
    In-memory representation of a page for copy operations.
    Used to build the page tree before copying.
    """
    source_id: str
    title: str
    body_storage: str
    parent_source_id: Optional[str] = None
    children: List["PageNode"] = field(default_factory=list)
    labels: List[str] = field(default_factory=list)
    
    # Populated during copy
    dest_id: Optional[str] = None
    copied: bool = False
    error: Optional[str] = None
    
    # Attachment tracking
    attachments: List["Attachment"] = field(default_factory=list)
    attachments_copied: int = 0
    attachments_skipped: int = 0
    attachments_failed: int = 0
    
    def add_child(self, child: "PageNode") -> None:
        """Add a child node."""
        self.children.append(child)
        child.parent_source_id = self.source_id
