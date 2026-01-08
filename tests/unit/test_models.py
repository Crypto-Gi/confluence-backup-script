"""
Unit tests for data models.
"""

import pytest
from src.models import (
    Space, Page, Version, Label, Ancestor, ChildRef,
    CreatePageRequest, UpdatePageRequest, PageNode,
)


class TestSpace:
    """Tests for Space model."""
    
    def test_from_api(self):
        """Test creating Space from API response."""
        data = {
            "id": "123",
            "key": "DOCS",
            "name": "Documentation",
            "type": "global",
            "status": "current",
        }
        
        space = Space.from_api(data)
        
        assert space.id == "123"
        assert space.key == "DOCS"
        assert space.name == "Documentation"
        assert space.type == "global"
    
    def test_from_api_with_defaults(self):
        """Test that missing fields get defaults."""
        data = {"id": "1", "key": "K", "name": "N"}
        
        space = Space.from_api(data)
        
        assert space.type == "global"
        assert space.status == "current"


class TestPage:
    """Tests for Page model."""
    
    def test_from_api_full(self):
        """Test creating Page from complete API response."""
        data = {
            "id": "12345",
            "title": "My Page",
            "spaceId": "1",
            "status": "current",
            "parentId": "456",
            "parentType": "page",
            "version": {
                "number": 5,
                "message": "Updated",
                "createdAt": "2024-01-01T00:00:00Z",
            },
            "body": {
                "storage": {
                    "value": "<p>Hello</p>",
                    "representation": "storage",
                }
            },
            "labels": {
                "results": [
                    {"id": "1", "name": "important", "prefix": "global"},
                ]
            },
        }
        
        page = Page.from_api(data)
        
        assert page.id == "12345"
        assert page.title == "My Page"
        assert page.space_id == "1"
        assert page.parent_id == "456"
        assert page.body_storage == "<p>Hello</p>"
        assert page.version.number == 5
        assert len(page.labels) == 1
        assert page.labels[0].name == "important"
    
    def test_from_api_minimal(self):
        """Test creating Page with minimal data."""
        data = {
            "id": "1",
            "title": "Test",
            "spaceId": "1",
        }
        
        page = Page.from_api(data)
        
        assert page.id == "1"
        assert page.title == "Test"
        assert page.parent_id is None
        assert page.body_storage is None
        assert page.labels == []


class TestVersion:
    """Tests for Version model."""
    
    def test_from_api(self):
        """Test creating Version from API response."""
        data = {
            "number": 10,
            "message": "Update",
            "createdAt": "2024-01-01T00:00:00Z",
            "minorEdit": True,
        }
        
        version = Version.from_api(data)
        
        assert version.number == 10
        assert version.message == "Update"
        assert version.minor_edit is True
    
    def test_to_api(self):
        """Test converting Version to API format."""
        version = Version(number=5, message="My update")
        
        result = version.to_api()
        
        assert result == {"number": 5, "message": "My update"}
    
    def test_to_api_without_message(self):
        """Test converting Version without message."""
        version = Version(number=5)
        
        result = version.to_api()
        
        assert result == {"number": 5}


class TestCreatePageRequest:
    """Tests for CreatePageRequest."""
    
    def test_to_api_basic(self):
        """Test basic request conversion."""
        request = CreatePageRequest(
            space_id="123",
            title="New Page",
            body_value="<p>Content</p>",
        )
        
        result = request.to_api()
        
        assert result["spaceId"] == "123"
        assert result["title"] == "New Page"
        assert result["status"] == "current"
        assert result["body"]["representation"] == "storage"
        assert result["body"]["value"] == "<p>Content</p>"
        assert "parentId" not in result
    
    def test_to_api_with_parent(self):
        """Test request with parent ID."""
        request = CreatePageRequest(
            space_id="123",
            title="Child Page",
            body_value="<p>Content</p>",
            parent_id="456",
        )
        
        result = request.to_api()
        
        assert result["parentId"] == "456"


class TestUpdatePageRequest:
    """Tests for UpdatePageRequest."""
    
    def test_to_api(self):
        """Test update request conversion."""
        request = UpdatePageRequest(
            page_id="12345",
            title="Updated Title",
            body_value="<p>New content</p>",
            version_number=2,
            version_message="Updated via API",
        )
        
        result = request.to_api()
        
        assert result["id"] == "12345"
        assert result["title"] == "Updated Title"
        assert result["status"] == "current"
        assert result["body"]["value"] == "<p>New content</p>"
        assert result["version"]["number"] == 2
        assert result["version"]["message"] == "Updated via API"


class TestPageNode:
    """Tests for PageNode."""
    
    def test_add_child(self):
        """Test adding child nodes."""
        parent = PageNode(
            source_id="1",
            title="Parent",
            body_storage="<p>Parent</p>",
        )
        
        child = PageNode(
            source_id="2",
            title="Child",
            body_storage="<p>Child</p>",
        )
        
        parent.add_child(child)
        
        assert len(parent.children) == 1
        assert parent.children[0] == child
        assert child.parent_source_id == "1"
    
    def test_initial_state(self):
        """Test initial node state."""
        node = PageNode(
            source_id="1",
            title="Test",
            body_storage="<p>Test</p>",
        )
        
        assert node.dest_id is None
        assert node.copied is False
        assert node.error is None
        assert node.children == []
