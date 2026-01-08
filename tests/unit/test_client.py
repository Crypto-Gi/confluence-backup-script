"""
Unit tests for ConfluenceClient.
"""

import base64
import pytest
import responses
from responses import matchers

from src.client import (
    ConfluenceClient,
    ConfluenceAPIError,
    ReadOnlyViolationError,
)
from src.models import CreatePageRequest


class TestConfluenceClientAuth:
    """Tests for authentication."""
    
    def test_auth_header_generated_correctly(self):
        """Test that basic auth header is correctly encoded."""
        client = ConfluenceClient(
            base_url="https://example.atlassian.net/wiki",
            user_email="test@example.com",
            api_token="test_token_123",
        )
        
        expected_credentials = "test@example.com:test_token_123"
        expected_encoded = base64.b64encode(expected_credentials.encode()).decode()
        expected_header = f"Basic {expected_encoded}"
        
        assert client._auth_header == expected_header
    
    def test_base_url_normalized(self):
        """Test that trailing slashes are removed."""
        client = ConfluenceClient(
            base_url="https://example.atlassian.net/wiki/",
            user_email="test@example.com",
            api_token="test_token",
        )
        
        assert client.base_url == "https://example.atlassian.net/wiki"


class TestConfluenceClientReadOnly:
    """Tests for read-only mode."""
    
    def test_read_only_blocks_writes(self):
        """Test that write operations are blocked in read-only mode."""
        client = ConfluenceClient(
            base_url="https://example.atlassian.net/wiki",
            user_email="test@example.com",
            api_token="test_token",
            read_only=True,
        )
        
        request = CreatePageRequest(
            space_id="123",
            title="Test",
            body_value="<p>Content</p>",
        )
        
        with pytest.raises(ReadOnlyViolationError):
            client.create_page(request)
    
    @responses.activate
    def test_read_only_allows_reads(self):
        """Test that read operations work in read-only mode."""
        responses.add(
            responses.GET,
            "https://example.atlassian.net/wiki/api/v2/spaces",
            json={"results": [], "_links": {}},
            status=200,
        )
        
        client = ConfluenceClient(
            base_url="https://example.atlassian.net/wiki",
            user_email="test@example.com",
            api_token="test_token",
            read_only=True,
        )
        
        # Should not raise
        spaces = list(client.list_spaces())
        assert spaces == []


class TestConfluenceClientAPI:
    """Tests for API operations."""
    
    @responses.activate
    def test_list_spaces(self):
        """Test listing spaces."""
        responses.add(
            responses.GET,
            "https://example.atlassian.net/wiki/api/v2/spaces",
            json={
                "results": [
                    {"id": "1", "key": "DOCS", "name": "Documentation", "type": "global"},
                    {"id": "2", "key": "KB", "name": "Knowledge Base", "type": "global"},
                ],
                "_links": {},
            },
            status=200,
        )
        
        client = ConfluenceClient(
            base_url="https://example.atlassian.net/wiki",
            user_email="test@example.com",
            api_token="test_token",
        )
        
        spaces = list(client.list_spaces())
        
        assert len(spaces) == 2
        assert spaces[0].key == "DOCS"
        assert spaces[1].key == "KB"
    
    @responses.activate
    def test_get_page_by_id(self):
        """Test getting a page by ID."""
        responses.add(
            responses.GET,
            "https://example.atlassian.net/wiki/api/v2/pages/12345",
            json={
                "id": "12345",
                "title": "Test Page",
                "spaceId": "1",
                "status": "current",
                "parentId": "456",
                "version": {"number": 1},
                "body": {
                    "storage": {
                        "value": "<p>Hello World</p>",
                        "representation": "storage",
                    }
                },
            },
            status=200,
        )
        
        client = ConfluenceClient(
            base_url="https://example.atlassian.net/wiki",
            user_email="test@example.com",
            api_token="test_token",
        )
        
        page = client.get_page_by_id("12345", include_body=True)
        
        assert page.id == "12345"
        assert page.title == "Test Page"
        assert page.parent_id == "456"
        assert page.body_storage == "<p>Hello World</p>"
    
    @responses.activate
    def test_create_page(self):
        """Test creating a page."""
        responses.add(
            responses.POST,
            "https://example.atlassian.net/wiki/api/v2/pages",
            json={
                "id": "99999",
                "title": "New Page",
                "spaceId": "1",
                "status": "current",
                "version": {"number": 1},
            },
            status=200,
        )
        
        client = ConfluenceClient(
            base_url="https://example.atlassian.net/wiki",
            user_email="test@example.com",
            api_token="test_token",
            read_only=False,
        )
        
        request = CreatePageRequest(
            space_id="1",
            title="New Page",
            body_value="<p>Content</p>",
        )
        
        page = client.create_page(request)
        
        assert page.id == "99999"
        assert page.title == "New Page"
    
    @responses.activate
    def test_api_error_handling(self):
        """Test that API errors are properly raised."""
        responses.add(
            responses.GET,
            "https://example.atlassian.net/wiki/api/v2/spaces/999",
            json={"message": "Space not found"},
            status=404,
        )
        
        client = ConfluenceClient(
            base_url="https://example.atlassian.net/wiki",
            user_email="test@example.com",
            api_token="test_token",
        )
        
        with pytest.raises(ConfluenceAPIError) as exc_info:
            client.get_space_by_id("999")
            
        assert "404" in str(exc_info.value)


class TestConfluenceClientPagination:
    """Tests for pagination."""
    
    @responses.activate
    def test_pagination_follows_cursor(self):
        """Test that pagination follows cursor links."""
        # First page
        responses.add(
            responses.GET,
            "https://example.atlassian.net/wiki/api/v2/spaces",
            json={
                "results": [{"id": "1", "key": "A", "name": "Space A"}],
                "_links": {
                    "next": "/wiki/api/v2/spaces?cursor=abc123",
                },
            },
            status=200,
        )
        
        # Second page
        responses.add(
            responses.GET,
            "https://example.atlassian.net/wiki/api/v2/spaces",
            json={
                "results": [{"id": "2", "key": "B", "name": "Space B"}],
                "_links": {},
            },
            status=200,
        )
        
        client = ConfluenceClient(
            base_url="https://example.atlassian.net/wiki",
            user_email="test@example.com",
            api_token="test_token",
            api_delay=0,  # Disable delay for testing
        )
        
        spaces = list(client.list_spaces())
        
        assert len(spaces) == 2
        assert spaces[0].key == "A"
        assert spaces[1].key == "B"
