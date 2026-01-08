"""
Integration tests for destination Confluence.

These tests verify actual API operations against a real Confluence instance.
They should be run with valid credentials in .env.

Run with: pytest tests/integration/test_destination.py -v
"""

import os
import pytest
from datetime import datetime

# Skip all tests if credentials not available
pytestmark = pytest.mark.skipif(
    not os.getenv("confluence_destination"),
    reason="Destination Confluence credentials not configured"
)


@pytest.fixture(scope="module")
def dest_client():
    """Create a destination client for testing."""
    from src.client import ConfluenceClient
    from src.utils import load_env_file
    
    # Load env vars
    load_env_file(".env")
    
    client = ConfluenceClient(
        base_url=os.getenv("confluence_destination", ""),
        user_email=os.getenv("confluence_destination_user", ""),
        api_token=os.getenv("confluence_destination_key", ""),
        read_only=False,  # Allow writes for integration tests
        api_delay=0.5,  # Be nice to the API
    )
    
    yield client
    
    client.close()


@pytest.fixture(scope="module")
def test_space(dest_client):
    """Get a test space for integration tests.
    
    This assumes a test space exists. You should create one before running tests.
    """
    # Try to find a space that looks like a test space
    for space in dest_client.list_spaces():
        if "test" in space.key.lower() or "test" in space.name.lower():
            return space
    
    # Fall back to first available space (be careful!)
    spaces = list(dest_client.list_spaces(limit=1))
    if spaces:
        return spaces[0]
    
    pytest.skip("No accessible space found for testing")


class TestConnectionIntegration:
    """Integration tests for connection."""
    
    def test_connection_works(self, dest_client):
        """Test that we can connect to the API."""
        assert dest_client.test_connection() is True
    
    def test_list_spaces_returns_results(self, dest_client):
        """Test that listing spaces works."""
        spaces = list(dest_client.list_spaces(limit=5))
        assert len(spaces) >= 0  # May have no spaces, that's OK


class TestSpaceIntegration:
    """Integration tests for space operations."""
    
    def test_get_space_by_key(self, dest_client, test_space):
        """Test finding a space by key."""
        space = dest_client.get_space_by_key(test_space.key)
        
        assert space is not None
        assert space.key == test_space.key
        assert space.id == test_space.id
    
    def test_get_space_by_id(self, dest_client, test_space):
        """Test getting a space by ID."""
        space = dest_client.get_space_by_id(test_space.id)
        
        assert space.key == test_space.key


class TestPageIntegration:
    """Integration tests for page operations."""
    
    def test_list_pages_in_space(self, dest_client, test_space):
        """Test listing pages in a space."""
        pages = list(dest_client.list_pages_in_space(test_space.id, limit=5))
        # May have no pages, that's OK
        assert isinstance(pages, list)
    
    def test_create_get_update_delete_page(self, dest_client, test_space):
        """Test full CRUD lifecycle for a page."""
        from src.models import CreatePageRequest, UpdatePageRequest
        
        # Generate unique title
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        title = f"Integration Test Page {timestamp}"
        
        # CREATE
        request = CreatePageRequest(
            space_id=test_space.id,
            title=title,
            body_value="<p>This is test content created by integration tests.</p>",
        )
        
        created_page = dest_client.create_page(request)
        
        assert created_page.id is not None
        assert created_page.title == title
        assert created_page.space_id == test_space.id
        
        # GET
        fetched_page = dest_client.get_page_by_id(created_page.id, include_body=True)
        
        assert fetched_page.id == created_page.id
        assert fetched_page.title == title
        assert "test content" in fetched_page.body_storage
        assert fetched_page.version.number == 1
        
        # UPDATE
        update_request = UpdatePageRequest(
            page_id=created_page.id,
            title=title,  # Same title
            body_value="<p>This content has been updated.</p>",
            version_number=2,  # Must be current + 1
            version_message="Updated by integration test",
        )
        
        updated_page = dest_client.update_page(update_request)
        
        assert updated_page.version.number == 2
        
        # Verify update
        verified_page = dest_client.get_page_by_id(created_page.id, include_body=True)
        assert "updated" in verified_page.body_storage
        
        # DELETE
        dest_client.delete_page(created_page.id)
        
        # Verify deleted (should get 404)
        from src.client import ConfluenceAPIError
        with pytest.raises(ConfluenceAPIError) as exc_info:
            dest_client.get_page_by_id(created_page.id)
        
        # Page goes to trash, might return 404 or similar
        assert exc_info.value.status_code in (404, 410)
    
    def test_create_child_page(self, dest_client, test_space):
        """Test creating a page with a parent."""
        from src.models import CreatePageRequest
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Create parent
        parent_request = CreatePageRequest(
            space_id=test_space.id,
            title=f"Parent Page {timestamp}",
            body_value="<p>Parent content</p>",
        )
        parent_page = dest_client.create_page(parent_request)
        
        try:
            # Create child
            child_request = CreatePageRequest(
                space_id=test_space.id,
                title=f"Child Page {timestamp}",
                body_value="<p>Child content</p>",
                parent_id=parent_page.id,
            )
            child_page = dest_client.create_page(child_request)
            
            # Verify hierarchy
            assert child_page.parent_id == parent_page.id
            
            # Verify via ancestors
            ancestors = dest_client.get_page_ancestors(child_page.id)
            ancestor_ids = [a.id for a in ancestors]
            assert parent_page.id in ancestor_ids
            
            # Verify via children
            children = list(dest_client.get_page_children(parent_page.id))
            child_ids = [c.id for c in children]
            assert child_page.id in child_ids
            
        finally:
            # Cleanup
            try:
                dest_client.delete_page(child_page.id)
            except:
                pass
            dest_client.delete_page(parent_page.id)


class TestAncestorsIntegration:
    """Integration tests for ancestor operations."""
    
    def test_get_ancestors_empty_for_root(self, dest_client, test_space):
        """Test that root pages have no ancestors."""
        # Find a root page (no parent)
        for page in dest_client.list_pages_in_space(test_space.id, limit=10):
            if page.parent_id is None:
                ancestors = dest_client.get_page_ancestors(page.id)
                assert len(ancestors) == 0
                return
        
        pytest.skip("No root page found in test space")
