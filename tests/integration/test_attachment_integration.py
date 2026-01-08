"""
Integration tests for attachment functionality.

These tests require a live Confluence instance for validation.
They use only the destination instance to avoid modifying source data.
"""

import pytest
import os
from pathlib import Path
from src.client import ConfluenceClient, ConfluenceAPIError
from src.models import Attachment
from src.utils import load_app_config


@pytest.fixture(scope="module")
def dest_client():
    """Create destination client from config."""
    config = load_app_config()
    
    if not config.destination.is_valid:
        pytest.skip("Destination Confluence config not available")
    
    client = ConfluenceClient(
        base_url=config.destination.base_url,
        user_email=config.destination.user_email,
        api_token=config.destination.api_token,
        read_only=False,
    )
    
    yield client


@pytest.fixture(scope="module")
def test_space(dest_client):
    """Get or create a test space for integration tests."""
    test_space_key = "ITEST"
    
    # Try to find existing test space
    space = dest_client.get_space_by_key(test_space_key)
    
    if not space:
        pytest.skip(f"Test space '{test_space_key}' not found. Please create it manually.")
    
    return space


@pytest.fixture
def test_page(dest_client, test_space):
    """Create a temporary test page for attachment tests."""
    from src.models import CreatePageRequest
    
    request = CreatePageRequest(
        space_id=test_space.id,
        title=f"Attachment Test Page {os.urandom(4).hex()}",
        body_value="<p>Test page for attachment integration tests</p>",
    )
    
    page = dest_client.create_page(request)
    
    yield page
    
    # Cleanup
    try:
        dest_client.delete_page(page.id)
    except:
        pass  # Best effort cleanup


class TestAttachmentRoundTrip:
    """Test uploading and downloading attachments."""
    
    def test_upload_small_text_file(self, dest_client, test_page):
        """Test uploading a small text file."""
        content = b"Hello, this is a test attachment!\n" * 100
        filename = "test_file.txt"
        
        # Upload
        uploaded = dest_client.upload_attachment(
            page_id=test_page.id,
            filename=filename,
            file_content=content,
            comment="Test upload from integration tests",
        )
        
        assert uploaded.id is not None
        assert uploaded.title == filename
        assert uploaded.file_size == len(content)
        assert uploaded.media_type == "text/plain"
    
    def test_download_attachment(self, dest_client, test_page):
        """Test downloading an attachment."""
        # First upload
        original_content = b"Test content for download validation\n" * 50
        filename = "download_test.txt"
        
        uploaded = dest_client.upload_attachment(
            page_id=test_page.id,
            filename=filename,
            file_content=original_content,
        )
        
        # Verify download URL was provided
        assert uploaded.download_url is not None
        
        # Download
        downloaded_content = dest_client.download_attachment(uploaded)
        
        # Verify content matches
        assert downloaded_content == original_content
    
    def test_list_page_attachments(self, dest_client, test_page):
        """Test listing attachments for a page."""
        # Upload multiple files
        files = [
            ("file1.txt", b"Content 1"),
            ("file2.txt", b"Content 2"),
        ]
        
        for filename, content in files:
            dest_client.upload_attachment(
                page_id=test_page.id,
                filename=filename,
                file_content=content,
            )
        
        # List attachments
        attachments = list(dest_client.list_page_attachments(test_page.id))
        
        # Should have at least the files we uploaded
        assert len(attachments) >= 2
        
        # Verify file names
        filenames = {att.title for att in attachments}
        assert "file1.txt" in filenames
        assert "file2.txt" in filenames
    
    def test_url_construction_no_double_wiki(self, dest_client, test_page):
        """Test that download URLs don't have double /wiki prefix."""
        content = b"URL test content"
        filename = "url_test.txt"
        
        uploaded = dest_client.upload_attachment(
            page_id=test_page.id,
            filename=filename,
            file_content=content,
        )
        
        # Download using internal method to verify URL construction
        assert uploaded.download_url is not None
        
        # Construct the URL the way download_attachment does
        if uploaded.download_url.startswith("/wiki/"):
            base = dest_client.base_url.rsplit("/wiki", 1)[0]
            full_url = f"{base}{uploaded.download_url}"
        else:
            full_url = f"{dest_client.base_url}{uploaded.download_url}"
        
        # Verify no double /wiki
        assert "/wiki/wiki/" not in full_url
        
        # Verify download works
        downloaded = dest_client.download_attachment(uploaded)
        assert downloaded == content


class TestLargeFiles:
    """Test handling of larger files (within limits)."""
    
    def test_upload_1mb_file(self, dest_client, test_page):
        """Test uploading a 1MB file."""
        content = b"X" * (1024 * 1024)  # 1 MB
        filename = "large_1mb.bin"
        
        uploaded = dest_client.upload_attachment(
            page_id=test_page.id,
            filename=filename,
            file_content=content,
        )
        
        assert uploaded.file_size == len(content)
        
        # Verify download
        downloaded = dest_client.download_attachment(uploaded)
        assert len(downloaded) == len(content)
