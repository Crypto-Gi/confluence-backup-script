"""
Additional unit tests for URL construction and retry logic.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from requests.exceptions import HTTPError
from src.client import ConfluenceClient, ConfluenceAPIError
from src.models import Attachment, Version


class TestDownloadURLConstruction:
    """Test URL construction for attachment downloads."""
    
    def test_url_with_wiki_prefix(self):
        """Test URL construction when download URL has /wiki prefix."""
        client = ConfluenceClient(
            base_url="https://example.atlassian.net/wiki",
            user_email="test@example.com",
            api_token="token123",
            read_only=True,
        )
        
        attachment = Attachment(
            id="att1",
            title="test.pdf",
            file_id="file1",
            file_size=1000,
            media_type="application/pdf",
            download_url="/wiki/download/attachments/12345/test.pdf",
        )
        
        # Mock the session.get to verify URL
        with patch.object(client._session, 'get') as mock_get:
            mock_response = Mock()
            mock_response.content = b"test content"
            mock_response.raise_for_status = Mock()
            mock_get.return_value = mock_response
            
            client.download_attachment(attachment)
            
            # Verify URL doesn't have double /wiki
            called_url = mock_get.call_args[0][0]
            assert "/wiki/wiki/" not in called_url
            assert called_url == "https://example.atlassian.net/wiki/download/attachments/12345/test.pdf"
    
    def test_url_without_wiki_prefix(self):
        """Test URL construction when download URL lacks /wiki prefix."""
        client = ConfluenceClient(
            base_url="https://example.atlassian.net/wiki",
            user_email="test@example.com",
            api_token="token123",
            read_only=True,
        )
        
        attachment = Attachment(
            id="att1",
            title="test.pdf",
            file_id="file1",
            file_size=1000,
            media_type="application/pdf",
            download_url="/download/attachments/12345/test.pdf",
        )
        
        with patch.object(client._session, 'get') as mock_get:
            mock_response = Mock()
            mock_response.content = b"test content"
            mock_response.raise_for_status = Mock()
            mock_get.return_value = mock_response
            
            client.download_attachment(attachment)
            
            called_url = mock_get.call_args[0][0]
            assert called_url == "https://example.atlassian.net/wiki/download/attachments/12345/test.pdf"


class TestUploadRetryLogic:
    """Test retry logic for upload_attachment."""
    
    def test_retry_on_429(self):
        """Test that upload retries on 429 rate limit."""
        client = ConfluenceClient(
            base_url="https://example.atlassian.net/wiki",
            user_email="test@example.com",
            api_token="token123",
            read_only=False,
        )
        
        with patch('src.client.requests.post') as mock_post, \
             patch('src.client.time.sleep') as mock_sleep:
            
            # First call returns 429, second succeeds
            mock_429 = Mock()
            mock_429.status_code = 429
            mock_429.headers = {"Retry-After": "1"}
            
            mock_success = Mock()
            mock_success.status_code = 200
            mock_success.json.return_value = {
                "results": [{
                    "id": "att1",
                    "title": "test.txt",
                    "extensions": {
                        "fileId": "file1",
                        "fileSize": 100,
                        "mediaType": "text/plain",
                    },
                    "version": {"number": 1},
                    "_links": {},
                }]
            }
            mock_success.raise_for_status = Mock()
            
            mock_post.side_effect = [mock_429, mock_success]
            
            result = client.upload_attachment(
                page_id="page1",
                filename="test.txt",
                file_content=b"content",
            )
            
            # Should have retried
            assert mock_post.call_count == 2
            assert mock_sleep.called
            assert result.title == "test.txt"
    
    def test_retry_on_500(self):
        """Test that upload retries on 5xx errors."""
        client = ConfluenceClient(
            base_url="https://example.atlassian.net/wiki",
            user_email="test@example.com",
            api_token="token123",
            read_only=False,
        )
        
        with patch('src.client.requests.post') as mock_post, \
             patch('src.client.time.sleep') as mock_sleep:
            
            # First call returns 503, second succeeds
            mock_error = Mock()
            mock_error.status_code = 503
            mock_error.text = "Service unavailable"
            mock_error.raise_for_status.side_effect = HTTPError(response=mock_error)
            
            mock_success = Mock()
            mock_success.status_code = 200
            mock_success.json.return_value = {
                "results": [{
                    "id": "att1",
                    "title": "test.txt",
                    "extensions": {
                        "fileId": "file1",
                        "fileSize": 100,
                        "mediaType": "text/plain",
                    },
                    "version": {"number": 1},
                    "_links": {},
                }]
            }
            mock_success.raise_for_status = Mock()
            
            mock_post.side_effect = [mock_error, mock_success]
            
            result = client.upload_attachment(
                page_id="page1",
                filename="test.txt",
                file_content=b"content",
            )
            
            # Should have retried
            assert mock_post.call_count == 2
            assert mock_sleep.called
    
    def test_max_retries_exceeded(self):
        """Test that upload fails after max retries."""
        client = ConfluenceClient(
            base_url="https://example.atlassian.net/wiki",
            user_email="test@example.com",
            api_token="token123",
            read_only=False,
        )
        
        with patch('src.client.requests.post') as mock_post, \
             patch('src.client.time.sleep'):
            
            # Always return 503
            mock_error = Mock()
            mock_error.status_code = 503
            mock_error.text = "Service unavailable"
            mock_error.raise_for_status.side_effect = HTTPError(response=mock_error)
            
            mock_post.return_value = mock_error
            
            with pytest.raises(ConfluenceAPIError, match="Upload failed"):
                client.upload_attachment(
                    page_id="page1",
                    filename="test.txt",
                    file_content=b"content",
                )
            
            # Should have tried MAX_RETRIES times
            assert mock_post.call_count == client.MAX_RETRIES
