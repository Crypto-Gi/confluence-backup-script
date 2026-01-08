"""
Confluence Content Copier - REST API Client

Confluence Cloud REST API v2 client with support for both source (read-only)
and destination (read-write) instances.
"""

import base64
import logging
import time
from typing import Iterator, Optional, List, Dict, Any, Callable
from urllib.parse import urljoin, urlencode

import requests
from requests.exceptions import RequestException, HTTPError

from .models import (
    Space, Page, Ancestor, ChildRef, Version, Attachment,
    CreatePageRequest, UpdatePageRequest,
)


logger = logging.getLogger(__name__)


class ConfluenceAPIError(Exception):
    """Exception raised for Confluence API errors."""
    
    def __init__(self, message: str, status_code: Optional[int] = None, 
                 response_body: Optional[str] = None):
        super().__init__(message)
        self.status_code = status_code
        self.response_body = response_body


class ReadOnlyViolationError(Exception):
    """Exception raised when attempting to write on a read-only client."""
    pass


class ConfluenceClient:
    """
    Confluence Cloud REST API v2 client.
    
    Supports both read and write operations.
    Can be configured as read-only (for source) or read-write (for destination).
    
    Authentication uses Basic Auth with email:api_token.
    
    Example:
        client = ConfluenceClient(
            base_url="https://example.atlassian.net/wiki",
            user_email="user@example.com",
            api_token="your_api_token",
            read_only=True
        )
        
        for space in client.list_spaces():
            print(space.name)
    """
    
    API_V2_PATH = "/api/v2"
    DEFAULT_LIMIT = 100
    MAX_RETRIES = 3
    RETRY_BACKOFF_FACTOR = 2.0
    
    def __init__(
        self,
        base_url: str,
        user_email: str,
        api_token: str,
        read_only: bool = False,
        api_delay: float = 0.2,
        timeout: int = 30,
    ):
        """
        Initialize the Confluence client.
        
        Args:
            base_url: Base URL of Confluence instance (e.g., https://example.atlassian.net/wiki)
            user_email: Atlassian account email for authentication
            api_token: API token from https://id.atlassian.com/manage/api-tokens
            read_only: If True, block all write operations (for source instances)
            api_delay: Delay between API calls in seconds (for rate limiting)
            timeout: Request timeout in seconds
        """
        # Normalize base URL
        self.base_url = base_url.rstrip("/")
        if not self.base_url.endswith("/wiki"):
            logger.warning(
                f"Base URL '{base_url}' may be missing '/wiki' suffix. "
                "Confluence Cloud URLs typically end with /wiki"
            )
        
        self.user_email = user_email
        self.api_token = api_token
        self.read_only = read_only
        self.api_delay = api_delay
        self.timeout = timeout
        
        # Build auth header
        credentials = f"{user_email}:{api_token}"
        encoded_credentials = base64.b64encode(credentials.encode()).decode()
        self._auth_header = f"Basic {encoded_credentials}"
        
        # Session for connection pooling
        self._session = requests.Session()
        self._session.headers.update({
            "Authorization": self._auth_header,
            "Content-Type": "application/json",
            "Accept": "application/json",
        })
        
        self._last_request_time = 0.0
        
        logger.info(
            f"Initialized ConfluenceClient for {self.base_url} "
            f"(read_only={read_only})"
        )
    
    def _get_api_url(self, endpoint: str) -> str:
        """Build full API URL for an endpoint."""
        # Ensure endpoint starts with /
        if not endpoint.startswith("/"):
            endpoint = "/" + endpoint
        return f"{self.base_url}{self.API_V2_PATH}{endpoint}"
    
    def _rate_limit(self) -> None:
        """Apply rate limiting between API calls."""
        if self.api_delay > 0:
            elapsed = time.time() - self._last_request_time
            if elapsed < self.api_delay:
                time.sleep(self.api_delay - elapsed)
        self._last_request_time = time.time()
    
    def _request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
        is_write: bool = False,
    ) -> Dict[str, Any]:
        """
        Make an API request with retries and error handling.
        
        Args:
            method: HTTP method (GET, POST, PUT, DELETE)
            endpoint: API endpoint path
            params: Query parameters
            json_body: JSON request body
            is_write: Whether this is a write operation (blocked if read_only)
            
        Returns:
            Parsed JSON response
            
        Raises:
            ReadOnlyViolationError: If attempting write on read-only client
            ConfluenceAPIError: On API errors
        """
        if is_write and self.read_only:
            raise ReadOnlyViolationError(
                f"Cannot perform write operation ({method} {endpoint}) "
                "on read-only client"
            )
        
        url = self._get_api_url(endpoint)
        
        for attempt in range(self.MAX_RETRIES):
            try:
                self._rate_limit()
                
                logger.debug(f"{method} {url} params={params}")
                
                response = self._session.request(
                    method=method,
                    url=url,
                    params=params,
                    json=json_body,
                    timeout=self.timeout,
                )
                
                # Handle rate limiting (429)
                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", 60))
                    logger.warning(
                        f"Rate limited. Waiting {retry_after}s before retry..."
                    )
                    time.sleep(retry_after)
                    continue
                
                # Raise for other HTTP errors
                response.raise_for_status()
                
                # Handle empty responses (e.g., DELETE)
                if response.status_code == 204 or not response.content:
                    return {}
                
                return response.json()
                
            except HTTPError as e:
                status_code = e.response.status_code if e.response else None
                response_body = e.response.text if e.response else None
                
                # Log error details
                logger.error(
                    f"API error: {method} {url} -> {status_code}: {response_body}"
                )
                
                # Retry on 5xx errors
                if status_code and status_code >= 500:
                    if attempt < self.MAX_RETRIES - 1:
                        wait_time = self.RETRY_BACKOFF_FACTOR ** attempt
                        logger.info(f"Retrying in {wait_time}s...")
                        time.sleep(wait_time)
                        continue
                
                raise ConfluenceAPIError(
                    f"API request failed: {e}",
                    status_code=status_code,
                    response_body=response_body,
                )
                
            except RequestException as e:
                logger.error(f"Request error: {e}")
                if attempt < self.MAX_RETRIES - 1:
                    wait_time = self.RETRY_BACKOFF_FACTOR ** attempt
                    logger.info(f"Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                raise ConfluenceAPIError(f"Request failed: {e}")
        
        raise ConfluenceAPIError("Max retries exceeded")
    
    def _paginate(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        limit: int = DEFAULT_LIMIT,
    ) -> Iterator[Dict[str, Any]]:
        """
        Iterate through paginated API results.
        
        Yields individual items from the 'results' array across all pages.
        
        Args:
            endpoint: API endpoint path
            params: Additional query parameters
            limit: Items per page
            
        Yields:
            Individual result items
        """
        params = dict(params or {})
        params["limit"] = limit
        cursor = None
        
        while True:
            if cursor:
                params["cursor"] = cursor
            
            response = self._request("GET", endpoint, params=params)
            
            results = response.get("results", [])
            for item in results:
                yield item
            
            # Check for next page
            links = response.get("_links", {})
            next_url = links.get("next")
            
            if not next_url:
                break
            
            # Extract cursor from next URL
            # Format: /wiki/api/v2/...?cursor=<token>
            if "cursor=" in next_url:
                cursor = next_url.split("cursor=")[1].split("&")[0]
            else:
                break
    
    # =========================================================================
    # SPACE OPERATIONS
    # =========================================================================
    
    def list_spaces(self, limit: int = DEFAULT_LIMIT, status: str = "current") -> Iterator[Space]:
        """
        List all accessible spaces.
        
        Args:
            limit: Items per page
            status: Space status ("current" or "trashed")
            
        Yields:
            Space objects
        """
        params = {"status": status}
        for data in self._paginate("/spaces", params=params, limit=limit):
            yield Space.from_api(data)
    
    def get_space_by_id(self, space_id: str) -> Space:
        """
        Get a space by its ID.
        
        Args:
            space_id: The space ID
            
        Returns:
            Space object
        """
        response = self._request("GET", f"/spaces/{space_id}")
        return Space.from_api(response)
    
    def get_space_by_key(self, space_key: str) -> Optional[Space]:
        """
        Get a space by its key (V2 API).
        Checks both current and trashed spaces.
        
        Args:
            space_key: The space key
            
        Returns:
            Space object or None if not found
        """
        # First check active spaces
        for space in self.list_spaces(status="current"):
            if space.key.upper() == space_key.upper():
                return space
                
        # Then check trashed spaces
        for space in self.list_spaces(status="trashed"):
            if space.key.upper() == space_key.upper():
                return space
                
        return None
    

    def create_space(self, key: str, name: str) -> Space:
        """
        Create a new space (V2 API).
        
        Args:
            key: Space key
            name: Space name
            
        Returns:
            Created Space object
        """
        data = {
            "key": key,
            "name": name,
            "type": "global",
        }
        response = self._request("POST", "/spaces", json_body=data, is_write=True)
        return Space.from_api(response)
    # =========================================================================
    # PAGE OPERATIONS (READ)
    # =========================================================================
    
    def list_pages_in_space(
        self,
        space_id: str,
        limit: int = DEFAULT_LIMIT,
        body_format: Optional[str] = None,
    ) -> Iterator[Page]:
        """
        List all pages in a space.
        
        Args:
            space_id: The space ID
            limit: Items per page
            body_format: Body format to include (e.g., "storage", "view")
            
        Yields:
            Page objects
        """
        params = {}
        if body_format:
            params["body-format"] = body_format
            
        for data in self._paginate(f"/spaces/{space_id}/pages", params=params, limit=limit):
            yield Page.from_api(data)
    
    def get_page_by_id(
        self,
        page_id: str,
        include_body: bool = True,
        body_format: str = "storage",
    ) -> Page:
        """
        Get a page by its ID.
        
        Args:
            page_id: The page ID
            include_body: Whether to include page body content
            body_format: Body format ("storage", "atlas_doc_format", or "view")
            
        Returns:
            Page object with full details
        """
        params = {}
        if include_body:
            params["body-format"] = body_format
            
        response = self._request("GET", f"/pages/{page_id}", params=params)
        return Page.from_api(response)

    def delete_space(self, space_id: str) -> bool:
        """Delete a space by its ID.

        Args:
            space_id: The ID of the space to delete.

        Returns:
            True if deletion succeeded (HTTP 204).
        """
        # V2 API uses DELETE /spaces/{space_id}
        self._request("DELETE", f"/spaces/{space_id}", is_write=True)
        return True
    
    def get_page_ancestors(self, page_id: str, limit: int = 100) -> List[Ancestor]:
        """
        Get all ancestors of a page (parent, grandparent, etc.).
        
        Returns ancestors in top-to-bottom order (root first).
        
        Args:
            page_id: The page ID
            limit: Maximum ancestors to return
            
        Returns:
            List of Ancestor objects
        """
        response = self._request(
            "GET", 
            f"/pages/{page_id}/ancestors",
            params={"limit": limit}
        )
        
        results = response.get("results", [])
        return [Ancestor.from_api(a) for a in results]
    
    def get_page_children(self, page_id: str, limit: int = DEFAULT_LIMIT) -> Iterator[ChildRef]:
        """
        Get direct children of a page.
        
        Args:
            page_id: The page ID
            limit: Items per page
            
        Yields:
            ChildRef objects
        """
        for data in self._paginate(f"/pages/{page_id}/direct-children", limit=limit):
            yield ChildRef.from_api(data)
    
    def find_page_by_title(
        self,
        space_id: str,
        title: str,
        parent_id: Optional[str] = None,
    ) -> Optional[Page]:
        """
        Find a page by title in a space.
        
        Args:
            space_id: The space ID
            title: Page title to search for
            parent_id: Optional parent ID to narrow search
            
        Returns:
            Page object or None if not found
        """
        for page in self.list_pages_in_space(space_id):
            if page.title == title:
                if parent_id is None or page.parent_id == parent_id:
                    return page
        return None
    
    # =========================================================================
    # PAGE OPERATIONS (WRITE)
    # =========================================================================
    
    def create_page(self, request: CreatePageRequest) -> Page:
        """
        Create a new page.
        
        Args:
            request: CreatePageRequest with page details
            
        Returns:
            Created Page object
            
        Raises:
            ReadOnlyViolationError: If client is read-only
            ConfluenceAPIError: On API errors
        """
        response = self._request(
            "POST",
            "/pages",
            json_body=request.to_api(),
            is_write=True,
        )
        return Page.from_api(response)
    
    def update_page(self, request: UpdatePageRequest) -> Page:
        """
        Update an existing page.
        
        Note: Version number in request must be current version + 1.
        
        Args:
            request: UpdatePageRequest with page details and version
            
        Returns:
            Updated Page object
            
        Raises:
            ReadOnlyViolationError: If client is read-only
            ConfluenceAPIError: On API errors (including version conflicts)
        """
        response = self._request(
            "PUT",
            f"/pages/{request.page_id}",
            json_body=request.to_api(),
            is_write=True,
        )
        return Page.from_api(response)
    
    def delete_page(self, page_id: str) -> None:
        """
        Delete a page (moves to trash).
        
        Args:
            page_id: The page ID to delete
            
        Raises:
            ReadOnlyViolationError: If client is read-only
            ConfluenceAPIError: On API errors
        """
        self._request(
            "DELETE",
            f"/pages/{page_id}",
            is_write=True,
        )
    
    
    # =========================================================================
    # ATTACHMENT OPERATIONS (READ - V2 API)
    # =========================================================================
    
    def list_page_attachments(
        self,
        page_id: str,
        limit: int = DEFAULT_LIMIT,
    ) -> Iterator[Attachment]:
        """
        List all attachments for a page (V2 API).
        
        Args:
            page_id: The page ID
            limit: Items per page
            
        Yields:
            Attachment objects with metadata
        """
        for data in self._paginate(f"/pages/{page_id}/attachments", limit=limit):
            yield Attachment.from_api(data)
    
    def get_attachment_by_id(self, attachment_id: str) -> Attachment:
        """
        Get attachment metadata by ID (V2 API).
        
        Args:
            attachment_id: The attachment ID
            
        Returns:
            Attachment object
        """
        response = self._request("GET", f"/attachments/{attachment_id}")
        return Attachment.from_api(response)
    
    # =========================================================================
    # ATTACHMENT OPERATIONS (DOWNLOAD/UPLOAD - HYBRID V1/V2)
    # =========================================================================
    
    def download_attachment(self, attachment: Attachment) -> bytes:
        """
        Download attachment binary data.
        
        Uses the download URL from attachment metadata.
        
        Args:
            attachment: Attachment object with download_url
            
        Returns:
            Binary file content
            
        Raises:
            ConfluenceAPIError: On download failure
        """
        if not attachment.download_url:
            raise ValueError("Attachment has no download URL")
        
        # Download URL from API is relative (e.g., "/wiki/download/attachments/...")
        # Base URL already includes /wiki, so we need to strip it to avoid duplication
        # e.g., base_url = "https://site.atlassian.net/wiki"
        #       download_url = "/wiki/download/attachments/12345/file.pdf"
        # We want: "https://site.atlassian.net/wiki/download/attachments/12345/file.pdf"
        # NOT: "https://site.atlassian.net/wiki/wiki/download/attachments/12345/file.pdf"
        
        # Strip /wiki from base if download URL already has it
        if attachment.download_url.startswith("/wiki/"):
            # Use site base without /wiki suffix
            base = self.base_url.rsplit("/wiki", 1)[0]
            full_url = f"{base}{attachment.download_url}"
        else:
            # Fallback: just append (for unexpected URL formats)
            full_url = f"{self.base_url}{attachment.download_url}"
        
        try:
            self._rate_limit()
            response = self._session.get(full_url, timeout=self.timeout)
            response.raise_for_status()
            logger.debug(f"Downloaded {len(response.content)} bytes for {attachment.title}")
            return response.content
        except Exception as e:
            raise ConfluenceAPIError(f"Download failed for {attachment.title}: {e}")
    
    def upload_attachment(
        self,
        page_id: str,
        filename: str,
        file_content: bytes,
        comment: Optional[str] = None,
    ) -> Attachment:
        """
        Upload attachment to a page (V1 API).
        
        Uses multipart/form-data with V1 endpoint.
        Includes retry logic for 429 and 5xx errors.
        
        Args:
            page_id: Destination page ID
            filename: Filename for the attachment
            file_content: Binary file content
            comment: Optional comment
            
        Returns:
            Created Attachment object
            
        Raises:
            ReadOnlyViolationError: If client is read-only
            ConfluenceAPIError: On upload failure
        """
        if self.read_only:
            raise ReadOnlyViolationError(
                "Cannot upload attachment on read-only client"
            )
        
        # V1 API endpoint
        v1_endpoint = f"/rest/api/content/{page_id}/child/attachment"
        full_url = f"{self.base_url}{v1_endpoint}"
        
        # Retry loop (same pattern as _request method)
        for attempt in range(self.MAX_RETRIES):
            try:
                self._rate_limit()
                
                # Prepare multipart form data
                files = {"file": (filename, file_content)}
                data = {}
                if comment:
                    data["comment"] = comment
                
                # V1 API requires this header
                headers = {
                    "X-Atlassian-Token": "nocheck",
                    "Authorization": self._auth_header,
                }
                
                logger.debug(f"Uploading attachment (attempt {attempt + 1}/{self.MAX_RETRIES}): {filename}")
                
                response = requests.post(
                    full_url,
                    files=files,
                    data=data,
                    headers=headers,
                    timeout=self.timeout,
                )
                
                # Handle rate limiting (429) - same as _request
                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", 60))
                    logger.warning(
                        f"Rate limited on upload. Waiting {retry_after}s before retry..."
                    )
                    time.sleep(retry_after)
                    continue
                
                # Raise for other HTTP errors
                response.raise_for_status()
                
                # V1 API returns array in "results"
                result_data = response.json()
                if "results" in result_data and result_data["results"]:
                    # Convert V1 response to V2 format
                    v1_att = result_data["results"][0]
                    att = Attachment.from_api(self._v1_to_v2_attachment(v1_att))
                    logger.info(f"Uploaded attachment: {filename} ({len(file_content)} bytes)")
                    return att
                else:
                    raise ConfluenceAPIError("Upload succeeded but no attachment returned")
                    
            except HTTPError as e:
                status_code = e.response.status_code if e.response else None
                response_body = e.response.text if e.response else None
                
                logger.error(
                    f"Upload error: {status_code}: {response_body}"
                )
                
                # Retry on 5xx errors (same pattern as _request)
                if status_code and status_code >= 500:
                    if attempt < self.MAX_RETRIES - 1:
                        wait_time = self.RETRY_BACKOFF_FACTOR ** attempt
                        logger.info(f"Retrying upload in {wait_time}s...")
                        time.sleep(wait_time)
                        continue
                
                raise ConfluenceAPIError(
                    f"Upload failed for {filename}: {e}",
                    status_code=status_code,
                    response_body=response_body,
                )
                
            except RequestException as e:
                logger.error(f"Upload request error: {e}")
                if attempt < self.MAX_RETRIES - 1:
                    wait_time = self.RETRY_BACKOFF_FACTOR ** attempt
                    logger.info(f"Retrying upload in {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                raise ConfluenceAPIError(f"Upload failed for {filename}: {e}")
            
            except Exception as e:
                raise ConfluenceAPIError(f"Upload failed for {filename}: {e}")
        
        raise ConfluenceAPIError(f"Upload failed for {filename}: Max retries exceeded")
    
    def _v1_to_v2_attachment(self, v1_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convert V1 attachment response to V2 format.
        
        V1 and V2 have slightly different structures.
        """
        extensions = v1_data.get("extensions", {})
        
        return {
            "id": v1_data.get("id", ""),
            "title": v1_data.get("title", ""),
            "fileId": extensions.get("fileId", ""),
            "fileSize": extensions.get("fileSize", 0),
            "mediaType": extensions.get("mediaType", "application/octet-stream"),
            "comment": extensions.get("comment"),
            "version": v1_data.get("version", {}),
            "_links": v1_data.get("_links", {}),
        }
    
    # =========================================================================
    # UTILITY METHODS
    # =========================================================================
    
    def test_connection(self) -> bool:
        """
        Test the API connection and authentication.
        
        Returns:
            True if connection successful
            
        Raises:
            ConfluenceAPIError: On connection or auth errors
        """
        try:
            # Just list spaces with limit=1 to test auth
            response = self._request("GET", "/spaces", params={"limit": 1})
            logger.info(f"Connection test successful for {self.base_url}")
            return True
        except ConfluenceAPIError as e:
            logger.error(f"Connection test failed: {e}")
            raise
    
    def close(self) -> None:
        """Close the HTTP session."""
        self._session.close()
    
    def __enter__(self) -> "ConfluenceClient":
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
