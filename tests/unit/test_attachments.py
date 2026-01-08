"""
Unit tests for attachment functionality and depth control.
"""

import pytest
from unittest.mock import Mock, MagicMock, patch
from src.models import Attachment, Version, PageNode
from src.copier import CopyStats, CopyState, CopyEngine


class TestAttachmentModel:
    """Tests for Attachment model."""
    
    def test_from_api_complete(self):
        """Test Attachment.from_api with complete data."""
        data = {
            "id": "att123",
            "title": "document.pdf",
            "fileId": "file-abc",
            "fileSize": 1024000,
            "mediaType": "application/pdf",
            "comment": "Test attachment",
            "version": {"number": 1},
            "_links": {
                "download": "/wiki/download/attachments/12345/document.pdf"
            }
        }
        
        att = Attachment.from_api(data)
        
        assert att.id == "att123"
        assert att.title == "document.pdf"
        assert att.file_id == "file-abc"
        assert att.file_size == 1024000
        assert att.media_type == "application/pdf"
        assert att.comment == "Test attachment"
        assert att.version.number == 1
        assert att.download_url == "/wiki/download/attachments/12345/document.pdf"
    
    def test_from_api_minimal(self):
        """Test Attachment.from_api with minimal data."""
        data = {
            "id": "att456",
            "title": "file.txt",
        }
        
        att = Attachment.from_api(data)
        
        assert att.id == "att456"
        assert att.title == "file.txt"
        assert att.file_id == ""
        assert att.file_size == 0
        assert att.media_type == "application/octet-stream"
        assert att.download_url is None


class TestCopyStatsAttachments:
    """Tests for CopyStats attachment tracking."""
    
    def test_attachment_stats_defaults(self):
        """Test default attachment stats."""
        stats = CopyStats()
        
        assert stats.attachments_found == 0
        assert stats.attachments_downloaded == 0
        assert stats.attachments_uploaded == 0
        assert stats.attachments_skipped == 0
        assert stats.attachments_failed == 0
    
    def test_summary_without_attachments(self):
        """Test summary when no attachments."""
        stats = CopyStats(pages_found=10, pages_created=5)
        summary = stats.summary()
        
        assert "Attachments" not in summary
    
    def test_summary_with_attachments(self):
        """Test summary includes attachments when present."""
        stats = CopyStats(
            pages_found=10,
            pages_created=5,
            attachments_found=20,
            attachments_uploaded=18,
            attachments_skipped=2,
        )
        summary = stats.summary()
        
        assert "Attachments" in summary
        assert "Found: 20" in summary
        assert "Uploaded: 18" in summary


class TestCopyStateAttachments:
    """Tests for CopyState attachment mapping."""
    
    def test_attachment_mapping_methods(self):
        """Test attachment mapping get/set."""
        state = CopyState()
        
        # Initially empty
        assert state.get_dest_attachment_id("att-src-1") is None
        
        # Set mapping
        state.set_attachment_mapping("att-src-1", "att-dest-1")
        
        # Retrieve
        assert state.get_dest_attachment_id("att-src-1") == "att-dest-1"
    
    def test_backward_compatibility(self):
        """Test state loads without attachment_mapping (v1.0 format)."""
        import json
        import tempfile
        import os
        
        # Create v1.0 format state file
        old_state = {
            "last_run": "2024-01-01T00:00:00Z",
            "page_mapping": {"src1": "dest1"}
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(old_state, f)
            temp_path = f.name
        
        try:
            state = CopyState.load(temp_path)
            assert state.page_mapping == {"src1": "dest1"}
            assert state.attachment_mapping == {}  # Default empty
        finally:
            os.unlink(temp_path)


class TestPageNodeAttachments:
    """Tests for PageNode attachment fields."""
    
    def test_attachment_fields_default(self):
        """Test default attachment tracking fields."""
        node = PageNode(
            source_id="1",
            title="Test",
            body_storage="<p>test</p>",
        )
        
        assert node.attachments == []
        assert node.attachments_copied == 0
        assert node.attachments_skipped == 0
        assert node.attachments_failed == 0


class TestDepthLimiting:
    """Tests for depth-limited tree traversal."""
    
    def test_depth_calculation(self):
        """Test depth counting logic."""
        # Simulate: root (depth 0) -> child1 (depth 1) -> grandchild (depth 2)
        
        # max_depth=1 should include root + children, not grandchildren
        depths_visited = []
        max_depth = 1
        
        def simulate_fetch(page_id: str, current_depth: int):
            depths_visited.append(current_depth)
            if max_depth > 0 and current_depth >= max_depth:
                return  # Stop
            # Simulate children
            if current_depth == 0:
                simulate_fetch("child1", 1)
                simulate_fetch("child2", 1)
        
        simulate_fetch("root", 0)
        
        assert depths_visited == [0, 1, 1]  # root + 2 children
    
    def test_unlimited_depth(self):
        """Test that depth 0 means unlimited."""
        depths_visited = []
        max_depth = 0  # Unlimited
        
        def simulate_fetch(page_id: str, current_depth: int):
            depths_visited.append(current_depth)
            # max_depth=0 means never stop due to depth
            if max_depth > 0 and current_depth >= max_depth:
                return
            if current_depth < 3:  # Simulate 3 levels
                simulate_fetch(f"child_{current_depth+1}", current_depth + 1)
        
        simulate_fetch("root", 0)
        
        assert depths_visited == [0, 1, 2, 3]  # All levels


class TestCopyEngineInit:
    """Tests for CopyEngine initialization with new parameters."""
    
    def test_new_parameters_defaults(self):
        """Test new parameters have correct defaults."""
        source = Mock()
        dest = Mock()
        
        with patch.object(CopyState, 'load', return_value=CopyState()):
            engine = CopyEngine(source, dest)
        
        assert engine.copy_attachments is False
        assert engine.max_attachment_size == 52428800  # 50 MB
        assert engine.skip_large_attachments is True
        assert engine.max_tree_depth == 0  # Unlimited
    
    def test_new_parameters_custom(self):
        """Test custom new parameters."""
        source = Mock()
        dest = Mock()
        
        with patch.object(CopyState, 'load', return_value=CopyState()):
            engine = CopyEngine(
                source,
                dest,
                copy_attachments=True,
                max_attachment_size=50000000,  # 50 MB
                skip_large_attachments=False,
                max_tree_depth=3,
            )
        
        assert engine.copy_attachments is True
        assert engine.max_attachment_size == 50000000
        assert engine.skip_large_attachments is False
        assert engine.max_tree_depth == 3


class TestAttachmentSizeLimit:
    """Tests for attachment size limit enforcement."""
    
    def test_size_limit_skip(self):
        """Test that large attachments are skipped when configured."""
        # Create mock attachment exceeding limit
        large_att = Attachment(
            id="att1",
            title="large.zip",
            file_id="f1",
            file_size=200_000_000,  # 200 MB
            media_type="application/zip",
        )
        
        node = PageNode(
            source_id="1",
            title="Test",
            body_storage="<p>test</p>",
            attachments=[large_att],
        )
        
        source = Mock()
        dest = Mock()
        
        with patch.object(CopyState, 'load', return_value=CopyState()):
            engine = CopyEngine(
                source,
                dest,
                copy_attachments=True,
                max_attachment_size=100_000_000,  # 100 MB limit
                skip_large_attachments=True,
                dry_run=True,
            )
        
        # Call the attachment copy method
        engine._copy_attachments_for_page(node, "dest-page-1")
        
        # Should be skipped, not copied
        assert node.attachments_skipped == 1
        assert node.attachments_copied == 0
        assert engine.stats.attachments_skipped == 1
