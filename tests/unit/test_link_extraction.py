"""
Tests for external link extraction feature.
"""
import os
import csv
import pytest
from unittest.mock import MagicMock, mock_open, patch
from src.copier import CopyEngine
from src.client import ConfluenceClient

class TestLinkExtraction:
    """Test external link extraction logic."""
    
    @pytest.fixture
    def mock_clients(self):
        source = MagicMock(spec=ConfluenceClient)
        source.base_url = "https://source.atlassian.net/wiki"
        dest = MagicMock(spec=ConfluenceClient)
        dest.base_url = "https://dest.atlassian.net/wiki"
        return source, dest
    
    def test_extract_external_links_logic(self, mock_clients, tmp_path):
        """Test extraction logic with various link types."""
        source, dest = mock_clients
        
        # Use a temp file for log
        log_file = tmp_path / "links.csv"
        
        engine = CopyEngine(source, dest, dry_run=False)
        engine.external_links_log = str(log_file)
        engine._init_link_log()
        
        # Content with mixed links
        content = """
        <p>
            <a href="https://sharepoint.com/doc">SharePoint</a>
            <a href="/wiki/space/page">Internal Relative</a>
            <a href="https://source.atlassian.net/wiki/spaces/DS">Internal Absolute</a>
            <a href="http://google.com">Google</a>
            <a href="#anchor">Anchor</a>
        </p>
        """
        
        engine._extract_external_links("123", "Test Page", content)
        
        # Read log
        with open(log_file, "r") as f:
            reader = csv.reader(f)
            rows = list(reader)
            
        assert rows[0] == ["Page ID", "Page Title", "External URL"]
        
        urls = [r[2] for r in rows[1:]]
        assert "https://sharepoint.com/doc" in urls
        assert "http://google.com" in urls
        assert "/wiki/space/page" not in urls
        assert "https://source.atlassian.net/wiki/spaces/DS" not in urls
        assert "#anchor" not in urls
        
    def test_dry_run_skips_logging(self, mock_clients, tmp_path):
        """Test that dry run does not write to log."""
        source, dest = mock_clients
        log_file = tmp_path / "links_dry.csv"
        
        engine = CopyEngine(source, dest, dry_run=True)
        engine.external_links_log = str(log_file)
        
        engine._init_link_log()
        # Should not create file in dry run
        assert not log_file.exists()
        
        engine._extract_external_links("123", "Test", '<a href="http://ext.com">Link</a>')
        assert not log_file.exists()
