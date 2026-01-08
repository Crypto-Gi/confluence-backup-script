"""
Tests for configuration loading validity.
"""

import os
import yaml
from unittest import mock
from src.utils import AppConfig, load_config, load_app_config

class TestConfigLoading:
    """Test configuration loading logic."""

    def test_app_config_defaults(self):
        """Test AppConfig default values."""
        # Create minimal valid config
        # Just mock source/dest for testing defaults
        from src.utils import ConfluenceConfig
        source = ConfluenceConfig("http://src", "u", "t")
        dest = ConfluenceConfig("http://dest", "u", "t")
        
        config = AppConfig(source, dest)
        
        # Verify defaults
        assert config.max_attachment_size_mb == 50
        assert config.create_space_if_missing is False

    def test_load_app_config_with_mb(self, tmp_path):
        """Test loading config with MB setting."""
        # Create temp config file
        config_file = tmp_path / "config.yaml"
        config_file.write_text("""
allowed_source_spaces: []
allowed_destination_spaces: []
max_attachment_size_mb: 25
create_space_if_missing: true
""")
        
        # Create dummy env file
        env_file = tmp_path / ".env"
        env_file.write_text("""
CONFLUENCE_SOURCE=https://src.atlassian.net/wiki
CONFLUENCE_SOURCE_USER=u
CONFLUENCE_SOURCE_KEY=k
CONFLUENCE_DESTINATION=https://dest.atlassian.net/wiki
CONFLUENCE_DESTINATION_USER=u
CONFLUENCE_DESTINATION_KEY=k
""")
        
        # Load
        app_config = load_app_config(str(env_file), str(config_file))
        
        assert app_config.max_attachment_size_mb == 25
        assert app_config.create_space_if_missing is True

    def test_load_all_config_fields(self, tmp_path):
        """Test loading all configuration fields including skip_large_attachments."""
        env_file = tmp_path / ".env"
        env_file.write_text("confluence_source=http://src\nconfluence_destination=http://dst")
        
        config_file = tmp_path / "config.yaml"
        # We write fields that we want to ensure are loaded
        config_file.write_text("""
max_pages: 99
skip_large_attachments: false
verbose_logging: true
allowed_source_spaces: ['A', 'B']
""")
        
        # Mock os.environ to avoid side effects
        with mock.patch.dict(os.environ, clear=True):
             app_config = load_app_config(str(env_file), str(config_file))
             
             assert app_config.max_pages == 99
             assert app_config.skip_large_attachments is False
             assert app_config.verbose_logging is True
             assert app_config.allowed_source_spaces == ['A', 'B']
