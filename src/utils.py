"""
Confluence Content Copier - Utility Functions

Helper functions for configuration, environment, and logging.
"""

import os
import logging
import yaml
from pathlib import Path
from typing import Dict, Any, Optional
from dataclasses import dataclass


logger = logging.getLogger(__name__)


@dataclass
class ConfluenceConfig:
    """Configuration for a Confluence instance."""
    base_url: str
    user_email: str
    api_token: str
    
    @property
    def is_valid(self) -> bool:
        """Check if all required fields are present."""
        return bool(self.base_url and self.user_email and self.api_token)


@dataclass
class AppConfig:
    """Application configuration."""
    source: ConfluenceConfig
    destination: ConfluenceConfig
    
    # Copy settings
    conflict_handling: str = "skip"
    max_pages: int = 0
    max_tree_depth: int = 0
    api_delay: float = 0.2
    
    # Safety settings
    verbose_logging: bool = False
    
    # Allowlists
    allowed_source_spaces: list = None
    allowed_destination_spaces: list = None
    
    # Space creation
    create_space_if_missing: bool = False
    
    # Attachments
    copy_attachments: bool = False
    max_attachment_size_mb: int = 50
    skip_large_attachments: bool = True
    
    def __post_init__(self):
        if self.allowed_source_spaces is None:
            self.allowed_source_spaces = []
        if self.allowed_destination_spaces is None:
            self.allowed_destination_spaces = []


def load_env_file(env_path: str = ".env") -> Dict[str, str]:
    """
    Load environment variables from a .env file.
    
    Supports simple KEY=VALUE format and quoted values.
    
    Args:
        env_path: Path to .env file
        
    Returns:
        Dictionary of environment variables
    """
    env_vars = {}
    path = Path(env_path)
    
    if not path.exists():
        logger.warning(f"Env file not found: {env_path}")
        return env_vars
    
    with open(path, "r") as f:
        for line in f:
            line = line.strip()
            
            # Skip empty lines and comments
            if not line or line.startswith("#"):
                continue
            
            # Split on first =
            if "=" not in line:
                continue
                
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip()
            
            # Remove surrounding quotes
            if (value.startswith('"') and value.endswith('"')) or \
               (value.startswith("'") and value.endswith("'")):
                value = value[1:-1]
            
            env_vars[key] = value
    
    logger.debug(f"Loaded {len(env_vars)} variables from {env_path}")
    return env_vars


def load_config(config_path: str = "config.yaml") -> Dict[str, Any]:
    """
    Load application configuration from YAML file.
    
    Args:
        config_path: Path to config.yaml
        
    Returns:
        Configuration dictionary
    """
    path = Path(config_path)
    
    if not path.exists():
        logger.warning(f"Config file not found: {config_path}")
        return {}
    
    with open(path, "r") as f:
        config = yaml.safe_load(f) or {}
    
    logger.debug(f"Loaded config from {config_path}")
    return config


def get_confluence_config(prefix: str, env_vars: Optional[Dict[str, str]] = None) -> ConfluenceConfig:
    """
    Build a ConfluenceConfig from environment variables.
    
    Args:
        prefix: Variable prefix ("confluence_source" or "confluence_destination")
        env_vars: Optional pre-loaded env vars, otherwise reads from os.environ
        
    Returns:
        ConfluenceConfig object
    """
    env = env_vars or os.environ
    
    def get_var(base_key: str) -> str:
        """Get value from env trying various cases."""
        # Try upper first (shell convention), then exact match, then lower
        return (
            env.get(base_key.upper()) or 
            env.get(base_key) or 
            env.get(base_key.lower()) or 
            ""
        )
    
    base_url = get_var(prefix)
    api_token = get_var(f"{prefix}_key")
    user_email = get_var(f"{prefix}_user")
    
    # Ensure base_url has /wiki suffix
    if base_url and not base_url.endswith("/wiki"):
        logger.warning(
            f"Base URL for {prefix} (or {prefix.upper()}) doesn't end with /wiki. "
            "This may cause API errors."
        )
    
    if base_url:
        masked_token = "***" if api_token else "None"
        logger.debug(f"Loaded config for {prefix}: URL={base_url}, User={user_email}, Token={masked_token}")

    return ConfluenceConfig(
        base_url=base_url,
        user_email=user_email,
        api_token=api_token,
    )


def load_app_config(
    env_path: str = ".env",
    config_path: str = "config.yaml",
) -> AppConfig:
    """
    Load complete application configuration from env and config files.
    
    Args:
        env_path: Path to .env file
        config_path: Path to config.yaml
        
    Returns:
        AppConfig object
    """
    # Load environment variables
    env_vars = load_env_file(env_path)
    
    # Merge with os.environ (env vars take precedence)
    for key, value in env_vars.items():
        if key not in os.environ:
            os.environ[key] = value
    
    # Build Confluence configs
    source_config = get_confluence_config("confluence_source")
    dest_config = get_confluence_config("confluence_destination")
    
    # Load YAML config
    yaml_config = load_config(config_path)
    
    return AppConfig(
        source=source_config,
        destination=dest_config,
        conflict_handling=yaml_config.get("conflict_handling", "skip"),
        max_pages=yaml_config.get("max_pages", 0),
        max_tree_depth=yaml_config.get("max_tree_depth", 0),
        api_delay=yaml_config.get("api_delay_seconds", 0.2),
        verbose_logging=yaml_config.get("verbose_logging", False),
        allowed_source_spaces=yaml_config.get("allowed_source_spaces", []),
        allowed_destination_spaces=yaml_config.get("allowed_destination_spaces", []),
        create_space_if_missing=yaml_config.get("create_space_if_missing", False),
        copy_attachments=yaml_config.get("copy_attachments", False),
        max_attachment_size_mb=yaml_config.get("max_attachment_size_mb", 50),
        skip_large_attachments=yaml_config.get("skip_large_attachments", True),
    )


def setup_logging(verbose: bool = False) -> None:
    """
    Configure logging for the application.
    
    Args:
        verbose: Enable debug logging
    """
    level = logging.DEBUG if verbose else logging.INFO
    
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    
    # Reduce noise from requests library
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)


def validate_config(config: AppConfig) -> list[str]:
    """
    Validate application configuration.
    
    Args:
        config: AppConfig to validate
        
    Returns:
        List of validation error messages (empty if valid)
    """
    errors = []
    
    # Check source config
    if not config.source.base_url:
        errors.append("Missing source Confluence URL (confluence_source)")
    if not config.source.user_email:
        errors.append("Missing source user email (confluence_source_user)")
    if not config.source.api_token:
        errors.append("Missing source API token (confluence_source_key)")
    
    # Check destination config
    if not config.destination.base_url:
        errors.append("Missing destination Confluence URL (confluence_destination)")
    if not config.destination.user_email:
        errors.append("Missing destination user email (confluence_destination_user)")
    if not config.destination.api_token:
        errors.append("Missing destination API token (confluence_destination_key)")
    
    return errors
