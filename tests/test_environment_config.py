"""
Test environment-based configuration system.
Tests the symlink-based environment switching mechanism for dev/prod isolation.
"""

from __future__ import annotations

import os
import tempfile
import shutil
from pathlib import Path
from unittest.mock import patch
import pytest

from sports_analytics_pipeline.ingest import (
    _setup_dlt_config_for_environment,
    get_dlt_destination,
)


class TestEnvironmentConfiguration:
    """Test environment-based configuration switching."""

    @pytest.fixture
    def temp_workspace(self):
        """Create a temporary workspace for testing."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Change to temp directory for testing
            original_cwd = os.getcwd()
            os.chdir(temp_dir)
            
            # Create test environment directories
            dev_dir = Path(".dlt-dev")
            prod_dir = Path(".dlt-prod")
            dev_dir.mkdir()
            prod_dir.mkdir()
            
            # Create test secrets files
            dev_secrets = dev_dir / "secrets.toml"
            prod_secrets = prod_dir / "secrets.toml"
            
            dev_secrets.write_text("""
[destination.motherduck.credentials]
token = "test_dev_token"
database = "sports_analytics_dev"
""")
            
            prod_secrets.write_text("""
[destination.motherduck.credentials]
token = "test_prod_token"
database = "sports_analytics_prod"
""")
            
            yield temp_dir
            
            # Cleanup: return to original directory
            os.chdir(original_cwd)

    def test_setup_dev_environment(self, temp_workspace):
        """Test setting up dev environment configuration."""
        # Ensure no existing .dlt symlink
        dlt_path = Path(".dlt")
        if dlt_path.exists():
            dlt_path.unlink()
        
        with patch.dict(os.environ, {"SPORTS_ANALYTICS_ENV": "dev"}):
            _setup_dlt_config_for_environment()
        
        # Verify symlink was created and points to dev
        assert dlt_path.is_symlink()
        assert dlt_path.readlink().name == ".dlt-dev"
        
        # Verify secrets file is accessible
        secrets_file = dlt_path / "secrets.toml"
        assert secrets_file.exists()
        content = secrets_file.read_text()
        assert "sports_analytics_dev" in content
        assert "test_dev_token" in content

    def test_setup_prod_environment(self, temp_workspace):
        """Test setting up prod environment configuration."""
        # Ensure no existing .dlt symlink
        dlt_path = Path(".dlt")
        if dlt_path.exists():
            dlt_path.unlink()
        
        with patch.dict(os.environ, {"SPORTS_ANALYTICS_ENV": "prod"}):
            _setup_dlt_config_for_environment()
        
        # Verify symlink was created and points to prod
        assert dlt_path.is_symlink()
        assert dlt_path.readlink().name == ".dlt-prod"
        
        # Verify secrets file is accessible
        secrets_file = dlt_path / "secrets.toml"
        assert secrets_file.exists()
        content = secrets_file.read_text()
        assert "sports_analytics_prod" in content
        assert "test_prod_token" in content

    def test_environment_switching(self, temp_workspace):
        """Test switching between environments."""
        dlt_path = Path(".dlt")
        
        # Start with dev environment
        with patch.dict(os.environ, {"SPORTS_ANALYTICS_ENV": "dev"}):
            _setup_dlt_config_for_environment()
            assert dlt_path.readlink().name == ".dlt-dev"
        
        # Switch to prod environment
        with patch.dict(os.environ, {"SPORTS_ANALYTICS_ENV": "prod"}):
            _setup_dlt_config_for_environment()
            assert dlt_path.readlink().name == ".dlt-prod"
        
        # Switch back to dev
        with patch.dict(os.environ, {"SPORTS_ANALYTICS_ENV": "dev"}):
            _setup_dlt_config_for_environment()
            assert dlt_path.readlink().name == ".dlt-dev"

    def test_default_environment(self, temp_workspace):
        """Test default environment when SPORTS_ANALYTICS_ENV is not set."""
        dlt_path = Path(".dlt")
        if dlt_path.exists():
            dlt_path.unlink()
        
        # Remove environment variable if it exists
        with patch.dict(os.environ, {}, clear=True):
            _setup_dlt_config_for_environment()
        
        # Should default to dev
        assert dlt_path.is_symlink()
        assert dlt_path.readlink().name == ".dlt-dev"

    def test_missing_environment_directory(self, temp_workspace):
        """Test error handling when environment directory doesn't exist."""
        # Remove the dev directory
        shutil.rmtree(".dlt-dev")
        
        with patch.dict(os.environ, {"SPORTS_ANALYTICS_ENV": "dev"}):
            with pytest.raises(FileNotFoundError, match="Environment directory .dlt-dev not found"):
                _setup_dlt_config_for_environment()

    def test_existing_dlt_directory_not_symlink(self, temp_workspace):
        """Test handling of existing .dlt directory that's not a symlink."""
        # Create a regular directory instead of symlink
        dlt_path = Path(".dlt")
        dlt_path.mkdir()
        
        with patch.dict(os.environ, {"SPORTS_ANALYTICS_ENV": "dev"}):
            _setup_dlt_config_for_environment()
        
        # Should replace directory with symlink
        assert dlt_path.is_symlink()
        assert dlt_path.readlink().name == ".dlt-dev"

    @patch("dlt.destinations.motherduck")
    def test_destination_creation_with_environment(self, mock_motherduck, temp_workspace):
        """Test that get_dlt_destination works with environment configuration."""
        # Mock motherduck destination creation
        mock_destination = mock_motherduck.return_value
        
        # Setup dev environment
        with patch.dict(os.environ, {"SPORTS_ANALYTICS_ENV": "dev"}):
            destination = get_dlt_destination("motherduck", "test.db")
        
        # Verify destination was created (database comes from secrets.toml now)
        mock_motherduck.assert_called_once()
        assert destination == mock_destination

    def test_environment_isolation(self, temp_workspace):
        """Test that environments are properly isolated."""
        # Setup dev environment and check secrets
        with patch.dict(os.environ, {"SPORTS_ANALYTICS_ENV": "dev"}):
            _setup_dlt_config_for_environment()
            dev_secrets = Path(".dlt/secrets.toml").read_text()
            assert "sports_analytics_dev" in dev_secrets
            assert "test_dev_token" in dev_secrets
        
        # Switch to prod and verify different secrets
        with patch.dict(os.environ, {"SPORTS_ANALYTICS_ENV": "prod"}):
            _setup_dlt_config_for_environment()
            prod_secrets = Path(".dlt/secrets.toml").read_text()
            assert "sports_analytics_prod" in prod_secrets
            assert "test_prod_token" in prod_secrets
            
            # Ensure dev secrets are not accessible
            assert "sports_analytics_dev" not in prod_secrets
            assert "test_dev_token" not in prod_secrets