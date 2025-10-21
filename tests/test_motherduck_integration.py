"""Tests for MotherDuck integration.

Since MotherDuck requires external credentials, these tests focus on:
1. Testing the destination factory function
2. Testing CLI parameter validation
3. Testing error handling
"""

from __future__ import annotations

import pytest
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

from sports_analytics_pipeline.ingest import get_dlt_destination, IngestionError


class TestDestinationFactory:
    """Test the destination factory function."""

    def test_local_destination_creation(self) -> None:
        """Test that local destinations are created correctly."""
        db_path = Path("test.duckdb")
        destination = get_dlt_destination("local", db_path)
        assert destination is not None

    @patch("dlt.destinations.motherduck")
    def test_motherduck_destination_creation(self, mock_motherduck: MagicMock) -> None:
        """Test that MotherDuck destinations are created correctly."""
        mock_motherduck.return_value = Mock()
        destination = get_dlt_destination("motherduck", "my_db")

        # Verify it was called (database parameter comes from secrets.toml now)
        mock_motherduck.assert_called_once()
        assert destination is not None

    @patch("dlt.destinations.motherduck")
    def test_motherduck_destination_error_handling(
        self, mock_motherduck: MagicMock
    ) -> None:
        """Test error handling when MotherDuck connection fails."""
        mock_motherduck.side_effect = Exception("Connection failed")

        with pytest.raises(IngestionError) as exc_info:
            get_dlt_destination("motherduck", "my_db")

        assert "Failed to create MotherDuck destination" in str(exc_info.value)
        assert "Ensure MotherDuck credentials are configured" in str(exc_info.value)

    def test_invalid_storage_backend(self) -> None:
        """Test error with invalid storage backend."""
        with pytest.raises(IngestionError) as exc_info:
            get_dlt_destination("invalid", "test.db")

        assert "Invalid storage backend: invalid" in str(exc_info.value)
        assert "Use 'local' or 'motherduck'" in str(exc_info.value)


class TestCLIIntegration:
    """Test CLI integration with storage parameter."""

    def test_cli_accepts_storage_parameter(self) -> None:
        """Test that CLI accepts and processes storage parameter correctly."""
        import subprocess
        import sys

        # Test that help shows the storage parameter
        result = subprocess.run(
            [sys.executable, "main.py", "--help"],
            capture_output=True,
            text=True,
            cwd="/home/justin/projects/scraping-testing/sports-analytics-pipeline",
        )

        assert "--storage {local,motherduck}" in result.stdout
        assert "MotherDuck cloud" in result.stdout

    def test_cli_validates_storage_choices(self) -> None:
        """Test that CLI validates storage parameter choices."""
        import subprocess
        import sys

        # Test invalid storage backend
        result = subprocess.run(
            [sys.executable, "main.py", "--all", "--storage", "invalid"],
            capture_output=True,
            text=True,
            cwd="/home/justin/projects/scraping-testing/sports-analytics-pipeline",
        )

        # Should exit with error code and show validation message
        assert result.returncode != 0
        assert "invalid choice: 'invalid'" in result.stderr
