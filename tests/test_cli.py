"""Tests for CLI functionality in main.py.

Tests the new database initialization and table-specific ingestion features.
"""

from __future__ import annotations

from unittest.mock import patch
import pytest

# Import main functions to test
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
import main


class TestCLIArgumentParsing:
    """Test CLI argument parsing and validation."""

    def test_tables_argument_validation_valid(self) -> None:
        """Test valid table names are accepted."""
        valid_tables = ["scoreboard", "game_summary"]

        with patch("main.run_espn_source") as mock_run_source:
            with patch(
                "sys.argv",
                ["main.py", "--date", "2024-10-23", "--tables", ",".join(valid_tables)],
            ):
                main.main()
                # Check that the function was called with the correct resource selection
                _, kwargs = mock_run_source.call_args
                assert kwargs["include_schedule"] is True
                assert kwargs["include_game_summary"] is True
                assert kwargs["include_teams"] is False
                assert kwargs["include_rosters"] is False

    def test_tables_argument_validation_invalid(self) -> None:
        """Test invalid table names are rejected."""
        with pytest.raises(SystemExit):  # argparse calls sys.exit on error
            with patch(
                "sys.argv",
                ["main.py", "--date", "2024-10-23", "--tables", "invalid_table"],
            ):
                main.main()


class TestCLITableSelection:
    """Test table selection functionality across different operations."""

    def test_date_ingestion_with_table_selection(self) -> None:
        """Test date ingestion with specific tables."""
        with patch("main.run_espn_source") as mock_run_source:
            with patch(
                "sys.argv",
                [
                    "main.py",
                    "--date",
                    "2024-10-23",
                    "--tables",
                    "teams,scoreboard",
                ],
            ):
                main.main()
                # Check that only selected resources are included
                _, kwargs = mock_run_source.call_args
                assert kwargs["include_teams"] is True
                assert kwargs["include_rosters"] is False 
                assert kwargs["include_schedule"] is True
                assert kwargs["include_game_summary"] is False

    def test_backfill_with_table_selection(self) -> None:
        """Test backfill with specific tables."""
        with patch("main.run_espn_source") as mock_run_source:
            with patch(
                "sys.argv",
                [
                    "main.py",
                    "--backfill",
                    "2025",
                    "--start",
                    "2024-10-01",
                    "--end",
                    "2024-10-31",
                    "--tables",
                    "game_summary",
                ],
            ):
                main.main()

                # Check that run_espn_source was called with correct resource selection
                mock_run_source.assert_called_once()
                _, kwargs = mock_run_source.call_args

                assert kwargs["season_end_year"] == 2025
                assert kwargs["include_game_summary"] is True
                assert kwargs["include_schedule"] is False
                assert kwargs["include_teams"] is False
                assert kwargs["include_rosters"] is False


class TestCLIErrorHandling:
    """Test CLI error handling scenarios."""

    def test_invalid_date_format(self) -> None:
        """Test handling of invalid date format."""
        with pytest.raises(ValueError):
            with patch("sys.argv", ["main.py", "--date", "invalid-date"]):
                main.main()

    def test_no_operation_specified(self) -> None:
        """Test error when no operation is specified."""
        with pytest.raises(SystemExit):
            with patch("sys.argv", ["main.py"]):
                main.main()
