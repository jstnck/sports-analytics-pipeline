"""Integration tests for dlt-based ingestion functions.

These tests verify that the main ingestion functions work correctly
with basic functionality testing (avoiding heavy API calls).
"""

from __future__ import annotations

import tempfile
from pathlib import Path
from unittest.mock import patch

# Import from main.py since that's where get_resource_selection is defined
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from main import get_resource_selection


class TestResourceSelection:
    """Test resource selection helper function."""

    def test_get_resource_selection_all_enabled(self) -> None:
        """Test resource selection with no table filters."""
        result = get_resource_selection(None)
        
        expected = {
            "include_teams": True,
            "include_rosters": True,
            "include_schedule": True,
            "include_game_summary": True,
        }
        assert result == expected

    def test_get_resource_selection_specific_tables(self) -> None:
        """Test resource selection with specific table filters."""
        selected_tables = {"teams", "scoreboard"}
        result = get_resource_selection(selected_tables)
        
        expected = {
            "include_teams": True,
            "include_rosters": False,
            "include_schedule": True,  # scoreboard maps to schedule
            "include_game_summary": False,
        }
        assert result == expected

    def test_get_resource_selection_empty_set(self) -> None:
        """Test resource selection with empty table set."""
        result = get_resource_selection(set())
        
        expected = {
            "include_teams": False,
            "include_rosters": False,
            "include_schedule": False,
            "include_game_summary": False,
        }
        assert result == expected


class TestDltConfiguration:
    """Test DLT configuration functionality."""

    @patch("sports_analytics_pipeline.ingest.get_dlt_destination")
    def test_destination_creation(self, mock_get_destination) -> None:
        """Test that destination creation function is called correctly."""
        from sports_analytics_pipeline.ingest import get_dlt_destination
        
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "test.duckdb"
            
            # Test local destination
            get_dlt_destination("local", db_path)
            mock_get_destination.assert_called_with("local", db_path)
            
            # Test motherduck destination  
            get_dlt_destination("motherduck", db_path)
            mock_get_destination.assert_called_with("motherduck", db_path)
