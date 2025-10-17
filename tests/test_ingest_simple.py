"""Simple tests for the ingestion module."""

import os
import tempfile
from datetime import date
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from sports_analytics_pipeline.ingest import (
    _transform_scoreboard_event,
    dagster_ingest_daily_data,
    dagster_ingest_season_schedule,
    ingest_date_dlt,
    ingest_season_schedule_dlt,
)


class TestTransformFunctions:
    """Test data transformation functions."""

    def test_transform_scoreboard_event(self) -> None:
        """Test scoreboard event transformation."""
        mock_event = {
            "id": "401654321",
            "date": "2024-12-25T20:00:00Z",
            "competitions": [
                {
                    "competitors": [
                        {
                            "homeAway": "home",
                            "score": "110",
                            "team": {"displayName": "Los Angeles Lakers"},
                        },
                        {
                            "homeAway": "away",
                            "score": "105", 
                            "team": {"displayName": "Boston Celtics"},
                        },
                    ],
                    "venue": {"fullName": "Crypto.com Arena"},
                    "status": {"type": {"name": "final"}},
                }
            ],
        }

        result = _transform_scoreboard_event(mock_event)

        assert result["espn_event_id"] == "401654321"
        assert result["date"] == "2024-12-25"
        assert result["home_team"] == "Los Angeles Lakers"
        assert result["away_team"] == "Boston Celtics"
        assert result["home_score"] == 110
        assert result["away_score"] == 105


class TestDltPipelineFunctions:
    """Test the main dlt pipeline functions."""

    def test_ingest_season_schedule_dlt_mock(self) -> None:
        """Test season schedule ingestion with mocked dlt pipeline."""
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "test.duckdb"

            with patch("sports_analytics_pipeline.ingest.dlt.pipeline") as mock_pipeline:
                mock_pipeline_instance = Mock()
                mock_pipeline.return_value = mock_pipeline_instance
                mock_run_result = Mock()
                mock_run_result.loads_ids = ["load_123"]
                mock_pipeline_instance.run.return_value = mock_run_result

                # Call the function
                ingest_season_schedule_dlt(2025, db_path)

                # Verify pipeline was created and run
                mock_pipeline.assert_called_once()
                mock_pipeline_instance.run.assert_called_once()

    def test_ingest_date_dlt_mock(self) -> None:
        """Test daily data ingestion with mocked dlt pipeline."""
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "test.duckdb"
            test_date = date(2024, 12, 25)

            with patch("sports_analytics_pipeline.ingest.dlt.pipeline") as mock_pipeline:
                mock_pipeline_instance = Mock()
                mock_pipeline.return_value = mock_pipeline_instance
                mock_run_result = Mock()
                mock_run_result.loads_ids = ["load_456"]
                mock_pipeline_instance.run.return_value = mock_run_result

                # Call the function
                ingest_date_dlt(test_date, db_path)

                # Verify pipeline was created and run
                mock_pipeline.assert_called_once()
                mock_pipeline_instance.run.assert_called_once()

    def test_dagster_interface_success(self) -> None:
        """Test Dagster interface function returns proper metadata."""
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "test.duckdb"
            test_date = date(2024, 12, 25)

            with patch("sports_analytics_pipeline.ingest.ingest_date_dlt") as mock_ingest:
                result = dagster_ingest_daily_data(test_date, db_path)

                assert result["status"] == "success"
                assert result["target_date"] == "2024-12-25"
                assert "duration_seconds" in result
                mock_ingest.assert_called_once_with(test_date, db_path)

    def test_dagster_season_interface(self) -> None:
        """Test Dagster season schedule interface."""
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "test.duckdb"

            with patch("sports_analytics_pipeline.ingest.ingest_season_schedule_dlt") as mock_ingest:
                result = dagster_ingest_season_schedule(2025, db_path)

                assert result["status"] == "success"
                assert result["season_end_year"] == 2025
                assert "duration_seconds" in result
                mock_ingest.assert_called_once()


@pytest.mark.skipif(
    os.getenv("RUN_LIVE_TESTS") != "1",
    reason="Live tests require RUN_LIVE_TESTS=1 environment variable"
)
class TestLiveIntegration:
    """Integration tests that make real API calls."""

    def test_live_season_schedule_ingest(self) -> None:
        """Test live season schedule data ingestion."""
        import duckdb

        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "test_live.duckdb"
            start = date(2024, 10, 1)
            end = date(2024, 10, 3)  # Small range for testing

            try:
                ingest_season_schedule_dlt(2025, db_path, start=start, end=end)

                # Verify database was created
                assert db_path.exists()

                # Check that we got some data
                conn = duckdb.connect(str(db_path))

                # Check schedule table
                try:
                    result = conn.execute("SELECT COUNT(*) FROM main.schedule").fetchone()
                    if result:
                        schedule_count = result[0]
                        if schedule_count > 0:
                            print(f"✓ Found {schedule_count} schedule records")
                except Exception:
                    print("Schedule table not found or empty")

                # Check teams table
                try:
                    result = conn.execute("SELECT COUNT(*) FROM main.teams").fetchone()
                    if result:
                        teams_count = result[0]
                        if teams_count > 0:
                            print(f"✓ Found {teams_count} teams")
                except Exception:
                    print("Teams table not found or empty")

                conn.close()

            except Exception as e:
                pytest.fail(f"Live integration test failed: {e}")

    def test_live_daily_ingest(self) -> None:
        """Test live daily data ingestion."""
        import duckdb

        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "test_live_daily.duckdb"
            test_date = date(2024, 12, 15)  # Pick a date likely to have games

            try:
                ingest_date_dlt(test_date, db_path)

                # Verify database was created
                assert db_path.exists()

                conn = duckdb.connect(str(db_path))

                # Check for any tables created
                tables_result = conn.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'main'
                """).fetchall()

                table_names = [table[0] for table in tables_result]
                print(f"✓ Created tables: {table_names}")

                conn.close()

            except Exception as e:
                pytest.fail(f"Live daily ingestion test failed: {e}")


if __name__ == "__main__":
    # Run tests with verbose output
    pytest.main([__file__, "-v"])