"""Comprehensive tests for the NBA data ingestion pipeline."""

from __future__ import annotations

import os
import tempfile
from datetime import date
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from sports_analytics_pipeline.ingest import (
    _transform_player_stats,
    _transform_scoreboard_event,
    backfill_box_scores_dlt,
    dagster_backfill_box_scores,
    dagster_ingest_daily_data,
    dagster_ingest_season_schedule,
    ingest_date_dlt,
    ingest_season_schedule_dlt,
)


class TestScoreboardTransformation:
    """Test scoreboard data transformation functions."""

    def test_transform_scoreboard_event_basic(self) -> None:
        """Test basic scoreboard event transformation."""
        mock_event = {
            "id": "401654321",
            "date": "2024-12-25T20:00:00Z",
            "competitions": [
                {
                    "id": "401654321",
                    "date": "2024-12-25T20:00:00Z",
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
        assert result["venue"] == "Crypto.com Arena"
        assert result["status"] == "final"
        assert result["game_type"] == "regular season"

    def test_transform_scoreboard_event_minimal(self) -> None:
        """Test scoreboard event transformation with minimal data."""
        mock_event = {"id": "401654321"}

        result = _transform_scoreboard_event(mock_event)

        assert result["espn_event_id"] == "401654321"
        assert result["date"] is None
        assert result["home_team"] is None
        assert result["away_team"] is None
        assert result["home_score"] is None
        assert result["away_score"] is None

    def test_transform_scoreboard_event_invalid_score(self) -> None:
        """Test scoreboard event transformation with invalid score data."""
        mock_event = {
            "id": "401654321",
            "competitions": [
                {
                    "competitors": [
                        {
                            "homeAway": "home",
                            "score": "invalid",
                            "team": {"displayName": "Team A"},
                        },
                        {
                            "homeAway": "away",
                            "score": None,
                            "team": {"displayName": "Team B"},
                        },
                    ]
                }
            ],
        }

        result = _transform_scoreboard_event(mock_event)
        assert result["home_score"] is None
        assert result["away_score"] is None


class TestPlayerTransformation:
    """Test player data transformation functions."""

    def test_transform_player_stats_basic(self) -> None:
        """Test basic player stats transformation."""
        mock_player_data = {
            "athlete": {"firstName": "LeBron", "lastName": "James"},
            "team": "Los Angeles Lakers",
            "stats": [
                {"name": "points", "value": 25},
                {"name": "rebounds", "value": 8},
                {"name": "assists", "value": 12},
                {"name": "minutes", "value": "38:45"},
                {"name": "fouls", "value": 2},
                {"name": "plusMinus", "value": 15},
            ],
        }

        result = _transform_player_stats(
            mock_player_data, "401654321", "2024-12-25", "Boston Celtics", "Los Angeles Lakers"
        )

        assert result["first_name"] == "LeBron"
        assert result["last_name"] == "James"
        assert result["team"] == "Los Angeles Lakers"
        assert result["points"] == 25
        assert result["rebounds"] == 8
        assert result["assists"] == 12
        assert result["minutes_played"] == "38:45"
        assert result["fouls"] == 2
        assert result["plus_minus"] == 15
        assert result["espn_event_id"] == "401654321"
        assert result["date"] == "2024-12-25"

    def test_transform_player_stats_name_splitting(self) -> None:
        """Test player stats transformation with full name that needs splitting."""
        mock_player_data = {
            "athlete": {"displayName": "Anthony Davis Jr."},
            "stats": [],
        }

        result = _transform_player_stats(
            mock_player_data, "401654321", "2024-12-25", "Team A", "Team B"
        )

        assert result["first_name"] == "Anthony"
        assert result["last_name"] == "Davis Jr."

    def test_transform_player_stats_minimal(self) -> None:
        """Test player stats transformation with minimal data."""
        mock_player_data = {"athlete": {"firstName": "Test", "lastName": "Player"}}

        result = _transform_player_stats(
            mock_player_data, "401654321", "2024-12-25", "Team A", "Team B"
        )

        assert result["first_name"] == "Test"
        assert result["last_name"] == "Player"
        assert result["points"] is None
        assert result["rebounds"] is None
        assert result["assists"] is None


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

                # Check pipeline configuration
                call_args = mock_pipeline.call_args
                assert call_args[1]["pipeline_name"] == "nba_schedule"
                assert "duckdb" in str(call_args[1]["destination"])
                assert call_args[1]["dataset_name"] == "main"

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

                # Check pipeline configuration
                call_args = mock_pipeline.call_args
                assert call_args[1]["pipeline_name"] == "nba_daily"
                assert call_args[1]["dataset_name"] == "main"


class TestDagsterInterface:
    """Test Dagster-compatible interface functions."""

    def test_dagster_ingest_season_schedule_success(self) -> None:
        """Test Dagster season schedule function returns proper metadata on success."""
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "test.duckdb"

            with patch("sports_analytics_pipeline.ingest.ingest_season_schedule_dlt") as mock_ingest:
                result = dagster_ingest_season_schedule(2025, db_path)

                assert result["status"] == "success"
                assert result["season_end_year"] == 2025
                assert "duration_seconds" in result
                assert "message" in result
                mock_ingest.assert_called_once_with(2025, db_path)

    def test_dagster_ingest_season_schedule_failure(self) -> None:
        """Test Dagster season schedule function handles errors properly."""
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "test.duckdb"

            with patch("sports_analytics_pipeline.ingest.ingest_season_schedule_dlt") as mock_ingest:
                mock_ingest.side_effect = Exception("Test error")

                result = dagster_ingest_season_schedule(2025, db_path)

                assert result["status"] == "failed"
                assert result["season_end_year"] == 2025
                assert "error" in result
                assert result["error"] == "Test error"

    def test_dagster_ingest_daily_data_success(self) -> None:
        """Test Dagster daily data function returns proper metadata on success."""
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "test.duckdb"
            test_date = date(2024, 12, 25)

            with patch("sports_analytics_pipeline.ingest.ingest_date_dlt") as mock_ingest:
                result = dagster_ingest_daily_data(test_date, db_path)

                assert result["status"] == "success"
                assert result["target_date"] == "2024-12-25"
                assert "duration_seconds" in result
                mock_ingest.assert_called_once_with(test_date, db_path)

    def test_dagster_backfill_box_scores_success(self) -> None:
        """Test Dagster backfill function returns proper metadata on success."""
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "test.duckdb"
            start_date = date(2024, 10, 1)
            end_date = date(2024, 10, 31)

            with patch("sports_analytics_pipeline.ingest.backfill_box_scores_dlt") as mock_backfill:
                result = dagster_backfill_box_scores(
                    2025, db_path, start_date=start_date, end_date=end_date
                )

                assert result["status"] == "success"
                assert result["season_end_year"] == 2025
                assert result["start_date"] == "2024-10-01"
                assert result["end_date"] == "2024-10-31"
                mock_backfill.assert_called_once_with(
                    2025, db_path, start=start_date, end=end_date, skip_existing=True
                )


class TestBackfillLogic:
    """Test backfill functionality."""

    def test_backfill_box_scores_dlt_mock(self) -> None:
        """Test backfill logic with mocked database and ingest functions."""
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "test.duckdb"
            start_date = date(2024, 10, 1)
            end_date = date(2024, 10, 3)

            with patch("sports_analytics_pipeline.ingest.ingest_date_dlt") as mock_ingest:
                with patch("duckdb.connect") as mock_connect:
                    # Mock database connection to return no existing dates
                    mock_conn = Mock()
                    mock_connect.return_value = mock_conn
                    mock_conn.execute.return_value.fetchall.return_value = []

                    # Call backfill function
                    backfill_box_scores_dlt(
                        2025, 
                        db_path, 
                        start=start_date, 
                        end=end_date, 
                        skip_existing=True
                    )

                    # Should call ingest_date_dlt for each date
                    assert mock_ingest.call_count == 3
                    expected_dates = [date(2024, 10, 1), date(2024, 10, 2), date(2024, 10, 3)]
                    actual_dates = [call[0][0] for call in mock_ingest.call_args_list]
                    assert actual_dates == expected_dates


@pytest.mark.skipif(
    os.getenv("RUN_LIVE_TESTS") != "1", 
    reason="Live tests require RUN_LIVE_TESTS=1 environment variable"
)
class TestLiveIntegration:
    """Integration tests that make real API calls to ESPN."""

    def test_ingest_recent_date_live(self) -> None:
        """Test ingesting data for a recent date with real ESPN API."""
        import duckdb

        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "test_live.duckdb"
            test_date = date(2024, 10, 15)  # A date likely to have NBA games

            try:
                # Run the actual ingestion
                ingest_date_dlt(test_date, db_path)

                # Check that database was created and has data
                conn = duckdb.connect(str(db_path))

                # Check schedule table
                schedule_count = conn.execute("SELECT COUNT(*) FROM main.schedule").fetchone()[0]
                assert schedule_count > 0, "Schedule table should have data"

                # Check that we have some box score data
                try:
                    box_score_count = conn.execute("SELECT COUNT(*) FROM main.box_score").fetchone()[0]
                    if box_score_count > 0:
                        print(f"✓ Found {box_score_count} box score records")
                except Exception:
                    print("No box score data found (may be expected if no games on this date)")

                conn.close()

            except Exception as e:
                pytest.fail(f"Live integration test failed: {e}")

    def test_dagster_interface_live(self) -> None:
        """Test Dagster interface functions with real API calls."""
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "test_dagster_live.duckdb"
            test_date = date(2024, 10, 15)

            try:
                # Test daily ingestion
                result = dagster_ingest_daily_data(test_date, db_path)

                assert result["status"] == "success"
                assert result["target_date"] == "2024-10-15"
                assert result["duration_seconds"] > 0

                print(f"✓ Dagster daily ingestion completed in {result['duration_seconds']:.2f}s")

            except Exception as e:
                pytest.fail(f"Live Dagster interface test failed: {e}")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])