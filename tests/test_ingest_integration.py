"""Integration tests for dlt-based ingestion functions.

These tests verify that the main ingestion functions work correctly
with mocked API responses, focusing on the dlt pipeline integration.
"""

from __future__ import annotations

import tempfile
from datetime import date
from pathlib import Path
from unittest.mock import Mock, patch


from sports_analytics_pipeline.ingest import (
    ingest_season_schedule_dlt,
    ingest_date_dlt,
    backfill_box_scores_dlt,
)


class TestDltIngestionFunctions:
    """Test main dlt ingestion functions with mocked data."""

    @patch("sports_analytics_pipeline.ingest.rest_api_source")
    def test_ingest_season_schedule_dlt_basic(self, mock_rest_api_source) -> None:
        """Test season schedule ingestion with minimal mock data."""
        # Mock the REST API source and response
        mock_resource = Mock()
        mock_resource.__iter__ = Mock(
            return_value=iter(
                [
                    {
                        "events": [
                            {
                                "id": "401654321",
                                "date": "2024-10-15T20:00:00Z",
                                "competitions": [
                                    {
                                        "competitors": [
                                            {
                                                "homeAway": "home",
                                                "score": "110",
                                                "team": {
                                                    "displayName": "Los Angeles Lakers"
                                                },
                                            },
                                            {
                                                "homeAway": "away",
                                                "score": "105",
                                                "team": {
                                                    "displayName": "Boston Celtics"
                                                },
                                            },
                                        ],
                                        "venue": {"fullName": "Crypto.com Arena"},
                                        "status": {"type": {"name": "STATUS_FINAL"}},
                                    }
                                ],
                            }
                        ]
                    }
                ]
            )
        )

        mock_source = Mock()
        mock_source.resources = {"scoreboard_data": mock_resource}
        mock_rest_api_source.return_value = mock_source

        # Use a temporary database
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "test.duckdb"

            # Test the function doesn't crash
            start_date = date(2024, 10, 15)
            end_date = date(2024, 10, 15)

            # Should not raise an exception
            ingest_season_schedule_dlt(
                season_end_year=2025,
                db_path=str(db_path),
                start=start_date,
                end=end_date,
            )

            # Verify REST API source was called
            assert mock_rest_api_source.called

    @patch("sports_analytics_pipeline.ingest.rest_api_source")
    @patch("sports_analytics_pipeline.ingest.schedule_resource")
    def test_ingest_date_dlt_basic(
        self, mock_schedule_resource, mock_rest_api_source
    ) -> None:
        """Test daily data ingestion with minimal mock data."""
        # Mock schedule resource to return some game data
        mock_schedule_resource.return_value = [
            {
                "espn_event_id": "401654321",
                "date": "2024-10-15",
                "away_team": "Boston Celtics",
                "home_team": "Los Angeles Lakers",
            }
        ]

        # Mock REST API source for summary data
        mock_resource = Mock()
        mock_resource.__iter__ = Mock(
            return_value=iter(
                [
                    {
                        "boxscore": {
                            "teams": [
                                {
                                    "team": {"displayName": "Los Angeles Lakers"},
                                    "statistics": {
                                        "points": 110,
                                        "rebounds": 45,
                                        "assists": 25,
                                    },
                                },
                                {
                                    "team": {"displayName": "Boston Celtics"},
                                    "statistics": {
                                        "points": 105,
                                        "rebounds": 42,
                                        "assists": 22,
                                    },
                                },
                            ]
                        }
                    }
                ]
            )
        )

        mock_source = Mock()
        mock_source.resources = {"summary_000": mock_resource}
        mock_rest_api_source.return_value = mock_source

        # Use a temporary database
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "test.duckdb"

            # Test the function doesn't crash
            target_date = date(2024, 10, 15)

            # Should not raise an exception
            ingest_date_dlt(target_date=target_date, db_path=str(db_path))

            # Verify schedule resource was called
            assert mock_schedule_resource.called

    def test_backfill_box_scores_dlt_date_range(self) -> None:
        """Test that backfill function handles date ranges correctly."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "test.duckdb"

            # Test with custom date range
            with patch(
                "sports_analytics_pipeline.ingest.ingest_date_dlt"
            ) as mock_ingest:
                backfill_box_scores_dlt(
                    season_end_year=2025,
                    db_path=str(db_path),
                    start=date(2024, 10, 15),
                    end=date(2024, 10, 16),  # 2 days
                )

                # Should call ingest_date_dlt for each day
                assert mock_ingest.call_count == 2

                # Check the dates passed
                calls = mock_ingest.call_args_list
                assert calls[0][0][0] == date(2024, 10, 15)  # First positional arg
                assert calls[1][0][0] == date(2024, 10, 16)  # First positional arg

    def test_backfill_box_scores_dlt_default_dates(self) -> None:
        """Test that backfill function uses correct default date range."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "test.duckdb"

            with patch(
                "sports_analytics_pipeline.ingest.ingest_date_dlt"
            ) as mock_ingest:
                backfill_box_scores_dlt(
                    season_end_year=2025,
                    db_path=str(db_path),
                )

                # Should be called (for dates between 2024-10-01 and 2025-06-30)
                assert mock_ingest.call_count > 0

                # Check first and last calls use expected date range
                calls = mock_ingest.call_args_list
                first_date = calls[0][0][0]  # First positional arg
                last_date = calls[-1][0][0]  # First positional arg

                assert first_date >= date(2024, 10, 1)
                assert last_date <= date(2025, 6, 30)


class TestResourceConfiguration:
    """Test that dlt resources are configured correctly."""

    def test_schedule_resource_date_range_iteration(self) -> None:
        """Test that schedule resource iterates through date range correctly."""
        from sports_analytics_pipeline.ingest import _date_range

        start = date(2024, 10, 15)
        end = date(2024, 10, 17)

        dates = list(_date_range(start, end))

        assert len(dates) == 3
        assert dates[0] == date(2024, 10, 15)
        assert dates[1] == date(2024, 10, 16)
        assert dates[2] == date(2024, 10, 17)

    def test_nba_season_constants(self) -> None:
        """Test that NBA season constants are defined correctly."""
        from sports_analytics_pipeline.ingest import (
            NBA_SEASON_START_MONTH,
            NBA_SEASON_START_DAY,
            NBA_SEASON_END_MONTH,
            NBA_SEASON_END_DAY,
        )

        # NBA season should start in October
        assert NBA_SEASON_START_MONTH == 10
        assert NBA_SEASON_START_DAY == 1

        # NBA season should end in June
        assert NBA_SEASON_END_MONTH == 6
        assert NBA_SEASON_END_DAY == 30

    def test_api_configuration_constants(self) -> None:
        """Test that API configuration constants are defined correctly."""
        from sports_analytics_pipeline.ingest import (
            USER_AGENT,
            BASE_URL,
            SCOREBOARD_URL,
            SUMMARY_URL,
        )

        # User agent should be defined
        assert USER_AGENT is not None
        assert "sports-analytics-pipeline" in USER_AGENT

        # URLs should be ESPN API endpoints
        assert (
            BASE_URL == "https://site.api.espn.com/apis/site/v2/sports/basketball/nba"
        )
        assert SCOREBOARD_URL == f"{BASE_URL}/scoreboard"
        assert SUMMARY_URL == f"{BASE_URL}/summary"
