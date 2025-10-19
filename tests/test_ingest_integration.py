"""Integration tests for dlt-based ingestion functions.

These tests verify that the main ingestion functions work correctly
with mocked API responses, focusing on the dlt pipeline integration.
"""

from __future__ import annotations

import tempfile
from datetime import date
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock


from sports_analytics_pipeline.ingest import (
    ingest_season_schedule,
    ingest_date,
    backfill_box_scores,
)


class TestDltIngestionFunctions:
    """Test main dlt ingestion functions with mocked data."""

    @patch("sports_analytics_pipeline.ingest.rest_api_source")
    def test_ingest_season_schedule_basic(self, mock_rest_api_source: MagicMock) -> None:
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
            ingest_season_schedule(
                season_end_year=2025,
                db_path=str(db_path),
                start=start_date,
                end=end_date,
            )

            # Verify REST API source was called
            assert mock_rest_api_source.called

    @patch("sports_analytics_pipeline.ingest.rest_api_source")
    def test_ingest_date_basic(
        self, mock_rest_api_source: MagicMock
    ) -> None:
        """Test daily data ingestion with minimal mock data."""
        # Mock REST API source for both scoreboard and game summary data
        mock_scoreboard_resource = Mock()
        mock_scoreboard_resource.__iter__ = Mock(
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
                                                "team": {"displayName": "Los Angeles Lakers"},
                                            },
                                            {
                                                "homeAway": "away", 
                                                "team": {"displayName": "Boston Celtics"},
                                            },
                                        ],
                                    }
                                ],
                            }
                        ]
                    }
                ]
            )
        )

        mock_summary_resource = Mock()
        mock_summary_resource.__iter__ = Mock(
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

        # Mock different responses for different calls
        def mock_rest_api_side_effect(config):
            mock_source = Mock()
            # Check if this is a scoreboard or summary call based on resources
            if config.get("resources", [{}])[0].get("name") == "scoreboard_data":
                mock_source.resources = {"scoreboard_data": mock_scoreboard_resource}
            else:
                mock_source.resources = {"summary_000": mock_summary_resource}
            return mock_source

        mock_rest_api_source.side_effect = mock_rest_api_side_effect

        # Use a temporary database
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "test.duckdb"

            # Test the function doesn't crash
            target_date = date(2024, 10, 15)

            # Should not raise an exception
            ingest_date(target_date=target_date, db_path=str(db_path))

            # Verify REST API source was called
            assert mock_rest_api_source.called

    def test_backfill_box_scores_date_range(self) -> None:
        """Test that backfill function handles date ranges correctly."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "test.duckdb"

            # Test with custom date range
            with patch(
                "sports_analytics_pipeline.ingest.ingest_date"
            ) as mock_ingest:
                backfill_box_scores(
                    season_end_year=2025,
                    db_path=str(db_path),
                    start=date(2024, 10, 15),
                    end=date(2024, 10, 16),  # 2 days
                )

                # Should call ingest_date for each day
                assert mock_ingest.call_count == 2

                # Check the dates passed
                calls = mock_ingest.call_args_list
                assert calls[0][0][0] == date(2024, 10, 15)  # First positional arg
                assert calls[1][0][0] == date(2024, 10, 16)  # First positional arg

    def test_backfill_box_scores_default_dates(self) -> None:
        """Test that backfill function uses correct default date range."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "test.duckdb"

            with patch(
                "sports_analytics_pipeline.ingest.ingest_date"
            ) as mock_ingest:
                backfill_box_scores(
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


