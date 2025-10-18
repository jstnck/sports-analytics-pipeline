"""Tests for data transformation functions in ingest.py.

These tests focus on the core transformation logic without
external dependencies or orchestration concerns.
"""

from __future__ import annotations


from sports_analytics_pipeline.ingest import (
    _transform_scoreboard_event,
    _transform_player_stats,
    _extract_teams_from_schedule,
    _extract_venues_from_schedule,
)


class TestScoreboardTransformation:
    """Test scoreboard event transformation to schedule format."""

    def test_transform_scoreboard_event_complete(self) -> None:
        """Test complete scoreboard event with all fields."""
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
                    "status": {"type": {"name": "STATUS_FINAL"}},
                }
            ],
        }

        result = _transform_scoreboard_event(mock_event)

        # Test core fields
        assert result["espn_event_id"] == "401654321"
        assert result["date"] == "2024-12-25"
        assert result["timestamp_utc"] == "2024-12-25T20:00:00+00:00"
        assert result["away_team"] == "Boston Celtics"
        assert result["home_team"] == "Los Angeles Lakers"
        assert result["venue"] == "Crypto.com Arena"
        assert result["status"] == "STATUS_FINAL"
        assert result["home_score"] == 110
        assert result["away_score"] == 105
        assert result["game_type"] == "regular season"

        # Test that created_at is recent
        assert "created_at" in result
        assert result["created_at"] is not None

    def test_transform_scoreboard_event_minimal(self) -> None:
        """Test minimal scoreboard event with missing fields."""
        mock_event = {
            "id": "401654322",
        }

        result = _transform_scoreboard_event(mock_event)

        # Should handle missing data gracefully
        assert result["espn_event_id"] == "401654322"
        assert result["date"] is None
        assert result["away_team"] is None
        assert result["home_team"] is None
        assert result["venue"] is None
        assert result["status"] == "unknown"
        assert result["home_score"] is None
        assert result["away_score"] is None

    def test_transform_scoreboard_event_invalid_score(self) -> None:
        """Test handling of invalid score data."""
        mock_event = {
            "id": "401654323",
            "competitions": [
                {
                    "competitors": [
                        {
                            "homeAway": "home",
                            "score": "invalid",  # Invalid score
                            "team": {"displayName": "Team A"},
                        },
                        {
                            "homeAway": "away",
                            "score": None,  # Null score
                            "team": {"displayName": "Team B"},
                        },
                    ],
                }
            ],
        }

        result = _transform_scoreboard_event(mock_event)

        # Should handle invalid scores gracefully
        assert result["home_score"] is None
        assert result["away_score"] is None
        assert result["home_team"] == "Team A"
        assert result["away_team"] == "Team B"


class TestPlayerStatsTransformation:
    """Test player statistics transformation."""

    def test_transform_player_stats_complete(self) -> None:
        """Test complete player stats transformation."""
        mock_player_data = {
            "athlete": {
                "firstName": "LeBron",
                "lastName": "James",
            },
            "team": {"displayName": "Los Angeles Lakers"},
            "stats": [
                {"name": "minutes", "value": "35:45"},
                {"name": "points", "value": "28"},
                {"name": "rebounds", "value": "8"},
                {"name": "assists", "value": "11"},
                {"name": "fouls", "value": "2"},
                {"name": "plusMinus", "value": "+15"},
            ],
        }

        result = _transform_player_stats(
            mock_player_data,
            event_id="401654321",
            game_date="2024-12-25",
            away_team="Boston Celtics",
            home_team="Los Angeles Lakers",
        )

        assert result["espn_event_id"] == "401654321"
        assert result["date"] == "2024-12-25"
        assert result["away_team"] == "Boston Celtics"
        assert result["home_team"] == "Los Angeles Lakers"
        assert result["first_name"] == "LeBron"
        assert result["last_name"] == "James"
        assert result["team"] == "Los Angeles Lakers"
        assert result["minutes_played"] == "35:45"
        assert result["points"] == 28
        assert result["rebounds"] == 8
        assert result["assists"] == 11
        assert result["fouls"] == 2
        assert result["plus_minus"] == 15

    def test_transform_player_stats_missing_data(self) -> None:
        """Test player stats with missing data."""
        mock_player_data = {
            "athlete": {
                "displayName": "John Doe",  # Only display name, no first/last
            },
            "stats": [],  # No stats
        }

        result = _transform_player_stats(
            mock_player_data,
            event_id="401654321",
            game_date="2024-12-25",
            away_team="Team A",
            home_team="Team B",
        )

        assert result["first_name"] == "John"
        assert result["last_name"] == "Doe"
        assert result["team"] is None
        assert result["minutes_played"] is None
        assert result["points"] is None
        assert result["rebounds"] is None
        assert result["assists"] is None


class TestDataExtraction:
    """Test data extraction helper functions."""

    def test_extract_teams_from_schedule(self) -> None:
        """Test unique team extraction from schedule records."""
        schedule_records = [
            {"home_team": "Lakers", "away_team": "Celtics"},
            {"home_team": "Warriors", "away_team": "Lakers"},  # Lakers repeated
            {"home_team": None, "away_team": "Heat"},  # Missing home team
        ]

        teams = list(_extract_teams_from_schedule(schedule_records))
        team_names = {team["name"] for team in teams}

        # Should extract unique teams only
        assert team_names == {"Lakers", "Celtics", "Warriors", "Heat"}

    def test_extract_venues_from_schedule(self) -> None:
        """Test unique venue extraction from schedule records."""
        schedule_records = [
            {"venue": "Crypto.com Arena"},
            {"venue": "TD Garden"},
            {"venue": "Crypto.com Arena"},  # Repeated
            {"venue": None},  # Missing venue
        ]

        venues = list(_extract_venues_from_schedule(schedule_records))
        venue_names = {venue["name"] for venue in venues}

        # Should extract unique venues only
        assert venue_names == {"Crypto.com Arena", "TD Garden"}

        # Check venue structure
        for venue in venues:
            assert venue["name"] is not None
            assert venue["city"] is None  # TODO: enhance with city data
            assert venue["state"] is None
