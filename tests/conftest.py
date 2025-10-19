"""Test configuration and fixtures for the sports analytics pipeline."""

from __future__ import annotations

import pytest
import tempfile
from pathlib import Path
from typing import Any, Generator


@pytest.fixture
def temp_db_path() -> Generator[Path, None, None]:
    """Provide a temporary database path for tests."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        yield Path(tmp_dir) / "test_games.duckdb"


@pytest.fixture
def sample_scoreboard_event() -> dict[str, Any]:
    """Sample ESPN scoreboard event for testing."""
    return {
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


@pytest.fixture
def sample_player_stats() -> dict[str, Any]:
    """Sample ESPN player stats for testing."""
    return {
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
