from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

from bball_season.storage import (
    ingest_schedule,
    ingest_players,
    ingest_box_scores,
    ingest_player_box_scores,
)


def test_ingest_all(tmp_path: Path) -> None:
    """Smoke test for storage ingestion helpers using a temp DuckDB file.

    This test creates minimal DataFrames for schedule, players, team box scores,
    and player box scores, runs the ingestion helpers, and queries the DB to
    assert the expected rows were inserted.
    """
    db_path = tmp_path / "test_games.duckdb"

    # Minimal schedule row
    schedule_df = pd.DataFrame(
        [
            {
                "espn_event_id": "401716947",
                "date": "2024-10-04",
                "start_time": "00:00:00",
                "away_team": "Warriors",
                "home_team": "Lakers",
                "venue": "Staples Center",
                "home_score": 102,
                "away_score": 99,
                "game_type": "pre-season",
            }
        ]
    )

    # Minimal players
    players_df = pd.DataFrame(
        [
            {
                "first_name": "LeBron",
                "last_name": "James",
                "date_of_birth": "1984-12-30",
                "season": "2024-25",
                "country_of_birth": "USA",
            }
        ]
    )

    # Team box score
    box_df = pd.DataFrame(
        [
            {
                "espn_event_id": "401716947",
                "date": "2024-10-04",
                "away_team": "Warriors",
                "home_team": "Lakers",
                "team": "Lakers",
                "points": 102,
                "rebounds": 45,
                "assists": 25,
                "fouls": 18,
                "plus_minus": 3,
                "stats_json": "{}",
            }
        ]
    )

    # Player box score
    player_box_df = pd.DataFrame(
        [
            {
                "espn_event_id": "401716947",
                "date": "2024-10-04",
                "away_team": "Warriors",
                "home_team": "Lakers",
                "first_name": "LeBron",
                "last_name": "James",
                "team": "Lakers",
                "minutes_played": "35:12",
                "points": 30,
                "rebounds": 8,
                "assists": 10,
                "fouls": 2,
                "plus_minus": 5,
                "stats_json": "{}",
            }
        ]
    )

    # Run ingests
    ingest_schedule(schedule_df, db_path=db_path)
    ingest_players(players_df, db_path=db_path)
    ingest_box_scores(box_df, db_path=db_path)
    ingest_player_box_scores(player_box_df, db_path=db_path)

    # Open DB and assert counts and a sample value
    conn = duckdb.connect(database=str(db_path))
    schedule_count = conn.execute("SELECT COUNT(*) FROM schedule").fetchone()[0]
    assert schedule_count == 1

    players_count = conn.execute("SELECT COUNT(*) FROM players").fetchone()[0]
    assert players_count == 1

    box_count = conn.execute("SELECT COUNT(*) FROM box_score").fetchone()[0]
    assert box_count == 1

    pbox_count = conn.execute("SELECT COUNT(*) FROM player_box_score").fetchone()[0]
    assert pbox_count == 1

    # spot-check a value
    pts = conn.execute(
        "SELECT points FROM player_box_score WHERE first_name='LeBron' AND last_name='James'")
    assert pts.fetchone()[0] == 30

    conn.close()
