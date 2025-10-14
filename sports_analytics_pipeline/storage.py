"""DuckDB storage helpers for the sports_analytics_pipeline project.

This module creates a small relational schema and provides helpers to ingest
DataFrames produced by the ingest layer into normalized tables:
- teams (name, city)
- venues (name, city, state)
- schedule (date, start_time, away_team, home_team, venue, espn_event_id, ...)
- box_score (team-level box stats)
- player_box_score (player-level box stats)

The ingest is idempotent for teams and venues (new names are inserted,
existing ones are preserved). Schedule and box score inserts avoid duplicates
using natural/composite keys.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
from .schema import init_db


def init_db(db_path: str | Path) -> None:  # re-exported for backward compatibility
    from .schema import init_db as _init

    _init(db_path)


def ingest_schedule(df: pd.DataFrame, db_path: str | Path = "data/games.duckdb") -> None:
    """Ingest a DataFrame of schedule rows into DuckDB.

    Expected DataFrame columns: date, start_time, away_team, home_team, venue,
    espn_event_id (optional), home_score, away_score, status, game_type.

    The function upserts missing teams/venues and inserts schedule rows while
    avoiding duplicates by the composite key (date, away_team, home_team).
    """
    if df.empty:
        return

    init_db(db_path)
    conn = duckdb.connect(database=str(db_path))

    conn.register("incoming", df)

    # Upsert teams
    conn.execute(
        """
        INSERT INTO teams(name)
        SELECT name FROM (
            SELECT DISTINCT away_team AS name FROM incoming WHERE away_team IS NOT NULL
            UNION
            SELECT DISTINCT home_team AS name FROM incoming WHERE home_team IS NOT NULL
        ) AS names
        WHERE name NOT IN (SELECT name FROM teams)
        """
    )

    # Upsert venues
    conn.execute(
        """
        INSERT INTO venues(name)
        SELECT name FROM (
            SELECT DISTINCT venue AS name FROM incoming WHERE venue IS NOT NULL
        ) AS names
        WHERE name NOT IN (SELECT name FROM venues)
        """
    )

    # Insert schedule rows. Deduplicate incoming batch by natural key.
    conn.execute(
        """
        WITH dedup AS (
            SELECT
                CAST(date AS DATE) AS date_cast,
                COALESCE(start_time, '') AS start_time,
                away_team,
                home_team,
                MAX(espn_event_id) AS espn_event_id,
                MAX(venue) AS venue,
                MAX(home_score) AS home_score,
                MAX(away_score) AS away_score,
                MAX(game_type) AS game_type
            FROM incoming
            GROUP BY date_cast, start_time, away_team, home_team
        )
        INSERT INTO schedule(espn_event_id, date, start_time, away_team, home_team, venue, status, home_score, away_score, game_type)
        SELECT
            d.espn_event_id,
            d.date_cast,
            d.start_time,
            d.away_team,
            d.home_team,
            d.venue,
            NULL,
            d.home_score,
            d.away_score,
            d.game_type
        FROM dedup d
        WHERE NOT EXISTS (
            SELECT 1 FROM schedule s
            WHERE s.date = d.date_cast
              AND s.away_team = d.away_team
              AND s.home_team = d.home_team
        )
        """
    )

    conn.unregister("incoming")
    conn.close()


def ingest_players(df: pd.DataFrame, db_path: str | Path = "data/games.duckdb") -> None:
    """Ingest player rows into the `players` table.

    Expected DataFrame columns: first_name, last_name, date_of_birth, season,
    country_of_birth. Rows are deduplicated in the incoming batch and only
    inserted if the (first_name,last_name,date_of_birth) key does not exist.
    """
    if df.empty:
        return

    init_db(db_path)
    conn = duckdb.connect(database=str(db_path))
    conn.register("incoming", df)

    conn.execute(
        """
        WITH dedup AS (
            SELECT
                first_name,
                last_name,
                CAST(date_of_birth AS DATE) AS date_of_birth,
                MAX(season) AS season,
                MAX(country_of_birth) AS country_of_birth
            FROM incoming
            GROUP BY first_name, last_name, CAST(date_of_birth AS DATE)
        )
        INSERT INTO players(first_name, last_name, date_of_birth, season, country_of_birth)
        SELECT
            d.first_name,
            d.last_name,
            d.date_of_birth,
            d.season,
            d.country_of_birth
        FROM dedup d
        WHERE NOT EXISTS (
            SELECT 1 FROM players p
            WHERE p.first_name = d.first_name
              AND p.last_name = d.last_name
              AND p.date_of_birth = d.date_of_birth
        )
        """
    )

    conn.unregister("incoming")
    conn.close()


def ingest_box_scores(df: pd.DataFrame, db_path: str | Path = "data/games.duckdb") -> None:
    """Ingest game-level box score rows into the `box_score` table.

    Expected DataFrame columns: espn_event_id, date, away_team, home_team,
    home_points, away_points, home_rebounds, away_rebounds, home_assists,
    away_assists, stats_json.
    """
    if df.empty:
        return

    init_db(db_path)
    conn = duckdb.connect(database=str(db_path))
    conn.register("incoming", df)
    conn.execute(
        """
        WITH dedup AS (
            SELECT
                MAX(espn_event_id) AS espn_event_id,
                CAST(date AS DATE) AS date_cast,
                away_team,
                home_team,
                MAX(home_points) AS home_points,
                MAX(away_points) AS away_points,
                MAX(home_rebounds) AS home_rebounds,
                MAX(away_rebounds) AS away_rebounds,
                MAX(home_assists) AS home_assists,
                MAX(away_assists) AS away_assists,
                MAX(stats_json) AS stats_json
            FROM incoming
            GROUP BY CAST(date AS DATE), away_team, home_team
        )
        INSERT INTO box_score(espn_event_id, date, away_team, home_team, home_points, away_points, home_rebounds, away_rebounds, home_assists, away_assists, stats_json)
        SELECT
            d.espn_event_id,
            d.date_cast,
            d.away_team,
            d.home_team,
            d.home_points,
            d.away_points,
            d.home_rebounds,
            d.away_rebounds,
            d.home_assists,
            d.away_assists,
            d.stats_json
        FROM dedup d
        WHERE NOT EXISTS (
            SELECT 1 FROM box_score b
            WHERE b.date = d.date_cast
              AND b.away_team = d.away_team
              AND b.home_team = d.home_team
        )
        """
    )

    conn.unregister("incoming")
    conn.close()


def ingest_player_box_scores(df: pd.DataFrame, db_path: str | Path = "data/games.duckdb") -> None:
    """Ingest player-level box score rows into the `player_box_score` table.

    Expected DataFrame columns: espn_event_id, date, away_team, home_team,
    first_name, last_name, team, minutes_played, points, rebounds, assists,
    fouls, plus_minus, stats_json.
    """
    if df.empty:
        return

    init_db(db_path)
    conn = duckdb.connect(database=str(db_path))
    conn.register("incoming", df)

    conn.execute(
        """
        WITH dedup AS (
            SELECT
                MAX(espn_event_id) AS espn_event_id,
                CAST(date AS DATE) AS date_cast,
                away_team,
                home_team,
                first_name,
                last_name,
                MAX(team) AS team,
                MAX(minutes_played) AS minutes_played,
                MAX(points) AS points,
                MAX(rebounds) AS rebounds,
                MAX(assists) AS assists,
                MAX(fouls) AS fouls,
                MAX(plus_minus) AS plus_minus,
                MAX(stats_json) AS stats_json
            FROM incoming
            GROUP BY CAST(date AS DATE), away_team, home_team, first_name, last_name
        )
        INSERT INTO player_box_score(espn_event_id, date, away_team, home_team, first_name, last_name, team, minutes_played, points, rebounds, assists, fouls, plus_minus, stats_json)
        SELECT
            d.espn_event_id,
            d.date_cast,
            d.away_team,
            d.home_team,
            d.first_name,
            d.last_name,
            d.team,
            d.minutes_played,
            d.points,
            d.rebounds,
            d.assists,
            d.fouls,
            d.plus_minus,
            d.stats_json
        FROM dedup d
        WHERE NOT EXISTS (
            SELECT 1 FROM player_box_score p
            WHERE p.date = d.date_cast
              AND p.away_team = d.away_team
              AND p.home_team = d.home_team
              AND p.first_name = d.first_name
              AND p.last_name = d.last_name
        )
        """
    )

    conn.unregister("incoming")
    conn.close()


__all__ = [
    "init_db",
    "ingest_schedule",
    "dedupe_schedule",
    "ingest_players",
    "ingest_box_scores",
    "ingest_player_box_scores",
]


def dedupe_schedule(db_path: str | Path = "data/games.duckdb") -> None:
    """Remove duplicate rows from the `schedule` table.

    Duplicates are defined by the natural key (date, away_team, home_team).
    The function keeps one row per key and preserves max values for scores/venue/status.
    """
    conn = duckdb.connect(database=str(db_path))
    # Create a deduplicated snapshot grouped by the natural key
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS schedule_dedup AS
        SELECT
            CAST(date AS DATE) AS date,
            COALESCE(start_time, '') AS start_time,
            away_team,
            home_team,
            MAX(espn_event_id) AS espn_event_id,
            MAX(venue) AS venue,
            MAX(status) AS status,
            MAX(home_score) AS home_score,
            MAX(away_score) AS away_score,
            MAX(created_at) AS created_at,
            MAX(game_type) AS game_type
        FROM schedule
        GROUP BY date, start_time, away_team, home_team
        """
    )

    # Replace the schedule table with the deduped version
    conn.execute("DROP TABLE schedule;")
    conn.execute(
        """
        CREATE TABLE schedule AS
        SELECT * FROM schedule_dedup;
        """
    )
    conn.execute("DROP TABLE schedule_dedup;")
    conn.close()
