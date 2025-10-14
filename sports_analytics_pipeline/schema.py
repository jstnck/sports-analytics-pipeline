"""Database schema (DDL) helpers for DuckDB.

This module centralizes the SQL schema for the project and exposes a single
`init_db` function that ensures all required tables exist.
"""

from __future__ import annotations

from pathlib import Path

import duckdb


def _schema_sql() -> str:
    """Return the SQL string that creates all required tables if missing."""
    return (
        """
        CREATE TABLE IF NOT EXISTS teams (
            name TEXT PRIMARY KEY,
            city TEXT
        );

        CREATE TABLE IF NOT EXISTS venues (
            name TEXT PRIMARY KEY,
            city TEXT,
            state TEXT
        );

        CREATE TABLE IF NOT EXISTS players (
            first_name TEXT,
            last_name TEXT,
            date_of_birth DATE,
            season TEXT,
            country_of_birth TEXT,
            PRIMARY KEY (first_name, last_name, date_of_birth)
        );

        CREATE TABLE IF NOT EXISTS schedule (
            espn_event_id TEXT,
            date DATE,
            start_time TEXT,
            away_team TEXT REFERENCES teams(name),
            home_team TEXT REFERENCES teams(name),
            venue TEXT REFERENCES venues(name),
            status TEXT,
            home_score INTEGER,
            away_score INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            game_type TEXT,
            UNIQUE(date, away_team, home_team)
        );

        CREATE TABLE IF NOT EXISTS box_score (
            espn_event_id TEXT,
            date DATE,
            away_team TEXT,
            home_team TEXT,
            home_points INTEGER,
            away_points INTEGER,
            home_rebounds INTEGER,
            away_rebounds INTEGER,
            home_assists INTEGER,
            away_assists INTEGER,
            stats_json TEXT,
            PRIMARY KEY (date, away_team, home_team)
        );

        CREATE TABLE IF NOT EXISTS player_box_score (
            espn_event_id TEXT,
            date DATE,
            away_team TEXT,
            home_team TEXT,
            first_name TEXT,
            last_name TEXT,
            team TEXT,
            minutes_played TEXT,
            points INTEGER,
            rebounds INTEGER,
            assists INTEGER,
            fouls INTEGER,
            plus_minus INTEGER,
            stats_json TEXT,
            PRIMARY KEY (date, away_team, home_team, first_name, last_name)
        );
        """
    )


def init_db(db_path: str | Path) -> None:
    """Create the DuckDB database file and required tables if they don't exist."""
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(database=str(db_path))
    conn.execute(_schema_sql())
    conn.close()


__all__ = ["init_db", "_schema_sql"]
