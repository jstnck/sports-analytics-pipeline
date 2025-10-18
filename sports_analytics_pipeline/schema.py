"""Minimal schema definition for the NBA data ingestion pipeline.

This module defines the staging layer schema used by dlt for raw data ingestion.
Tables are designed to be minimal and flexible, storing data in a mostly raw format
suitable for the ingestion layer of a data warehouse architecture.

Schema Philosophy:
- Minimal transformations: Light normalization only
- Flexible types: Mostly VARCHAR to handle API variations  
- No strong constraints: Let downstream (dbt) enforce business rules
- Include metadata: dlt tracking columns for lineage
- Raw data preservation: JSON blobs for complex nested data

This schema reflects what dlt actually creates, not an idealized relational model.
"""

from __future__ import annotations

from pathlib import Path

import duckdb


def _staging_schema_sql() -> str:
    """Return SQL for minimal staging tables that match dlt's auto-generated schema.
    
    These tables store raw/lightly processed data from ESPN API with minimal constraints.
    The downstream dbt layer will enforce stronger typing and business rules.
    """
    return """
        -- Core entity tables (minimal, flexible)
        CREATE TABLE IF NOT EXISTS teams (
            name VARCHAR NOT NULL,
            city VARCHAR,  -- Often NULL, filled by downstream processing
            -- dlt metadata columns
            _dlt_load_id VARCHAR NOT NULL,
            _dlt_id VARCHAR NOT NULL
        );

        CREATE TABLE IF NOT EXISTS venues (
            name VARCHAR NOT NULL,
            city VARCHAR,  -- Often NULL in API responses
            state VARCHAR, -- Often NULL in API responses
            -- dlt metadata columns  
            _dlt_load_id VARCHAR NOT NULL,
            _dlt_id VARCHAR NOT NULL
        );

        -- Schedule table: core game information (mostly raw from ESPN API)
        CREATE TABLE IF NOT EXISTS schedule (
            espn_event_id VARCHAR,  -- ESPN's unique game identifier
            date VARCHAR NOT NULL,  -- Date as string (YYYY-MM-DD format)
            start_time VARCHAR,     -- Time as string, may be "00:00:00" 
            timestamp_utc TIMESTAMP WITH TIME ZONE,  -- Parsed timestamp when available
            away_team VARCHAR NOT NULL,
            home_team VARCHAR NOT NULL,
            venue VARCHAR,          -- Venue name, may be NULL
            status VARCHAR,         -- Game status from ESPN (e.g., "STATUS_FINAL")
            home_score BIGINT,      -- Score as integer, NULL for future games
            away_score BIGINT,      -- Score as integer, NULL for future games 
            game_type VARCHAR,      -- "regular season", "playoffs", etc.
            created_at TIMESTAMP WITH TIME ZONE,  -- When this record was ingested
            -- dlt metadata columns
            _dlt_load_id VARCHAR NOT NULL,
            _dlt_id VARCHAR NOT NULL
        );

        -- Game-level box score (aggregated team stats per game)
        CREATE TABLE IF NOT EXISTS box_score (
            espn_event_id VARCHAR,
            date VARCHAR NOT NULL,
            away_team VARCHAR NOT NULL,
            home_team VARCHAR NOT NULL,
            -- Team-level aggregated stats (often NULL if not available)
            home_points BIGINT,
            away_points BIGINT,
            home_rebounds BIGINT,
            away_rebounds BIGINT,
            home_assists BIGINT,
            away_assists BIGINT,
            -- Raw JSON for full boxscore data preservation
            stats_json VARCHAR,     -- Complete ESPN boxscore response as JSON
            -- dlt metadata columns
            _dlt_load_id VARCHAR NOT NULL,
            _dlt_id VARCHAR NOT NULL
        );

        -- Player-level box score (individual player stats per game)
        CREATE TABLE IF NOT EXISTS player_box_score (
            espn_event_id VARCHAR,
            date VARCHAR NOT NULL,
            away_team VARCHAR NOT NULL,
            home_team VARCHAR NOT NULL,
            first_name VARCHAR NOT NULL,
            last_name VARCHAR NOT NULL,
            team VARCHAR,           -- Which team the player played for
            -- Basic stats (as available from ESPN API)
            minutes_played VARCHAR, -- Format: "35:45" or similar
            points BIGINT,
            rebounds BIGINT,
            assists BIGINT,
            fouls BIGINT,
            plus_minus BIGINT,
            -- Raw JSON for complete player data preservation
            stats_json VARCHAR,     -- Complete ESPN player stats as JSON
            -- dlt metadata columns
            _dlt_load_id VARCHAR NOT NULL,
            _dlt_id VARCHAR NOT NULL
        );

        -- Players table (roster information, when available)
        CREATE TABLE IF NOT EXISTS players (
            first_name VARCHAR NOT NULL,
            last_name VARCHAR NOT NULL,
            date_of_birth DATE,     -- When available from API
            season VARCHAR,         -- Season identifier (e.g., "2024-25")
            country_of_birth VARCHAR,
            -- dlt metadata columns
            _dlt_load_id VARCHAR NOT NULL,
            _dlt_id VARCHAR NOT NULL
        );
    """


def init_db(db_path: str | Path) -> None:
    """Initialize DuckDB with minimal staging schema for dlt ingestion.
    
    Creates tables that match what dlt automatically generates, allowing
    for manual schema management when needed.
    
    Args:
        db_path: Path to DuckDB database file
    """
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(database=str(db_path))
    conn.execute(_staging_schema_sql())
    conn.close()


__all__ = [
    "init_db",
]
