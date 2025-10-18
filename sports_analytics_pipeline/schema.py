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
    """Return SQL for minimal staging tables that match what dlt will actually create.
    
    Schema designed around:
    - Our actual resource yield statements
    - max_table_nesting=3 automatic unpacking by dlt
    - Native JSON types for complex nested data
    - Minimal but predictable structure for downstream consumption
    """
    return """
        -- Core entity tables (stable structure)
        CREATE TABLE IF NOT EXISTS teams (
            name VARCHAR NOT NULL,
            -- dlt metadata columns
            _dlt_load_id VARCHAR NOT NULL,
            _dlt_id VARCHAR NOT NULL
        );

        CREATE TABLE IF NOT EXISTS venues (
            name VARCHAR NOT NULL,
            city VARCHAR,      -- Often NULL in API responses
            state VARCHAR,     -- Often NULL in API responses
            -- dlt metadata columns  
            _dlt_load_id VARCHAR NOT NULL,
            _dlt_id VARCHAR NOT NULL
        );

        -- Schedule table: core game information
        CREATE TABLE IF NOT EXISTS schedule (
            espn_event_id VARCHAR,
            date VARCHAR NOT NULL,
            start_time VARCHAR,
            timestamp_utc TIMESTAMP WITH TIME ZONE,
            away_team VARCHAR NOT NULL,
            home_team VARCHAR NOT NULL,
            venue VARCHAR,
            status VARCHAR,
            home_score BIGINT,
            away_score BIGINT,
            game_type VARCHAR,
            created_at TIMESTAMP WITH TIME ZONE,
            -- dlt metadata columns
            _dlt_load_id VARCHAR NOT NULL,
            _dlt_id VARCHAR NOT NULL
        );

        -- Box score table: game-level data with JSON unpacking
        -- Based on: yield {"espn_event_id", "date", "away_team", "home_team", "boxscore", "header", "rosters"}
        -- With max_table_nesting=3, dlt will create columns like:
        -- boxscore__teams__0__team__displayName, boxscore__teams__0__statistics__points, etc.
        CREATE TABLE IF NOT EXISTS box_score (
            espn_event_id VARCHAR NOT NULL,
            date VARCHAR NOT NULL,
            away_team VARCHAR NOT NULL,
            home_team VARCHAR NOT NULL,
            -- Auto-generated columns from max_table_nesting=3 (examples):
            -- boxscore__teams (JSON array that couldn't be unpacked further)
            -- header__competitions__0__competitors (partially unpacked game info)
            -- rosters (JSON for player roster data)
            boxscore JSON,     -- Deep nested data that exceeds max_table_nesting
            header JSON,       -- Game header info that exceeds max_table_nesting  
            rosters JSON,      -- Player roster data
            -- dlt metadata columns
            _dlt_load_id VARCHAR NOT NULL,
            _dlt_id VARCHAR NOT NULL
        );

        -- Player box score table: individual player data
        -- Based on: yield {"espn_event_id", "player_id", "date", "away_team", "home_team", "player_data"}
        -- With max_table_nesting=3, dlt will create columns like:
        -- player_data__athlete__displayName, player_data__team__displayName, etc.
        CREATE TABLE IF NOT EXISTS player_box_score (
            espn_event_id VARCHAR NOT NULL,
            player_id VARCHAR NOT NULL,
            date VARCHAR NOT NULL,
            away_team VARCHAR NOT NULL,
            home_team VARCHAR NOT NULL,
            -- Auto-generated columns from max_table_nesting=3 (examples):
            -- player_data__athlete__displayName, player_data__athlete__firstName
            -- player_data__team__displayName
            player_data JSON,  -- Deep nested stats arrays that exceed max_table_nesting
            -- dlt metadata columns
            _dlt_load_id VARCHAR NOT NULL,
            _dlt_id VARCHAR NOT NULL
        );

        -- Players table (roster information)
        CREATE TABLE IF NOT EXISTS players (
            first_name VARCHAR NOT NULL,
            last_name VARCHAR NOT NULL,
            date_of_birth DATE,
            season VARCHAR,
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
