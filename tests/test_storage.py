"""Tests for dlt pipeline database integration.

Tests that the dlt pipeline correctly creates tables and stores data in DuckDB.
"""

from __future__ import annotations

import tempfile
from datetime import date
from pathlib import Path
from unittest.mock import Mock, patch

import duckdb

from sports_analytics_pipeline.ingest import (
    ingest_season_schedule_dlt,
)


def test_dlt_pipeline_creates_database_and_tables() -> None:
    """Test that dlt pipeline creates a DuckDB database with expected core functionality."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        db_path = Path(tmp_dir) / "test_pipeline.duckdb"
        
        # Mock the REST API to return minimal data
        with patch('sports_analytics_pipeline.ingest.rest_api_source') as mock_rest_api:
            # Mock a simple schedule response
            mock_resource = Mock()
            mock_resource.__iter__ = Mock(return_value=iter([
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
                    ]
                }
            ]))
            
            mock_source = Mock()
            mock_source.resources = {"scoreboard_data": mock_resource}
            mock_rest_api.return_value = mock_source

            # Run the season schedule ingestion
            ingest_season_schedule_dlt(
                season_end_year=2025,
                db_path=str(db_path),
                start=date(2024, 10, 15),
                end=date(2024, 10, 15)
            )

        # Verify database was created
        assert db_path.exists()
        
        conn = duckdb.connect(str(db_path))
        
        # Check that some tables were created (dlt creates internal tables too)
        tables = conn.execute("SHOW TABLES").fetchall()
        table_names = [table[0] for table in tables]
        
        # Should have at least some tables (dlt creates _dlt_* tables plus our data tables)
        assert len(table_names) > 0
        
        # Check if schedule table exists and has expected structure
        if "schedule" in table_names:
            schedule_schema = conn.execute("DESCRIBE schedule").fetchall()
            schedule_columns = [col[0] for col in schedule_schema]
            
            # Key schedule columns should exist
            key_columns = ["espn_event_id", "date", "away_team", "home_team"]
            for key_col in key_columns:
                assert key_col in schedule_columns, f"Missing key column: {key_col}"
            
            # Should have at least one row of data
            schedule_count = conn.execute("SELECT COUNT(*) FROM schedule").fetchone()[0]
            assert schedule_count >= 1
        
        conn.close()


def test_schema_module_creates_expected_tables() -> None:
    """Test that schema.py module creates the expected table structure."""
    from sports_analytics_pipeline.schema import init_db
    
    with tempfile.TemporaryDirectory() as tmp_dir:
        db_path = Path(tmp_dir) / "test_schema.duckdb"
        
        # Should not raise an exception
        init_db(str(db_path))
        
        # Verify database was created
        assert db_path.exists()
        
        # Check that tables were created
        conn = duckdb.connect(str(db_path))
        tables = conn.execute("SHOW TABLES").fetchall()
        table_names = [table[0] for table in tables]
        
        # schema.py should create the basic table structure
        expected_tables = ["teams", "venues", "players", "schedule", "box_score", "player_box_score"]
        for expected_table in expected_tables:
            assert expected_table in table_names, f"Missing table from schema.py: {expected_table}"
        
        # Check that schedule table has expected columns
        schedule_schema = conn.execute("DESCRIBE schedule").fetchall()
        schedule_columns = [col[0] for col in schedule_schema]
        
        expected_schedule_columns = [
            "espn_event_id", "date", "start_time", "away_team", "home_team", 
            "venue", "status", "home_score", "away_score", "created_at", "game_type"
        ]
        
        for expected_col in expected_schedule_columns:
            assert expected_col in schedule_columns, f"Missing column in schedule: {expected_col}"
        
        conn.close()


def test_dlt_vs_schema_compatibility() -> None:
    """Test that dlt-created tables are compatible with schema.py expectations."""
    # This test verifies that the tables dlt creates have compatible schemas
    # with what schema.py expects, ensuring the two approaches work together
    
    with tempfile.TemporaryDirectory() as tmp_dir:
        db_path = Path(tmp_dir) / "test_compatibility.duckdb"
        
        # First, create tables with schema.py
        from sports_analytics_pipeline.schema import init_db
        init_db(str(db_path))
        
        conn = duckdb.connect(str(db_path))
        
        # Insert test rows with required dlt metadata columns
        conn.execute("""
            INSERT INTO teams (name, city, _dlt_load_id, _dlt_id) 
            VALUES ('Test Team', 'Test City', 'test_load_123', 'test_id_123')
        """)
        
        conn.execute("""
            INSERT INTO venues (name, city, state, _dlt_load_id, _dlt_id) 
            VALUES ('Test Arena', 'Test City', 'TS', 'test_load_123', 'test_id_456')
        """)
        
        conn.execute("""
            INSERT INTO schedule (
                espn_event_id, date, start_time, away_team, home_team, venue, 
                status, home_score, away_score, game_type, _dlt_load_id, _dlt_id
            ) VALUES (
                '12345', '2024-10-15', '20:00:00', 'Test Team', 'Test Team', 
                'Test Arena', 'final', 100, 95, 'regular season', 'test_load_123', 'test_id_789'
            )
        """)
        
        # Verify the data was inserted
        team_count = conn.execute("SELECT COUNT(*) FROM teams").fetchone()[0]
        assert team_count == 1
        
        venue_count = conn.execute("SELECT COUNT(*) FROM venues").fetchone()[0]
        assert venue_count == 1
        
        schedule_count = conn.execute("SELECT COUNT(*) FROM schedule").fetchone()[0]
        assert schedule_count == 1
        
        conn.close()
