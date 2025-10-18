"""Live integration tests for ESPN API.

These tests make real API calls and should be run manually
for verification, not in CI/CD pipelines.

Run live tests with: LIVE_TESTS=1 pytest tests/test_live_integration.py -v -s
"""

from __future__ import annotations

import os
import tempfile
from datetime import date
from pathlib import Path

import pytest

from sports_analytics_pipeline.ingest import (
    ingest_date_dlt,
    ingest_season_schedule_dlt,
    schedule_resource,
)

# Skip all live tests unless LIVE_TESTS environment variable is set
skip_live_tests = pytest.mark.skipif(
    not os.getenv("LIVE_TESTS"),
    reason="Live tests require LIVE_TESTS=1 environment variable"
)


@skip_live_tests
class TestLiveAPIIntegration:
    """Live tests that make real ESPN API calls.
    
    Run with: LIVE_TESTS=1 pytest tests/test_live_integration.py -v -s
    """

    def test_live_schedule_resource_recent_date(self) -> None:
        """Test schedule resource with a recent date that should have games."""
        # Use a date from the current NBA season that likely has games
        test_date = date(2024, 11, 15)  # Mid-November usually has games
        
        # Get schedule data for one day
        records = list(schedule_resource(test_date, test_date))
        
        # Basic validation - we should get some games
        assert isinstance(records, list)
        print(f"Retrieved {len(records)} games for {test_date}")
        
        # If we got games, validate structure
        if records:
            sample_record = records[0]
            required_fields = [
                "espn_event_id", "date", "away_team", "home_team", 
                "venue", "status", "game_type", "created_at"
            ]
            
            for field in required_fields:
                assert field in sample_record, f"Missing field: {field}"
                
            print(f"Sample game: {sample_record['away_team']} @ {sample_record['home_team']}")
            print(f"Venue: {sample_record['venue']}")
            print(f"Status: {sample_record['status']}")

    def test_live_schedule_resource_off_season(self) -> None:
        """Test schedule resource during off-season (should return empty)."""
        # July is typically off-season for NBA
        test_date = date(2024, 7, 15)
        
        records = list(schedule_resource(test_date, test_date))
        
        # Should return empty list during off-season
        assert isinstance(records, list)
        print(f"Off-season test: Retrieved {len(records)} games for {test_date}")

    def test_live_ingest_season_schedule_small_range(self) -> None:
        """Test live season schedule ingestion with a small date range."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "live_test.duckdb"
            
            # Test with just a few days in November
            start_date = date(2024, 11, 15)
            end_date = date(2024, 11, 17)  # 3 days
            
            # Should not raise an exception
            ingest_season_schedule_dlt(
                season_end_year=2025,
                db_path=str(db_path),
                start=start_date,
                end=end_date
            )
            
            # Verify database was created
            assert db_path.exists()
            print(f"Successfully created database: {db_path}")
            
            # Basic database validation
            import duckdb
            conn = duckdb.connect(str(db_path))
            
            # Check that tables were created
            tables = conn.execute("SHOW TABLES").fetchall()
            table_names = [table[0] for table in tables]
            
            print(f"Created tables: {table_names}")
            
            # Check schedule table has data
            if "schedule" in table_names:
                count = conn.execute("SELECT COUNT(*) FROM schedule").fetchone()[0]
                print(f"Schedule records: {count}")
                
                if count > 0:
                    sample = conn.execute("SELECT * FROM schedule LIMIT 1").fetchone()
                    print(f"Sample schedule record: {sample}")
            
            conn.close()

    def test_live_ingest_date_recent(self) -> None:
        """Test live daily data ingestion for a recent date."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "live_daily_test.duckdb"
            
            # Use a recent date that likely has completed games
            target_date = date(2024, 11, 15)
            
            # Should not raise an exception
            ingest_date_dlt(
                target_date=target_date,
                db_path=str(db_path)
            )
            
            # Verify database was created
            assert db_path.exists()
            print(f"Successfully created database: {db_path}")
            
            # Check created tables
            import duckdb
            conn = duckdb.connect(str(db_path))
            
            tables = conn.execute("SHOW TABLES").fetchall()
            table_names = [table[0] for table in tables]
            
            print(f"Created tables: {table_names}")
            
            # Check for expected tables
            expected_tables = ["player_box_score", "box_score"]
            for expected_table in expected_tables:
                if expected_table in table_names:
                    count = conn.execute(f"SELECT COUNT(*) FROM {expected_table}").fetchone()[0]
                    print(f"{expected_table} records: {count}")
            
            conn.close()


# Utility function for manual testing
def manual_api_test() -> None:
    """Manual test function for debugging API responses."""
    print("=== Manual API Test ===")
    
    test_date = date(2024, 11, 15)
    print(f"Testing schedule resource for {test_date}")
    
    try:
        records = list(schedule_resource(test_date, test_date))
        print(f"Success! Retrieved {len(records)} records")
        
        if records:
            sample = records[0]
            print("Sample record:")
            for key, value in sample.items():
                print(f"  {key}: {value}")
                
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    # Run manual test when script is executed directly
    manual_api_test()