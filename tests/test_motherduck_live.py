"""
Live integration tests for MotherDuck connectivity.
These tests verify that the MotherDuck integration works end-to-end.
"""
from __future__ import annotations

import os
import pytest
import dlt
from sports_analytics_pipeline.ingest import get_dlt_destination


def has_motherduck_token() -> bool:
    """Check if MotherDuck credentials are available."""
    if os.environ.get("motherduck_token"):
        return True
    secrets_path = os.path.join(os.getcwd(), ".dlt", "secrets.toml")
    if os.path.exists(secrets_path):
        try:
            with open(secrets_path, "r") as f:
                content = f.read()
            if "motherduck" in content:
                return True
        except Exception:
            pass
    return False


@pytest.mark.live
def test_motherduck_connection() -> None:
    """Test MotherDuck connection and basic query."""
    if not has_motherduck_token():
        pytest.skip("MotherDuck credentials not found")

    # Ensure dev environment for tests
    original_env = os.environ.get("SPORTS_ANALYTICS_ENV")
    try:
        os.environ["SPORTS_ANALYTICS_ENV"] = "dev"
        
        destination = get_dlt_destination("motherduck", "")
        pipeline = dlt.pipeline(
            pipeline_name="test_motherduck_pipeline", 
            destination=destination, 
            dataset_name="ingest"
        )
        
        # Test basic connectivity
        with pipeline.sql_client() as conn:
            result = conn.execute("SELECT 1").fetchone()
            assert result is not None
            
    finally:
        if original_env is not None:
            os.environ["SPORTS_ANALYTICS_ENV"] = original_env
        elif "SPORTS_ANALYTICS_ENV" in os.environ:
            del os.environ["SPORTS_ANALYTICS_ENV"]


@pytest.mark.live  
def test_motherduck_schema_query() -> None:
    """Test querying MotherDuck schema information."""
    if not has_motherduck_token():
        pytest.skip("MotherDuck credentials not found")

    original_env = os.environ.get("SPORTS_ANALYTICS_ENV")
    try:
        os.environ["SPORTS_ANALYTICS_ENV"] = "dev"
        
        destination = get_dlt_destination("motherduck", "")
        pipeline = dlt.pipeline(
            pipeline_name="test_schema_query",
            destination=destination,
            dataset_name="ingest"
        )
        
        # Verify we can query schema information
        with pipeline.sql_client() as client:
            result = client.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'ingest'"
            ).fetchone()
            assert result is not None
            assert isinstance(result[0], int)
            
    finally:
        if original_env is not None:
            os.environ["SPORTS_ANALYTICS_ENV"] = original_env
        elif "SPORTS_ANALYTICS_ENV" in os.environ:
            del os.environ["SPORTS_ANALYTICS_ENV"]