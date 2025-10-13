import os
import pytest

# Live integration tests hit the ESPN API and are skipped by default. To enable
# set RUN_LIVE_TESTS=1 in your environment before running pytest.
if os.environ.get("RUN_LIVE_TESTS") != "1":
    pytest.skip("Set RUN_LIVE_TESTS=1 to run live integration tests against ESPN API", allow_module_level=True)

from datetime import date
from pathlib import Path

import duckdb

from bball_season.pipeline import ingest_date


def test_live_pipeline_ingest(tmp_path: Path) -> None:
    """Find a recent date with available boxscores, run the pipeline to ingest,
    and verify `box_score` exists with rows in DuckDB.

    This test uses live network calls and is gated by RUN_LIVE_TESTS.
    """
    # Use the first game date from the 2024-25 season (fixed) to keep this
    # integration deterministic.
    dt = date(2024, 10, 4)

    db_path = tmp_path / "live.duckdb"
    ingest_date(dt, db_path, cache_dir=None)

    conn = duckdb.connect(database=str(db_path))
    tbls = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='main'").fetchall()
    assert any(r[0] == 'box_score' for r in tbls)
    count = conn.execute("SELECT COUNT(*) FROM box_score").fetchone()[0]
    assert count >= 1
    conn.close()
