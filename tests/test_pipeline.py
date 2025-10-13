from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb

from bball_season.pipeline import ingest_date


def write_json(cache_dir: Path, name: str, obj: object) -> None:
    cache_dir.mkdir(parents=True, exist_ok=True)
    (cache_dir / name).write_text(__import__("json").dumps(obj), encoding="utf-8")


def test_ingest_date_end_to_end(tmp_path: Path) -> None:
    cache = tmp_path / "cache"
    dt = date(2024, 10, 4)

    # scoreboard with one event
    scoreboard = {
        "events": [
            {
                "id": "401716947",
                "date": "2024-10-04T00:00:00Z",
                "competitions": [{"id": "401716947"}],
            }
        ]
    }
    write_json(cache, f"{dt.strftime('%Y%m%d')}.json", scoreboard)

    # summary contains boxscore with a simple team-level structure and a players list
    summary = {
        "header": {"competitions": [{"date": "2024-10-04T00:00:00Z"}]},
        "competitions": [
            {
                "competitors": [
                    {"homeAway": "home", "team": {"displayName": "Lakers"}},
                    {"homeAway": "away", "team": {"displayName": "Warriors"}},
                ]
            }
        ],
        "boxscore": {"teams": [{"team": "Lakers", "statistics": {"points": 102}}], "players": []},
    }
    write_json(cache, "summary_401716947.json", summary)

    db_path = tmp_path / "tmp.duckdb"
    ingest_date(dt, db_path, cache_dir=str(cache))

    conn = duckdb.connect(database=str(db_path))
    # assert box_score table exists and has at least one row
    tbls = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='main'").fetchall()
    assert any(r[0] == 'box_score' for r in tbls)
    count = conn.execute("SELECT COUNT(*) FROM box_score").fetchone()[0]
    assert count >= 1
    conn.close()
