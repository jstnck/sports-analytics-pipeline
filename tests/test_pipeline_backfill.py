from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb

from sports_analytics_pipeline.pipeline import (
    ingest_season_schedule,
    backfill_box_scores_monthly,
)


def write_json(cache_dir: Path, name: str, obj: object) -> None:
    cache_dir.mkdir(parents=True, exist_ok=True)
    (cache_dir / name).write_text(__import__("json").dumps(obj), encoding="utf-8")


def test_ingest_season_schedule_from_cached_scoreboard(tmp_path: Path) -> None:
    cache = tmp_path / "cache"
    # Two days in October with one event each
    d1 = date(2024, 10, 3)
    d2 = date(2024, 10, 4)

    scoreboard1 = {
        "events": [
            {
                "id": "401716900",
                "date": "2024-10-03T00:00:00Z",
                "competitions": [
                    {
                        "id": "401716900",
                        "competitors": [
                            {"homeAway": "home", "team": {"displayName": "Lakers"}, "score": "101"},
                            {"homeAway": "away", "team": {"displayName": "Warriors"}, "score": "99"},
                        ],
                    }
                ],
            }
        ]
    }
    scoreboard2 = {
        "events": [
            {
                "id": "401716901",
                "date": "2024-10-04T00:00:00Z",
                "competitions": [
                    {
                        "id": "401716901",
                        "competitors": [
                            {"homeAway": "home", "team": {"displayName": "Celtics"}, "score": "88"},
                            {"homeAway": "away", "team": {"displayName": "Nets"}, "score": "85"},
                        ],
                    }
                ],
            }
        ]
    }
    write_json(cache, f"{d1.strftime('%Y%m%d')}.json", scoreboard1)
    write_json(cache, f"{d2.strftime('%Y%m%d')}.json", scoreboard2)

    db_path = tmp_path / "tmp.duckdb"
    # Season end 2025 covers Oct 2024
    ingest_season_schedule(2025, db_path, start=d1, end=d2, cache_dir=str(cache))

    con = duckdb.connect(str(db_path))
    cnt = con.execute("SELECT COUNT(*) FROM schedule").fetchone()[0]
    assert cnt == 2
    rows = con.execute("SELECT espn_event_id, home_score, away_score FROM schedule ORDER BY date").fetchall()
    # Scores should be present from scoreboard parsing
    assert rows[0][0] == "401716900" and rows[0][1] == 101 and rows[0][2] == 99
    assert rows[1][0] == "401716901" and rows[1][1] == 88 and rows[1][2] == 85
    con.close()


def test_backfill_box_scores_monthly_batches_and_idempotent(tmp_path: Path) -> None:
    cache = tmp_path / "cache"
    # Two days in October with one event each; summaries include simple team stats
    d1 = date(2024, 10, 5)
    d2 = date(2024, 10, 6)

    scoreboard1 = {"events": [{"id": "401716910", "date": "2024-10-05T00:00:00Z", "competitions": [{"id": "401716910"}]}]}
    scoreboard2 = {"events": [{"id": "401716911", "date": "2024-10-06T00:00:00Z", "competitions": [{"id": "401716911"}]}]}
    write_json(cache, f"{d1.strftime('%Y%m%d')}.json", scoreboard1)
    write_json(cache, f"{d2.strftime('%Y%m%d')}.json", scoreboard2)

    # Summaries include competitors and a team-level box
    summary1 = {
        "header": {"competitions": [{"date": "2024-10-05T00:00:00Z"}]},
        "competitions": [
            {
                "competitors": [
                    {"homeAway": "home", "team": {"displayName": "Heat"}},
                    {"homeAway": "away", "team": {"displayName": "Magic"}},
                ]
            }
        ],
        "boxscore": {
            "teams": [
                {"team": "Heat", "statistics": {"points": 110, "rebounds": 40, "assists": 22}},
                {"team": "Magic", "statistics": {"points": 102, "rebounds": 38, "assists": 20}},
            ]
        },
    }
    summary2 = {
        "header": {"competitions": [{"date": "2024-10-06T00:00:00Z"}]},
        "competitions": [
            {
                "competitors": [
                    {"homeAway": "home", "team": {"displayName": "Bulls"}},
                    {"homeAway": "away", "team": {"displayName": "Knicks"}},
                ]
            }
        ],
        "boxscore": {
            "teams": [
                {"team": "Bulls", "statistics": {"points": 95, "rebounds": 41, "assists": 18}},
                {"team": "Knicks", "statistics": {"points": 97, "rebounds": 39, "assists": 21}},
            ]
        },
    }
    write_json(cache, "summary_401716910.json", summary1)
    write_json(cache, "summary_401716911.json", summary2)

    db_path = tmp_path / "tmp.duckdb"
    # Limit backfill to Oct 2024 to keep test fast
    backfill_box_scores_monthly(
        2025,
        db_path,
        start=d1,
        end=d2,
        cache_dir=str(cache),
        delay=0.0,
        skip_existing=True,
    )

    con = duckdb.connect(str(db_path))
    cnt = con.execute("SELECT COUNT(*) FROM box_score").fetchone()[0]
    # Two games, one row per game under new schema
    assert cnt == 2
    # Re-run to ensure idempotency when skip_existing=True
    con.close()

    backfill_box_scores_monthly(
        2025,
        db_path,
        start=d1,
        end=d2,
        cache_dir=str(cache),
        delay=0.0,
        skip_existing=True,
    )

    con = duckdb.connect(str(db_path))
    cnt2 = con.execute("SELECT COUNT(*) FROM box_score").fetchone()[0]
    assert cnt2 == 2
    con.close()
