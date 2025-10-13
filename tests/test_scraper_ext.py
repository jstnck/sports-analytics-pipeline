from __future__ import annotations

from datetime import date
from pathlib import Path
import json

import pandas as pd

from sports_analytics_pipeline.scraper import (
    _date_range,
    _fetch_scoreboard_for_date,
    _fetch_summary_for_event,
    fetch_boxscore_for_event,
    scrape_boxscores_for_date,
    scrape_season,
    scrape_teams_from_schedule,
    _parse_players_from_summary,
    scrape_players_for_event,
    scrape_players_for_date,
)


def write_json(cache_dir: Path, filename: str, obj: object) -> None:
    cache_dir.mkdir(parents=True, exist_ok=True)
    path = cache_dir / filename
    path.write_text(json.dumps(obj), encoding="utf-8")


def test_date_range_simple() -> None:
    start = date(2024, 10, 1)
    end = date(2024, 10, 3)
    days = list(_date_range(start, end))
    assert days == [date(2024, 10, 1), date(2024, 10, 2), date(2024, 10, 3)]


def test_fetch_scoreboard_from_cache(tmp_path: Path) -> None:
    cache = tmp_path / "cache"
    dt = date(2024, 10, 4)
    payload = {
        "events": [
            {
                "id": "401716947",
                "date": "2024-10-04T00:00:00Z",
                "competitions": [
                    {
                        "id": "401716947",
                        "date": "2024-10-04T00:00:00Z",
                        "competitors": [
                            {"homeAway": "home", "team": {"displayName": "Lakers"}},
                            {"homeAway": "away", "team": {"displayName": "Warriors"}},
                        ],
                    }
                ],
            }
        ]
    }
    write_json(cache, f"{dt.strftime('%Y%m%d')}.json", payload)

    got = _fetch_scoreboard_for_date(dt, session=None, cache_dir=str(cache))
    assert isinstance(got, dict)
    assert got.get("events") == payload["events"]


def test_fetch_summary_and_boxscore_from_cache(tmp_path: Path) -> None:
    cache = tmp_path / "cache"
    event_id = "401716947"
    summary = {
        "boxscore": {"some": "payload"},
        "header": {"competitions": [{"date": "2024-10-04T00:00:00Z"}]},
        "competitions": [],
    }
    write_json(cache, f"summary_{event_id}.json", summary)

    got = _fetch_summary_for_event(event_id, session=None, cache_dir=str(cache))
    assert got["boxscore"]["some"] == "payload"

    box = fetch_boxscore_for_event(event_id, session=None, cache_dir=str(cache))
    assert box == summary["boxscore"]


def test_fetch_boxscore_raises_when_missing(tmp_path: Path) -> None:
    cache = tmp_path / "cache"
    event_id = "999999999"
    write_json(cache, f"summary_{event_id}.json", {"not_boxscore": True})

    import pytest

    with pytest.raises(ValueError):
        fetch_boxscore_for_event(event_id, session=None, cache_dir=str(cache))


def test_scrape_boxscores_for_date_with_cache(tmp_path: Path) -> None:
    cache = tmp_path / "cache"
    dt = date(2024, 10, 4)
    scoreboard = {
        "events": [
            {
                "id": "401716947",
                "date": "2024-10-04T00:00:00Z",
                "competitions": [
                    {"id": "401716947", "competitors": []}
                ],
            }
        ]
    }
    write_json(cache, f"{dt.strftime('%Y%m%d')}.json", scoreboard)
    summary = {"boxscore": {"team": "data"}}
    write_json(cache, "summary_401716947.json", summary)

    out = scrape_boxscores_for_date(dt, session=None, delay=0, cache_dir=str(cache))
    assert "401716947" in out
    assert out["401716947"] == summary["boxscore"]


def test_scrape_season_single_date(tmp_path: Path) -> None:
    cache = tmp_path / "cache"
    dt = date(2024, 10, 4)
    scoreboard = {
        "events": [
            {
                "id": "401716947",
                "date": "2024-10-04T00:00:00Z",
                "competitions": [
                    {
                        "id": "401716947",
                        "date": "2024-10-04T00:00:00Z",
                        "competitors": [
                            {"homeAway": "home", "team": {"displayName": "Lakers"}},
                            {"homeAway": "away", "team": {"displayName": "Warriors"}},
                        ],
                    }
                ],
            }
        ]
    }
    write_json(cache, f"{dt.strftime('%Y%m%d')}.json", scoreboard)

    df = scrape_season(2025, start=dt, end=dt, session=None, delay=0, cache_dir=str(cache))
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert df.iloc[0]["home_team"] == "Lakers"


def test_scrape_teams_from_schedule() -> None:
    df = pd.DataFrame(
        [
            {"home_team": "Lakers", "away_team": "Warriors"},
            {"home_team": "Celtics", "away_team": "Lakers"},
        ]
    )
    teams = scrape_teams_from_schedule(df)
    assert set(teams["name"]) == {"Lakers", "Warriors", "Celtics"}


def test_parse_players_from_summary_basic() -> None:
    summary = {
        "boxscore": {
            "players": [
                {
                    "athlete": {"firstName": "LeBron", "lastName": "James"},
                    "team": "Lakers",
                    "stats": [
                        {"name": "MIN", "value": "35:12"},
                        {"name": "PTS", "value": "30"},
                        {"name": "REB", "value": "8"},
                        {"name": "AST", "value": "10"},
                    ],
                }
            ]
        }
    }
    out = _parse_players_from_summary(summary)
    assert isinstance(out, list)
    assert out[0]["first_name"] == "LeBron"
    assert out[0]["points"] == 30


def test_scrape_players_for_event_with_cache(tmp_path: Path) -> None:
    cache = tmp_path / "cache"
    event_id = "401716947"
    summary = {
        "boxscore": {
            "players": [
                {
                    "athlete": {"firstName": "LeBron", "lastName": "James"},
                    "team": "Lakers",
                    "stats": [{"name": "PTS", "value": "30"}],
                }
            ]
        },
        "header": {"competitions": [{"date": "2024-10-04T00:00:00Z"}]},
        "competitions": [
            {
                "competitors": [
                    {"homeAway": "home", "team": {"displayName": "Lakers"}},
                    {"homeAway": "away", "team": {"displayName": "Warriors"}},
                ]
            }
        ],
    }
    write_json(cache, f"summary_{event_id}.json", summary)

    df = scrape_players_for_event(event_id, session=None, cache_dir=str(cache))
    assert not df.empty
    assert df.iloc[0]["first_name"] == "LeBron"
    assert df.iloc[0]["espn_event_id"] == event_id


def test_scrape_players_for_date_uses_scoreboard(tmp_path: Path) -> None:
    cache = tmp_path / "cache"
    dt = date(2024, 10, 4)
    scoreboard = {
        "events": [
            {"id": "401716947", "competitions": [{"id": "401716947"}]}
        ]
    }
    write_json(cache, f"{dt.strftime('%Y%m%d')}.json", scoreboard)
    summary = {
        "boxscore": {"players": []},
        "header": {"competitions": [{"date": "2024-10-04T00:00:00Z"}]},
        "competitions": [],
    }
    write_json(cache, "summary_401716947.json", summary)

    df = scrape_players_for_date(dt, session=None, delay=0, cache_dir=str(cache))
    # players empty in summary -> empty DF
    assert df.empty
