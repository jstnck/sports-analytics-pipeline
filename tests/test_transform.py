from __future__ import annotations


import pandas as pd

from sports_analytics_pipeline.transform import (
    schedule_from_scoreboard,
    players_from_summary,
    team_box_from_summary,
)


def test_schedule_from_scoreboard_minimal() -> None:
    payload = {
        "events": [
            {
                "id": "401716947",
                "date": "2024-10-04T00:00:00Z",
                "competitions": [{"id": "401716947", "competitors": [{"homeAway": "home", "team": {"displayName": "Lakers"}}, {"homeAway": "away", "team": {"displayName": "Warriors"}}], "venue": {"fullName": "Stadium"}}],
            }
        ]
    }

    df = schedule_from_scoreboard(payload)
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    row = df.iloc[0]
    assert row["event_id"] == "401716947"
    assert row["home_team"] == "Lakers"
    assert row["away_team"] == "Warriors"


def test_players_from_summary_minimal() -> None:
    summary = {
        "header": {"competitions": [{"date": "2024-10-04T00:00:00Z"}]},
        "competitions": [{"competitors": [{"homeAway": "home", "team": {"displayName": "Lakers"}}, {"homeAway": "away", "team": {"displayName": "Warriors"}}]}],
        "boxscore": {"players": [{"athlete": {"firstName": "LeBron", "lastName": "James"}, "stats": [{"name": "PTS", "value": 25}, {"name": "REB", "value": 8}], "team": "Lakers"}]},
    }

    df = players_from_summary(summary, espn_event_id="401716947")
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    row = df.iloc[0]
    assert row["first_name"] in ("LeBron",)
    assert row["last_name"] in ("James",)
    assert row["espn_event_id"] == "401716947"


def test_team_box_from_summary_minimal() -> None:
    box = {"teams": [{"team": "Lakers", "statistics": {"points": 102, "rebounds": 40}}, {"team": "Warriors", "statistics": {"points": 98}}]}

    df = team_box_from_summary(box, "401716947", event_date="2024-10-04", away_team="Warriors", home_team="Lakers")
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert set(df["team"]) == {"Lakers", "Warriors"}
    assert df[df["team"] == "Lakers"].iloc[0]["points"] == 102
