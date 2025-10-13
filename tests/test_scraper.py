from __future__ import annotations


from bball_season.scraper import _parse_scoreboard_json


def test_parse_scoreboard_minimal() -> None:
    """Ensure parser extracts date, time, teams and venue."""
    sample = {
        "events": [
            {
                "date": "2025-10-22T00:00:00Z",
                "competitions": [
                    {
                        "date": "2025-10-22T00:00:00Z",
                        "competitors": [
                            {
                                "homeAway": "home",
                                "team": {"displayName": "Lakers"},
                            },
                            {
                                "homeAway": "away",
                                "team": {"displayName": "Warriors"},
                            },
                        ],
                        "venue": {"fullName": "Staples Center"},
                    }
                ],
            }
        ]
    }

    out = _parse_scoreboard_json(sample)
    assert isinstance(out, list)
    assert len(out) == 1
    row = out[0]
    assert row["date"] == "2025-10-22"
    # start_time may be present as "00:00:00"
    assert row["start_time"] is not None
    assert row["home_team"] == "Lakers"
    assert row["away_team"] == "Warriors"
    assert row["venue"] == "Staples Center"
