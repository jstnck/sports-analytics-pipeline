"""ESPN NBA season scraper.

This module fetches daily ESPN scoreboard JSON for a date range and
aggregates games into a pandas DataFrame where each row is one game.

Design decisions:
- Use ESPN's public site API scoreboard endpoint (JSON) which is
    easier to parse than scraping HTML pages.
- Iterate over dates in the season range and collect events.

Columns returned (per-row):
- date: ISO date (YYYY-MM-DD)
- start_time: start time string (as provided by ESPN, typically with timezone)
- home_team: full name of home team
- away_team: full name of away team
- venue: venue / location string when available

The network fetching functions use `requests`. Parsing is pure Python
and tested with a small sample JSON in the test suite.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any, Dict, Generator, List, Optional, cast

import pandas as pd
import requests


SCOREBOARD_URL = (
    "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard"
)


def _date_range(start: date, end: date) -> Generator[date, None, None]:
    """Yield dates from start to end inclusive.

    Args:
        start: start date
        end: end date
    """
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)


def _fetch_scoreboard_for_date(dt: date) -> Dict[str, Any]:
    """Fetch ESPN scoreboard JSON for a single date (YYYYMMDD).

    Raises requests.HTTPError on non-200 responses.
    """
    params = {"dates": dt.strftime("%Y%m%d")}
    resp = requests.get(SCOREBOARD_URL, params=params, timeout=15)
    resp.raise_for_status()
    return cast(Dict[str, Any], resp.json())


def _parse_scoreboard_json(payload: Dict[str, Any]) -> List[Dict[str, Optional[str]]]:
    """Parse a scoreboard JSON payload into a list of game dicts.

    Each returned dict has keys: date, start_time, home_team, away_team, venue.
    Missing values are returned as None.
    """
    results: List[Dict[str, Optional[str]]] = []
    events = payload.get("events") or []
    for ev in events:
        # Many fields are nested under 'competitions'
        competitions = ev.get("competitions") or []
        # We'll use the first competition if present
        comp = competitions[0] if competitions else {}

        # date/time
        ev_date = ev.get("date") or comp.get("date")
        if ev_date:
            # Keep ISO date/time; caller can normalize
            try:
                dt = datetime.fromisoformat(ev_date.replace("Z", "+00:00"))
                date_str = dt.date().isoformat()
                time_str = dt.time().isoformat()
            except Exception:
                date_str = ev_date
                time_str = None
        else:
            date_str = None
            time_str = None

        # competitors -> determine home/away
        home_team = None
        away_team = None
        competitors = comp.get("competitors") or []
        for team in competitors:
            team_obj = team.get("team") or {}
            display_name = team_obj.get("displayName") or team_obj.get("name")
            if team.get("homeAway") == "home":
                home_team = display_name
            elif team.get("homeAway") == "away":
                away_team = display_name

        # venue
        venue = None
        venue_obj = comp.get("venue") or ev.get("venue")
        if venue_obj:
            venue = venue_obj.get("fullName") or venue_obj.get("name")

        results.append(
            {
                "date": date_str,
                "start_time": time_str,
                "home_team": home_team,
                "away_team": away_team,
                "venue": venue,
            }
        )

    return results


def scrape_season(
    season_end_year: int,
    start: Optional[date] = None,
    end: Optional[date] = None,
) -> pd.DataFrame:
    """Scrape the NBA season and return a pandas DataFrame.

    The `season_end_year` should be the ending year for the season (e.g.
    2026 for the 2025-2026 season). By default the function will use an
    approximate season window (Oct 1 of previous year through June 30 of
    the season end year).

    Note: this function performs one HTTP request per day in the date
    range. You can provide a narrower `start`/`end` to reduce requests.
    """
    if start is None:
        start = date(season_end_year - 1, 10, 1)
    if end is None:
        end = date(season_end_year, 6, 30)

    rows: List[Dict[str, Optional[str]]] = []
    for dt in _date_range(start, end):
        payload = _fetch_scoreboard_for_date(dt)
        rows.extend(_parse_scoreboard_json(payload))

    df = pd.DataFrame(rows)
    # Ensure consistent column order
    cols = ["date", "start_time", "home_team", "away_team", "venue"]
    for c in cols:
        if c not in df.columns:
            df[c] = None
    return df[cols]


__all__ = ["scrape_season", "_parse_scoreboard_json", "_fetch_scoreboard_for_date"]
