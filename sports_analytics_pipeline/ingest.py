"""ESPN NBA season ingest (package copy).

This module provides helpers to ingest ESPN scoreboard/summary JSON into
pandas DataFrames. It's a renamed copy of the previous `scraper` module to
use "ingest" terminology throughout the project.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from time import sleep
from typing import Any, Dict, Generator, List, Optional, Union, cast

import json
import logging

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

SCOREBOARD_URL = (
    "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard"
)
SUMMARY_URL = (
    "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/summary"
)


logger = logging.getLogger(__name__)


def _date_range(start: date, end: date) -> Generator[date, None, None]:
    """Yield dates from start to end (inclusive)."""
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)


def _make_session(retries: int = 3, backoff_factor: float = 0.5) -> requests.Session:
    """Return a requests.Session configured with retries and backoff."""
    session = requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    # polite default User-Agent
    session.headers.update(
        {"User-Agent": "sports-analytics-pipeline-ingest/0.1 (+https://example)"}
    )
    return session


def _fetch_scoreboard_for_date(
    dt: date,
    session: Optional[requests.Session] = None,
    cache_dir: Optional[Union[str, Path]] = None,
) -> Dict[str, Any]:
    """Fetch scoreboard JSON for a date, optionally using a cache dir."""
    if cache_dir is not None:
        cache_path = Path(cache_dir) / f"{dt.strftime('%Y%m%d')}.json"
        if cache_path.exists():
            try:
                data = json.loads(cache_path.read_text(encoding="utf-8"))
                return cast(Dict[str, Any], data)
            except Exception:
                logger.warning("Failed to read cache %s, refetching", cache_path)

    if session is None:
        session = _make_session()

    params = {"dates": dt.strftime("%Y%m%d")}
    resp = session.get(SCOREBOARD_URL, params=params, timeout=15)
    # If the server returned a 4xx/5xx we raise to let caller handle it.
    resp.raise_for_status()
    payload = resp.json()
    payload = cast(Dict[str, Any], payload)

    if cache_dir is not None:
        try:
            Path(cache_dir).mkdir(parents=True, exist_ok=True)
            cache_path.write_text(json.dumps(payload), encoding="utf-8")
        except Exception:
            logger.warning("Failed to write cache for %s", dt)

    return payload


def _parse_scoreboard_json(payload: Dict[str, Any]) -> List[Dict[str, Optional[Union[str, int]]]]:
    """Parse scoreboard JSON into list of game dicts (date,start_time,teams,venue,event_id)."""
    results: List[Dict[str, Optional[Union[str, int]]]] = []
    events = payload.get("events") or []
    for ev in events:
        competitions = ev.get("competitions") or []
        comp = competitions[0] if competitions else {}

        # Normalize event datetime to UTC and expose both the UTC timestamp
        # and a derived date (ISO) in UTC to avoid timezone ambiguity.
        ev_date = ev.get("date") or comp.get("date")
        if ev_date:
            try:
                # fromisoformat doesn't accept trailing 'Z', so normalize to +00:00
                dt = datetime.fromisoformat(ev_date.replace("Z", "+00:00"))
                # ensure tz-aware in UTC
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                dt_utc = dt.astimezone(timezone.utc)
                date_str = dt_utc.date().isoformat()
                # keep start_time as time in UTC (without offset) for backward compat
                time_str = dt_utc.time().isoformat()
                timestamp_utc = dt_utc.isoformat()
            except Exception:
                date_str = ev_date
                time_str = None
                timestamp_utc = None
        else:
            date_str = None
            time_str = None
            timestamp_utc = None

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

        # Extract scores when present. ESPN scoreboard JSON often includes a
        # 'score' field on the competitor entries; capture them as integers
        # when available so schedule rows can include home/away scores.
        home_score = None
        away_score = None
        for team in competitors:
            score = team.get("score")
            if score is None:
                # sometimes score is nested under 'linescores' or as string; try safe conversion
                try:
                    score = int(team.get("score")) if team.get("score") is not None else None
                except Exception:
                    score = None
            if team.get("homeAway") == "home":
                home_score = int(score) if score is not None else None
            elif team.get("homeAway") == "away":
                away_score = int(score) if score is not None else None

        venue = None
        venue_obj = comp.get("venue") or ev.get("venue")
        if venue_obj:
            venue = venue_obj.get("fullName") or venue_obj.get("name")

        # Prefer top-level event id, fall back to competition id when present
        event_id = ev.get("id") or comp.get("id")
        if event_id is not None:
            event_id = str(event_id)

        results.append(
            {
                "date": date_str,
                "start_time": time_str,
                "timestamp_utc": timestamp_utc,
                "home_team": home_team,
                "away_team": away_team,
                "venue": venue,
                "event_id": event_id,
                "home_score": home_score,
                "away_score": away_score,
            }
        )

    return results


def _fetch_summary_for_event(
    event_id: str,
    session: Optional[requests.Session] = None,
    cache_dir: Optional[Union[str, Path]] = "data/raw",
) -> Dict[str, Any]:
    """Fetch the summary JSON for a single event (cached).

    The ESPN `summary` endpoint typically contains the detailed boxscore
    under the top-level `boxscore` key. We cache per-event JSON as
    `data/raw/summary_<event_id>.json`.
    """
    if cache_dir is not None:
        cache_path = Path(cache_dir) / f"summary_{event_id}.json"
        if cache_path.exists():
            try:
                data = json.loads(cache_path.read_text(encoding="utf-8"))
                return cast(Dict[str, Any], data)
            except Exception:
                logger.warning("Failed to read cache %s, refetching", cache_path)

    if session is None:
        session = _make_session()

    resp = session.get(SUMMARY_URL, params={"event": event_id}, timeout=15)
    # summary endpoint may return 200 with useful payload or an error status
    resp.raise_for_status()
    payload = resp.json()
    payload = cast(Dict[str, Any], payload)

    if cache_dir is not None:
        try:
            Path(cache_dir).mkdir(parents=True, exist_ok=True)
            cache_path.write_text(json.dumps(payload), encoding="utf-8")
        except Exception:
            logger.warning("Failed to write summary cache for %s", event_id)

    return payload


def fetch_boxscore_for_event(
    event_id: Union[int, str],
    session: Optional[requests.Session] = None,
    cache_dir: Optional[Union[str, Path]] = "data/raw",
) -> Dict[str, Any]:
    """Return the `boxscore` dict for an ESPN event id.

    This helper prefers the `summary` endpoint since the standalone
    `/boxscore` endpoint may not always be available. Returns the raw
    `boxscore` mapping when present, otherwise raises ValueError.
    """
    ev = str(event_id)
    summary = _fetch_summary_for_event(ev, session=session, cache_dir=cache_dir)
    box = summary.get("boxscore")
    if not isinstance(box, dict):
        raise ValueError(f"no boxscore available in summary for event {ev}")
    return cast(Dict[str, Any], box)


def ingest_boxscores_for_date(
    dt: date,
    *,
    session: Optional[requests.Session] = None,
    delay: float = 0.5,
    cache_dir: Optional[Union[str, Path]] = "data/raw",
) -> Dict[str, Dict[str, Any]]:
    """Fetch boxscore payloads for all events on a given date.

    Returns a mapping of event_id -> boxscore dict for events where a
    boxscore was available. Non-fatal failures (HTTP errors, missing
    boxscore) are logged and skipped.
    """
    if session is None:
        session = _make_session()

    payload = _fetch_scoreboard_for_date(dt, session=session, cache_dir=cache_dir)
    events = payload.get("events") or []
    out: Dict[str, Dict[str, Any]] = {}
    for ev in events:
        ev_id = ev.get("id") or (ev.get("competitions") or [{}])[0].get("id")
        if ev_id is None:
            continue
        ev_id = str(ev_id)
        try:
            box = fetch_boxscore_for_event(ev_id, session=session, cache_dir=cache_dir)
        except Exception as exc:
            logger.info("No boxscore for %s: %s", ev_id, exc)
            # continue to next event
            sleep(delay)
            continue

        out[ev_id] = box
        # be polite between requests
        sleep(delay)

    return out


def ingest_season(
    season_end_year: int,
    start: Optional[date] = None,
    end: Optional[date] = None,
    *,
    session: Optional[requests.Session] = None,
    delay: float = 0.5,
    cache_dir: Optional[Union[str, Path]] = "data/raw",
) -> pd.DataFrame:
    """Return a DataFrame of games for the given season (supports caching/delay)."""
    if start is None:
        start = date(season_end_year - 1, 10, 1)
    if end is None:
        end = date(season_end_year, 6, 30)

    if session is None:
        session = _make_session()

    rows: List[Dict[str, Optional[Union[str, int]]]] = []
    for dt in _date_range(start, end):
        try:
            payload = _fetch_scoreboard_for_date(
                dt, session=session, cache_dir=cache_dir
            )
        except requests.HTTPError as exc:
            # Log and skip this date on HTTP errors (could be rate-limited)
            logger.warning("Failed to fetch %s: %s", dt, exc)
            # Sleep here too to avoid tight loops if server is rejecting us
            sleep(delay)
            continue
        except Exception as exc:  # defensive catch-all
            logger.exception("Unexpected error fetching %s: %s", dt, exc)
            sleep(delay)
            continue

        rows.extend(_parse_scoreboard_json(payload))

        # Only sleep after a network request (cache reads are fast)
        # If cache_dir is None we always slept after the request.
        sleep(delay)

    df = pd.DataFrame(rows)
    cols = ["date", "start_time", "home_team", "away_team", "venue", "event_id", "home_score", "away_score"]
    for c in cols:
        if c not in df.columns:
            df[c] = None
    return df[cols]


__all__ = ["ingest_season"]


def ingest_schedule(
    season_end_year: int,
    start: Optional[date] = None,
    end: Optional[date] = None,
    **kwargs: Any,
) -> pd.DataFrame:
    """Alias for `ingest_season` to make intent explicit (returns schedule rows).

    Keeps the same signature as `ingest_season` and simply forwards to it.
    """
    return ingest_season(season_end_year, start=start, end=end, **kwargs)


def ingest_teams_from_schedule(df: pd.DataFrame) -> pd.DataFrame:
    """Return a DataFrame of unique teams extracted from a schedule DataFrame.

    Columns: name, (city - may be None).
    """
    if df.empty:
        return pd.DataFrame(columns=["name", "city"])
    names = pd.Index(pd.concat([df["home_team"], df["away_team"]]).dropna().unique())
    teams_df = pd.DataFrame({"name": names})
    teams_df["city"] = None
    return teams_df


def _parse_players_from_summary(summary: Dict[str, Any]) -> List[Dict[str, Optional[Union[str, int]]]]:
    """Extract player rows from a `summary` payload's boxscore section.

    Returns list of dicts with keys: first_name, last_name, team, minutes_played,
    points, rebounds, assists, fouls, plus_minus, stats_json (string).
    This function is defensive and returns an empty list if the expected
    structures are not present.
    """
    out: List[Dict[str, Optional[Union[str, int]]]] = []
    box = summary.get("boxscore") or {}

    # Common shape: box['players'] is a list of player-stat entries.
    players = box.get("players")
    if isinstance(players, list) and players:
        for p in players:
            # p may be a dict with 'athlete' or nested structures
            athlete = p.get("athlete") or p.get("player")
            team = p.get("team") or p.get("teamName") or (p.get("athlete") or {}).get("team")
            # athlete may have displayName/fullName or firstName/lastName
            first = None
            last = None
            if isinstance(athlete, dict):
                first = athlete.get("firstName") or athlete.get("displayName")
                last = athlete.get("lastName")
                # try full name split if needed
                if not last and first and " " in first:
                    parts = first.split(" ")
                    first = parts[0]
                    last = " ".join(parts[1:])

            # fallback: look for 'name' field
            if not first and isinstance(p, dict):
                name = p.get("name") or p.get("fullName")
                if isinstance(name, str) and " " in name:
                    parts = name.split(" ")
                    first = parts[0]
                    last = " ".join(parts[1:])

            minutes = None
            points = None
            rebounds = None
            assists = None
            fouls = None
            plus_minus = None

            # try common stats locations
            stats = p.get("stats") or p.get("statistics")
            if isinstance(stats, list):
                # stats may be list of dicts with 'name' and 'value'
                for s in stats:
                    k = s.get("name") if isinstance(s, dict) else None
                    v = s.get("value") if isinstance(s, dict) else None
                    if k and v is not None:
                        lk = k.lower()
                        if "min" in lk and minutes is None:
                            minutes = str(v)
                        elif "pts" in lk and points is None:
                            try:
                                points = int(v)
                            except Exception:
                                points = None
                        elif "reb" in lk and rebounds is None:
                            try:
                                rebounds = int(v)
                            except Exception:
                                rebounds = None
                        elif "ast" in lk and assists is None:
                            try:
                                assists = int(v)
                            except Exception:
                                assists = None
                        elif "fouls" in lk or "pf" in lk:
                            try:
                                fouls = int(v)
                            except Exception:
                                fouls = None
                        elif "plus" in lk or "+/-" in lk:
                            try:
                                plus_minus = int(v)
                            except Exception:
                                plus_minus = None

            # stats_json fallback
            stats_json = None
            try:
                stats_json = json.dumps(p.get("stats") or p.get("statistics") or p)
            except Exception:
                stats_json = None

            out.append(
                {
                    "first_name": first,
                    "last_name": last,
                    "team": team if isinstance(team, str) else None,
                    "minutes_played": minutes,
                    "points": points,
                    "rebounds": rebounds,
                    "assists": assists,
                    "fouls": fouls,
                    "plus_minus": plus_minus,
                    "stats_json": stats_json,
                    # allow caller to attach date/teams/event id
                    "date": None,
                    "espn_event_id": None,
                }
            )

    return out


def ingest_players_for_event(
    event_id: Union[int, str],
    session: Optional[requests.Session] = None,
    cache_dir: Optional[Union[str, Path]] = "data/raw",
) -> pd.DataFrame:
    """Return a DataFrame of player box rows for an event using the summary endpoint.

    The DataFrame columns follow the `player_box_score` doc schema.
    """
    ev = str(event_id)
    try:
        summary = _fetch_summary_for_event(ev, session=session, cache_dir=cache_dir)
    except Exception as exc:
        logger.info("Failed to fetch summary for %s: %s", ev, exc)
        return pd.DataFrame()

    players = _parse_players_from_summary(summary)
    if not players:
        return pd.DataFrame()
    df = pd.DataFrame(players)
    # ensure columns exist
    for c in ["first_name", "last_name", "team", "minutes_played", "points", "rebounds", "assists", "fouls", "plus_minus", "stats_json"]:
        if c not in df.columns:
            df[c] = None

    # attach event/date/teams if available and normalize to UTC timestamp
    header = summary.get("header") or {}
    # try to extract date and teams from summary header competitions
    date_str = header.get("competitions") and header["competitions"][0].get("date") if isinstance(header.get("competitions"), list) else None
    timestamp_utc = None
    if date_str:
        try:
            dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            dt_utc = dt.astimezone(timezone.utc)
            df["date"] = dt_utc.date().isoformat()
            timestamp_utc = dt_utc.isoformat()
        except Exception:
            df["date"] = date_str
            timestamp_utc = None
    else:
        df["date"] = None

    # try teams from summary competitions
    comps = summary.get("competitions") or []
    if comps:
        comp = comps[0]
        # find away/home
        away = None
        home = None
        for comp_team in comp.get("competitors") or []:
            if comp_team.get("homeAway") == "home":
                home = (comp_team.get("team") or {}).get("displayName")
            elif comp_team.get("homeAway") == "away":
                away = (comp_team.get("team") or {}).get("displayName")
        df["away_team"] = away
        df["home_team"] = home
    else:
        df["away_team"] = None
        df["home_team"] = None

    df["espn_event_id"] = ev
    df["timestamp_utc"] = timestamp_utc
    return df


def ingest_players_for_date(
    dt: date,
    *,
    session: Optional[requests.Session] = None,
    delay: float = 0.5,
    cache_dir: Optional[Union[str, Path]] = "data/raw",
) -> pd.DataFrame:
    """Fetch players for all events on a date and return a combined DataFrame."""
    if session is None:
        session = _make_session()
    payload = _fetch_scoreboard_for_date(dt, session=session, cache_dir=cache_dir)
    events = payload.get("events") or []
    rows: List[pd.DataFrame] = []
    for ev in events:
        ev_id = ev.get("id") or (ev.get("competitions") or [{}])[0].get("id")
        if not ev_id:
            continue
        df = ingest_players_for_event(ev_id, session=session, cache_dir=cache_dir)
        if not df.empty:
            rows.append(df)
        sleep(delay)
    if not rows:
        return pd.DataFrame()
    return pd.concat(rows, ignore_index=True, sort=False)


def ingest_players_for_season(
    season_end_year: int,
    start: Optional[date] = None,
    end: Optional[date] = None,
    *,
    session: Optional[requests.Session] = None,
    delay: float = 0.5,
    cache_dir: Optional[Union[str, Path]] = "data/raw",
) -> pd.DataFrame:
    """Fetch players for all events in a season date range."""
    if start is None:
        start = date(season_end_year - 1, 10, 1)
    if end is None:
        end = date(season_end_year, 6, 30)
    if session is None:
        session = _make_session()
    rows: List[pd.DataFrame] = []
    cur = start
    while cur <= end:
        try:
            df = ingest_players_for_date(cur, session=session, delay=delay, cache_dir=cache_dir)
            if not df.empty:
                rows.append(df)
        except Exception:
            logger.exception("Failed to ingest players for %s", cur)
        cur = cur + timedelta(days=1)
    if not rows:
        return pd.DataFrame()
    return pd.concat(rows, ignore_index=True, sort=False)


def ingest_boxscores_for_season(
    season_end_year: int,
    start: Optional[date] = None,
    end: Optional[date] = None,
    *,
    session: Optional[requests.Session] = None,
    delay: float = 0.5,
    cache_dir: Optional[Union[str, Path]] = "data/raw",
) -> Dict[str, Dict[str, Any]]:
    """Fetch boxscores for every event in a season and return mapping event_id -> box dict."""
    if start is None:
        start = date(season_end_year - 1, 10, 1)
    if end is None:
        end = date(season_end_year, 6, 30)
    if session is None:
        session = _make_session()
    out: Dict[str, Dict[str, Any]] = {}
    cur = start
    while cur <= end:
        try:
            daily = ingest_boxscores_for_date(cur, session=session, delay=delay, cache_dir=cache_dir)
            out.update(daily)
        except Exception:
            logger.exception("Failed to ingest boxscores for %s", cur)
        cur = cur + timedelta(days=1)
    return out
