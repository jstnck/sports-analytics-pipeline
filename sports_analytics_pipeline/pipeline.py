from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, Iterator

import logging
import pandas as pd

from sports_analytics_pipeline.ingest import (
    ingest_season,
    ingest_players_for_date,
    ingest_boxscores_for_date,
    _fetch_summary_for_event,
)
from sports_analytics_pipeline.storage import (
    init_db,
    ingest_schedule,
    ingest_box_scores,
    ingest_player_box_scores,
)
from sports_analytics_pipeline.transform import (
    game_box_from_summary,
)

logger = logging.getLogger(__name__)


def ingest_season_schedule(season_end_year: int, db_path: Path, *, start: Optional[date] = None, end: Optional[date] = None, cache_dir: Optional[str] = "data/raw") -> None:
    """Scrape a season schedule and ingest into DuckDB."""
    init_db(db_path)
    df = ingest_season(season_end_year, start=start, end=end, cache_dir=cache_dir)
    if df.empty:
        logger.info("No schedule rows found for season %s", season_end_year)
        return
    # storage.ingest_schedule expects an `espn_event_id` column; the ingest
    # helpers return `event_id` so normalize the column name here for
    # robustness.
    if "event_id" in df.columns and "espn_event_id" not in df.columns:
        df = df.rename(columns={"event_id": "espn_event_id"})

    # Ensure columns referenced by the ingest SQL exist (even if NULL).
    for col in ("home_score", "away_score", "game_type", "status"):
        if col not in df.columns:
            df[col] = None

    # For games that should have happened already (UTC), attempt to populate
    # missing scores by fetching the event summary. This keeps schedule rows
    # up-to-date at ingest time (past games should include outcomes).
    now_utc = datetime.now(timezone.utc)
    def _parse_timestamp(ts: Optional[str]) -> Optional[datetime]:
        if not ts:
            return None
        try:
            # Normalize trailing Z to +00:00 for fromisoformat
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            return None

    # iterate rows and fill missing scores when event time is in the past
    if not df.empty and "espn_event_id" in df.columns:
        for idx, row in df.iterrows():
            try:
                ev = row.get("espn_event_id")
                if ev is None:
                    continue

                # skip if scores already present
                if (row.get("home_score") is not None) or (row.get("away_score") is not None):
                    continue

                # determine event datetime: prefer timestamp_utc, fall back to date
                ts = row.get("timestamp_utc") or row.get("start_time") or row.get("date")
                ev_dt = _parse_timestamp(ts) if isinstance(ts, str) else None
                # if no full timestamp, but a date exists, treat end-of-day UTC as event time
                if ev_dt is None and row.get("date"):
                    try:
                        ev_date = datetime.fromisoformat(str(row.get("date"))).date()
                        # use midnight UTC of that date as a conservative estimate
                        ev_dt = datetime(ev_date.year, ev_date.month, ev_date.day, tzinfo=timezone.utc)
                    except Exception:
                        ev_dt = None

                # only try to populate scores for events that are in the past
                if ev_dt is None or ev_dt > now_utc:
                    continue

                # fetch summary and attempt to extract scores
                try:
                    summary = _fetch_summary_for_event(str(ev), cache_dir=cache_dir)
                except Exception:
                    # couldn't fetch summary; skip fill
                    continue

                comps = summary.get("competitions") or []
                if not comps:
                    continue
                comp = comps[0]
                home_score = None
                away_score = None
                for comp_team in comp.get("competitors") or []:
                    score = comp_team.get("score")
                    try:
                        score = int(score) if score is not None else None
                    except Exception:
                        score = None
                    if comp_team.get("homeAway") == "home":
                        home_score = score
                    elif comp_team.get("homeAway") == "away":
                        away_score = score

                # write back into DataFrame if we found scores
                if home_score is not None or away_score is not None:
                    # Use .loc with index label to avoid ambiguous tuple index typing
                    if idx in df.index:
                        mask = df.index == idx
                        if home_score is not None:
                            df.loc[mask, "home_score"] = home_score
                        if away_score is not None:
                            df.loc[mask, "away_score"] = away_score
            except Exception:
                # be defensive: don't let a single event failure abort the whole ingest
                logger.exception("Failed to populate scores for event %s", row.get("espn_event_id"))

    ingest_schedule(df, db_path=db_path)


def ingest_date(dt: date, db_path: Path, *, cache_dir: Optional[str] = "data/raw", delay: float = 0.1) -> None:
    """Scrape players and boxscores for a date and ingest into DuckDB.

    The pipeline prefers cached summary JSON when available to extract metadata
    (teams/date) but will fall back to the scoreboard-derived schedule.
    """
    init_db(db_path)

    # build lookup for event -> away/home/date using schedule for the date
    schedule_df = ingest_season(dt.year + 1, start=dt, end=dt, cache_dir=cache_dir)
    mapping: dict[str, dict[str, Optional[str]]] = {}
    if not schedule_df.empty:
        for _, row in schedule_df.iterrows():
            eid = str(row.get("event_id")) if row.get("event_id") is not None else None
            if eid:
                mapping[eid] = {
                    "away_team": row.get("away_team"),
                    "home_team": row.get("home_team"),
                    "date": row.get("date"),
                }

    # players (player box score rows)
    players_df = ingest_players_for_date(dt, cache_dir=cache_dir, delay=delay)
    # enrich players_df with teams/date when missing using the schedule mapping
    if not players_df.empty and "espn_event_id" in players_df.columns:
        def _meta(ev_id: Optional[str]) -> dict[str, Optional[str]]:
            if ev_id is None:
                return {"away_team": None, "home_team": None, "date": None}
            return mapping.get(str(ev_id), {"away_team": None, "home_team": None, "date": None})

        # fill missing columns from mapping
        for col in ("away_team", "home_team", "date"):
            if col not in players_df.columns:
                players_df[col] = players_df["espn_event_id"].apply(lambda e: _meta(e).get(col))
            else:
                players_df.loc[players_df[col].isna(), col] = players_df.loc[players_df[col].isna(), "espn_event_id"].apply(lambda e: _meta(e).get(col))

    if not players_df.empty:
        # remove rows without player names (these are not valid player rows
        # for the player_box_score table and would violate NOT NULL / PK)
        if "first_name" in players_df.columns and "last_name" in players_df.columns:
            players_df = players_df.dropna(subset=["first_name", "last_name"])
        ingest_player_box_scores(players_df, db_path=db_path)

    # team-level boxscores
    boxes = ingest_boxscores_for_date(dt, cache_dir=cache_dir, delay=delay)

    for ev_id, box in boxes.items():
        # try to extract teams/date from cached summary if present (preferred)
        away = None
        home = None
        event_date_str = None
        try:
            if cache_dir is not None:
                cache_path = Path(cache_dir) / f"summary_{ev_id}.json"
                if cache_path.exists():
                    import json as _json

                    s = _json.loads(cache_path.read_text(encoding="utf-8"))
                    comps = s.get("competitions") or []
                    if comps:
                        comp = comps[0]
                        for comp_team in comp.get("competitors") or []:
                            if comp_team.get("homeAway") == "home":
                                home = (comp_team.get("team") or {}).get("displayName")
                            elif comp_team.get("homeAway") == "away":
                                away = (comp_team.get("team") or {}).get("displayName")
                    header = s.get("header") or {}
                    date_str = header.get("competitions") and header["competitions"][0].get("date") if isinstance(header.get("competitions"), list) else None
                    event_date_str = date_str
        except Exception:
            # fallback to schedule mapping
            meta = mapping.get(ev_id, {})
            away = meta.get("away_team")
            home = meta.get("home_team")
            event_date_str = meta.get("date")

        # if we still don't have teams/date, try a live fetch of the summary
        # (useful when box dicts are minimal). This will make a network
        # request when cache_dir is None or cache missing.
        if (away is None or home is None or event_date_str is None):
            try:
                s = _fetch_summary_for_event(ev_id, cache_dir=cache_dir)
                comps = s.get("competitions") or []
                if comps:
                    comp = comps[0]
                    for comp_team in comp.get("competitors") or []:
                        if comp_team.get("homeAway") == "home":
                            home = (comp_team.get("team") or {}).get("displayName")
                        elif comp_team.get("homeAway") == "away":
                            away = (comp_team.get("team") or {}).get("displayName")
                header = s.get("header") or {}
                date_str = header.get("competitions") and header["competitions"][0].get("date") if isinstance(header.get("competitions"), list) else None
                if date_str:
                    event_date_str = date_str
            except Exception:
                # swallow; we'll try other fallbacks below
                pass

        # final fallback: if we still don't have an event date, use the
        # pipeline's requested date (dt) so ingestion has a non-null date
        if event_date_str is None:
            try:
                event_date_str = dt.isoformat()
            except Exception:
                event_date_str = None

        team_df = game_box_from_summary(box, ev_id, event_date=event_date_str, away_team=away, home_team=home)
        if not team_df.empty:
            # Ensure the DataFrame has game-level away_team/home_team filled for every row.
            meta = mapping.get(ev_id, {})
            game_away = away or meta.get("away_team")
            game_home = home or meta.get("home_team")

            if game_away and game_home:
                team_df["away_team"] = game_away
                team_df["home_team"] = game_home
                ingest_box_scores(team_df, db_path=db_path)
            else:
                logger.warning(
                    "Skipping ingest for event %s because away/home teams could not be determined",
                    ev_id,
                )



def backfill_box_scores_monthly(
    season_end_year: int,
    db_path: Path,
    *,
    start: Optional[date] = None,
    end: Optional[date] = None,
    cache_dir: Optional[str] = "data/raw",
    delay: float = 0.1,
    skip_existing: bool = True,
) -> None:
    """Backfill team-level box scores in monthly chunks for faster ingestion.

    This batches all team box score rows for each calendar month into a single
    DataFrame and inserts once per month to reduce DuckDB overhead, while
    reusing existing ingest/transform helpers. It is resume-safe and will
    optionally skip dates that already exist in `box_score`.

    Args:
        season_end_year: NBA season end year (e.g., 2025 for 2024-25).
        db_path: Path to DuckDB database file.
        start: Optional override for start date (default Oct 1 of season start).
        end: Optional override for end date (default Jun 30 of season end).
        cache_dir: Cache directory for JSON.
        delay: Base delay between network calls; cache-aware logic in ingest
            will skip sleeping when reading from cache.
        skip_existing: If True, skip dates already present in `box_score`.
    """
    # Determine date range for the season
    if start is None:
        start = date(season_end_year - 1, 10, 1)
    if end is None:
        end = date(season_end_year, 6, 30)

    init_db(db_path)

    # Helper to iterate months
    def _month_iter(s: date, e: date) -> Iterator[tuple[date, date]]:
        cur = date(s.year, s.month, 1)
        while cur <= e:
            # compute end of month
            next_month = (cur.replace(day=28) + timedelta(days=4)).replace(day=1)
            month_end = next_month - timedelta(days=1)
            yield cur if cur >= s else s, (month_end if month_end <= e else e)
            cur = next_month

    for m_start, m_end in _month_iter(start, end):
        # Query existing dates for this month to optionally skip work
        existing_dates: set[str] = set()
        if skip_existing:
            try:
                import duckdb

                con = duckdb.connect(str(db_path))
                rows = con.execute(
                    "SELECT DISTINCT date FROM box_score WHERE date BETWEEN ? AND ?",
                    [m_start, m_end],
                ).fetchall()
                con.close()
                for (d,) in rows:
                    if isinstance(d, str):
                        existing_dates.add(d)
                    elif isinstance(d, (datetime, date)):
                        existing_dates.add((d if isinstance(d, date) else d.date()).isoformat())
            except Exception:
                # If the check fails, just proceed without skipping
                existing_dates = set()

        month_frames: list[pd.DataFrame] = []
        cur = m_start
        while cur <= m_end:
            if skip_existing and cur.isoformat() in existing_dates:
                cur = cur + timedelta(days=1)
                continue

            # Build a schedule mapping for that date (away/home/date), using cache
            schedule_df = ingest_season(cur.year + 1, start=cur, end=cur, cache_dir=cache_dir)
            mapping: dict[str, dict[str, Optional[str]]] = {}
            if not schedule_df.empty:
                for _, row in schedule_df.iterrows():
                    eid = str(row.get("event_id")) if row.get("event_id") is not None else None
                    if eid:
                        mapping[eid] = {
                            "away_team": row.get("away_team"),
                            "home_team": row.get("home_team"),
                            "date": row.get("date"),
                        }

            boxes = ingest_boxscores_for_date(cur, cache_dir=cache_dir, delay=delay)
            for ev_id, box in boxes.items():
                # try to extract teams/date from cached summary if present
                away = None
                home = None
                event_date_str: Optional[str] = None
                try:
                    if cache_dir is not None:
                        cache_path = Path(cache_dir) / f"summary_{ev_id}.json"
                        if cache_path.exists():
                            import json as _json

                            s = _json.loads(cache_path.read_text(encoding="utf-8"))
                            comps = s.get("competitions") or []
                            if comps:
                                comp = comps[0]
                                for comp_team in comp.get("competitors") or []:
                                    if comp_team.get("homeAway") == "home":
                                        home = (comp_team.get("team") or {}).get("displayName")
                                    elif comp_team.get("homeAway") == "away":
                                        away = (comp_team.get("team") or {}).get("displayName")
                            header = s.get("header") or {}
                            date_str = header.get("competitions") and header["competitions"][0].get("date") if isinstance(header.get("competitions"), list) else None
                            event_date_str = date_str
                except Exception:
                    meta = mapping.get(ev_id, {})
                    away = meta.get("away_team")
                    home = meta.get("home_team")
                    event_date_str = meta.get("date")

                if (away is None or home is None or event_date_str is None):
                    try:
                        s = _fetch_summary_for_event(ev_id, cache_dir=cache_dir)
                        comps = s.get("competitions") or []
                        if comps:
                            comp = comps[0]
                            for comp_team in comp.get("competitors") or []:
                                if comp_team.get("homeAway") == "home":
                                    home = (comp_team.get("team") or {}).get("displayName")
                                elif comp_team.get("homeAway") == "away":
                                    away = (comp_team.get("team") or {}).get("displayName")
                        header = s.get("header") or {}
                        date_str = header.get("competitions") and header["competitions"][0].get("date") if isinstance(header.get("competitions"), list) else None
                        if date_str:
                            event_date_str = date_str
                    except Exception:
                        pass

                if event_date_str is None:
                    try:
                        event_date_str = cur.isoformat()
                    except Exception:
                        event_date_str = None

                team_df = game_box_from_summary(box, ev_id, event_date=event_date_str, away_team=away, home_team=home)
                if not team_df.empty:
                    meta = mapping.get(ev_id, {})
                    game_away = away or meta.get("away_team")
                    game_home = home or meta.get("home_team")
                    if game_away and game_home:
                        team_df["away_team"] = game_away
                        team_df["home_team"] = game_home
                        month_frames.append(team_df)
                    else:
                        logger.warning("Skipping event %s in %s due to missing teams", ev_id, cur)

            cur = cur + timedelta(days=1)

        if month_frames:
            all_month = pd.concat(month_frames, ignore_index=True, sort=False)
            ingest_box_scores(all_month, db_path=db_path)

