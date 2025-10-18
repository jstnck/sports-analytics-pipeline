"""ESPN NBA data ingestion using dlt (data load tool).

This module provides a complete dlt-based data ingestion pipeline for NBA analytics.
It handles scoreboard, summary, and player data ingestion with built-in transformations
to match the expected schema.

Designed with clean function interfaces for orchestration flexibility.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Generator, Iterator, List, Optional

import json
import logging

import dlt
from dlt.sources.rest_api import rest_api_source


# User-Agent for API requests
USER_AGENT = "sports-analytics-pipeline-dlt/0.1 (+https://github.com/jstnck)"

# ESPN API configuration
BASE_URL = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba"
SCOREBOARD_URL = f"{BASE_URL}/scoreboard"
SUMMARY_URL = f"{BASE_URL}/summary"

# NBA season configuration
NBA_SEASON_START_MONTH = 10  # October
NBA_SEASON_START_DAY = 1
NBA_SEASON_END_MONTH = 6  # June
NBA_SEASON_END_DAY = 30

# TODO: Handling covid bubble season dates

logger = logging.getLogger(__name__)


def _date_range(start: date, end: date) -> Generator[date, None, None]:
    """Yield dates from start to end (inclusive)."""
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)


# Data transformation functions for dlt resources
def _transform_scoreboard_event(event: Dict[str, Any]) -> Dict[str, Any]:
    """Transform a single ESPN scoreboard event to schedule table format."""
    competitions = event.get("competitions") or []
    comp = competitions[0] if competitions else {}

    # Extract and normalize event datetime
    ev_date = event.get("date") or comp.get("date")
    date_str = None
    time_str = None
    timestamp_utc = None

    if ev_date:
        try:
            dt = datetime.fromisoformat(ev_date.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            dt_utc = dt.astimezone(timezone.utc)
            date_str = dt_utc.date().isoformat()
            time_str = dt_utc.time().isoformat()
            timestamp_utc = dt_utc.isoformat()
        except Exception:
            date_str = ev_date
            time_str = None
            timestamp_utc = None

    # Extract teams and scores
    home_team = None
    away_team = None
    home_score = None
    away_score = None

    competitors = comp.get("competitors") or []
    for team in competitors:
        team_obj = team.get("team") or {}
        display_name = team_obj.get("displayName") or team_obj.get("name")

        # Extract score
        score = team.get("score")
        if score is not None:
            try:
                score = int(score)
            except Exception:
                score = None

        if team.get("homeAway") == "home":
            home_team = display_name
            home_score = score
        elif team.get("homeAway") == "away":
            away_team = display_name
            away_score = score

    # Extract venue
    venue = None
    venue_obj = comp.get("venue") or event.get("venue")
    if venue_obj:
        venue = venue_obj.get("fullName") or venue_obj.get("name")

    # Get event ID
    event_id = str(event.get("id") or comp.get("id") or "")

    # Determine game status and type
    status = comp.get("status", {}).get("type", {}).get("name") or "unknown"
    game_type = "regular season"  # Default, could be enhanced based on event metadata

    return {
        "espn_event_id": event_id,
        "date": date_str,
        "start_time": time_str,
        "timestamp_utc": timestamp_utc,
        "away_team": away_team,
        "home_team": home_team,
        "venue": venue,
        "status": status,
        "home_score": home_score,
        "away_score": away_score,
        "game_type": game_type,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }


def _extract_teams_from_schedule(
    schedule_records: List[Dict[str, Any]],
) -> Iterator[Dict[str, Any]]:
    """Extract unique teams from schedule records."""
    teams = set()
    for record in schedule_records:
        if record.get("home_team"):
            teams.add(record["home_team"])
        if record.get("away_team"):
            teams.add(record["away_team"])

    for team_name in teams:
        yield {"name": team_name}


def _extract_venues_from_schedule(
    schedule_records: List[Dict[str, Any]],
) -> Iterator[Dict[str, Any]]:
    """Extract unique venues from schedule records.
    TODO: Create a seed file with matching teams, cities, venues"""
    venues = set()
    for record in schedule_records:
        if record.get("venue"):
            venues.add(record["venue"])

    for venue_name in venues:
        yield {
            "name": venue_name,
            "city": None,  # Could be enhanced based on venue metadata
            "state": None,
        }


def _transform_player_stats(
    player_data: Dict[str, Any],
    event_id: str,
    game_date: str,
    away_team: str,
    home_team: str,
) -> Dict[str, Any]:
    """Transform player statistics to player_box_score table format."""
    # Extract athlete info
    athlete = player_data.get("athlete") or player_data.get("player") or {}
    first_name = athlete.get("firstName") or athlete.get("displayName") or ""
    last_name = athlete.get("lastName") or ""

    # Handle full name split if needed
    if not last_name and first_name and " " in first_name:
        parts = first_name.split(" ")
        first_name = parts[0]
        last_name = " ".join(parts[1:])

    # Get player's team
    team = player_data.get("team") or athlete.get("team")
    if isinstance(team, dict):
        team = team.get("displayName") or team.get("name")

    # Extract stats
    stats = player_data.get("stats") or player_data.get("statistics") or []

    minutes_played = None
    points = None
    rebounds = None
    assists = None
    fouls = None
    plus_minus = None

    if isinstance(stats, list):
        for stat in stats:
            if not isinstance(stat, dict):
                continue
            stat_name = (stat.get("name") or "").lower()
            stat_value = stat.get("value")

            if "min" in stat_name and minutes_played is None:
                minutes_played = str(stat_value) if stat_value is not None else None
            elif "pts" in stat_name or "points" in stat_name:
                try:
                    points = int(stat_value) if stat_value is not None else None
                except Exception:
                    points = None
            elif "reb" in stat_name or "rebounds" in stat_name:
                try:
                    rebounds = int(stat_value) if stat_value is not None else None
                except Exception:
                    rebounds = None
            elif "ast" in stat_name or "assists" in stat_name:
                try:
                    assists = int(stat_value) if stat_value is not None else None
                except Exception:
                    assists = None
            elif "fouls" in stat_name or "pf" in stat_name:
                try:
                    fouls = int(stat_value) if stat_value is not None else None
                except Exception:
                    fouls = None
            elif "plus" in stat_name or "+/-" in stat_name:
                try:
                    plus_minus = int(stat_value) if stat_value is not None else None
                except Exception:
                    plus_minus = None

    return {
        "espn_event_id": event_id,
        "date": game_date,
        "away_team": away_team,
        "home_team": home_team,
        "first_name": first_name,
        "last_name": last_name,
        "team": team,
        "minutes_played": minutes_played,
        "points": points,
        "rebounds": rebounds,
        "assists": assists,
        "stats_json": json.dumps(player_data),
        "fouls": fouls,
        "plus_minus": plus_minus,
    }


# dlt resources using REST API source with built-in caching


@dlt.resource(
    name="schedule",
    write_disposition="merge",
    primary_key=["date", "away_team", "home_team"],
)
def schedule_resource(start_date: date, end_date: date) -> Iterator[Dict[str, Any]]:
    """Transformed schedule data from ESPN scoreboard API using dlt's built-in caching."""
    from typing import cast
    from dlt.sources.rest_api import RESTAPIConfig

    for dt in _date_range(start_date, end_date):
        date_str = dt.strftime("%Y%m%d")

        # Use dlt's REST API source with built-in caching
        source_config = cast(
            RESTAPIConfig,
            {
                "client": {
                    "base_url": BASE_URL,
                    "headers": {"User-Agent": USER_AGENT},
                },
                "resources": [
                    {
                        "name": "scoreboard_data",
                        "endpoint": {
                            "path": "scoreboard",
                            "params": {"dates": date_str},
                            "paginator": None,
                            "data_selector": "$",
                        },
                    }
                ],
            },
        )

        api_source = rest_api_source(source_config)

        # Get data from dlt source (with automatic caching)
        for resource in api_source.resources.values():
            for response_data in resource:
                events = response_data.get("events", [])

                for event in events:
                    yield _transform_scoreboard_event(event)


@dlt.resource(name="teams", write_disposition="merge", primary_key="name")
def teams_resource(start_date: date, end_date: date) -> Iterator[Dict[str, Any]]:
    """dlt resource for teams data extracted from schedule.
    TODO: Could create a seed file here, but would need international teams for preseason.
    """

    # Collect all schedule records first to extract teams
    schedule_records = list(schedule_resource(start_date, end_date))

    # Extract and yield unique teams
    yield from _extract_teams_from_schedule(schedule_records)


@dlt.resource(name="venues", write_disposition="merge", primary_key="name")
def venues_resource(start_date: date, end_date: date) -> Iterator[Dict[str, Any]]:
    """dlt resource for venues data extracted from schedule."""

    # Collect all schedule records first to extract venues
    schedule_records = list(schedule_resource(start_date, end_date))

    # Extract and yield unique venues
    yield from _extract_venues_from_schedule(schedule_records)


@dlt.resource(
    name="player_box_score",
    write_disposition="merge",
    primary_key=["date", "away_team", "home_team", "first_name", "last_name"],
)
def player_box_score_resource(target_date: date) -> Iterator[Dict[str, Any]]:
    """dlt resource for player box score data."""
    from typing import cast
    from dlt.sources.rest_api import RESTAPIConfig

    # First get schedule for the date to find events
    schedule_records = list(schedule_resource(target_date, target_date))

    # Configure REST API source for summary data
    resources: List[Dict[str, Any]] = []

    # Create resources for each event on this date
    event_mapping: Dict[str, Dict[str, Any]] = {}
    for i, record in enumerate(schedule_records):
        event_id = record.get("espn_event_id")
        if event_id:
            resources.append(
                {
                    "name": f"summary_{i:03d}",
                    "endpoint": {
                        "path": "summary",
                        "params": {"event": event_id},
                        "paginator": None,
                        "data_selector": "$",
                    },
                }
            )
            event_mapping[f"summary_{i:03d}"] = record

    if not resources:
        return

    source_config = cast(
        RESTAPIConfig,
        {
            "client": {
                "base_url": BASE_URL,
                "headers": {"User-Agent": USER_AGENT},
            },
            "resources": resources,
        },
    )

    # Get the REST API source
    api_source = rest_api_source(source_config)

    # Process each summary and extract player stats
    for resource_name, resource in api_source.resources.items():
        if resource_name not in event_mapping:
            continue

        game_info = event_mapping[resource_name]
        event_id = game_info["espn_event_id"]
        game_date = game_info["date"]
        away_team = game_info["away_team"]
        home_team = game_info["home_team"]

        for summary_data in resource:
            # Extract players from boxscore
            boxscore = summary_data.get("boxscore") or {}
            players = boxscore.get("players") or []

            for player_data in players:
                if not isinstance(player_data, dict):
                    continue

                # Transform and yield player data
                player_record = _transform_player_stats(
                    player_data, event_id, game_date, away_team, home_team
                )

                # Only yield if we have valid player names
                if player_record.get("first_name") and player_record.get("last_name"):
                    yield player_record


@dlt.resource(
    name="box_score",
    write_disposition="merge",
    primary_key=["date", "away_team", "home_team"],
)
def box_score_resource(target_date: date) -> Iterator[Dict[str, Any]]:
    """dlt resource for game-level box score data (aggregated by game, not team)."""
    from typing import cast
    from dlt.sources.rest_api import RESTAPIConfig

    # First get schedule for the date to find events
    schedule_records = list(schedule_resource(target_date, target_date))

    # Configure REST API source for summary data
    resources: List[Dict[str, Any]] = []

    # Create resources for each event on this date
    event_mapping: Dict[str, Dict[str, Any]] = {}
    for i, record in enumerate(schedule_records):
        event_id = record.get("espn_event_id")
        if event_id:
            resources.append(
                {
                    "name": f"summary_{i:03d}",
                    "endpoint": {
                        "path": "summary",
                        "params": {"event": event_id},
                        "paginator": None,
                        "data_selector": "$",
                    },
                }
            )
            event_mapping[f"summary_{i:03d}"] = record

    if not resources:
        return

    source_config = cast(
        RESTAPIConfig,
        {
            "client": {
                "base_url": BASE_URL,
                "headers": {"User-Agent": USER_AGENT},
            },
            "resources": resources,
        },
    )

    # Get the REST API source
    api_source = rest_api_source(source_config)

    # Process each summary and create game-level box score records
    for resource_name, resource in api_source.resources.items():
        if resource_name not in event_mapping:
            continue

        game_info = event_mapping[resource_name]
        event_id = game_info["espn_event_id"]
        game_date = game_info["date"]
        away_team = game_info["away_team"]
        home_team = game_info["home_team"]

        for summary_data in resource:
            # Extract teams from boxscore and aggregate into game-level record
            boxscore = summary_data.get("boxscore") or {}
            teams = boxscore.get("teams") or boxscore.get("teamStats") or []

            # Initialize stats for home and away teams
            home_points = None
            away_points = None
            home_rebounds = None
            away_rebounds = None
            home_assists = None
            away_assists = None

            # Process team data to extract home/away stats
            for team_data in teams:
                if not isinstance(team_data, dict):
                    continue

                team_name = (
                    (team_data.get("team") or {}).get("displayName")
                    if isinstance(team_data.get("team"), dict)
                    else team_data.get("team")
                )
                stats = team_data.get("statistics") or team_data.get("stats") or {}

                if isinstance(stats, dict):
                    points = stats.get("points")
                    rebounds = stats.get("rebounds")
                    assists = stats.get("assists")

                    # Assign to home or away based on team name match
                    if team_name == home_team:
                        home_points = points
                        home_rebounds = rebounds
                        home_assists = assists
                    elif team_name == away_team:
                        away_points = points
                        away_rebounds = rebounds
                        away_assists = assists

            # If we couldn't match by team name but have 2 teams, assign deterministically
            if (home_points is None and away_points is None) and len(teams) >= 2:
                # First team = home, second team = away (fallback)
                if len(teams) >= 1:
                    stats = teams[0].get("statistics") or teams[0].get("stats") or {}
                    if isinstance(stats, dict):
                        home_points = stats.get("points")
                        home_rebounds = stats.get("rebounds")
                        home_assists = stats.get("assists")

                if len(teams) >= 2:
                    stats = teams[1].get("statistics") or teams[1].get("stats") or {}
                    if isinstance(stats, dict):
                        away_points = stats.get("points")
                        away_rebounds = stats.get("rebounds")
                        away_assists = stats.get("assists")

            # Create game-level box score record matching the schema
            yield {
                "espn_event_id": event_id,
                "date": game_date,
                "away_team": away_team,
                "home_team": home_team,
                "home_points": home_points,
                "away_points": away_points,
                "home_rebounds": home_rebounds,
                "away_rebounds": away_rebounds,
                "home_assists": home_assists,
                "away_assists": away_assists,
                "stats_json": json.dumps(boxscore),
            }


def ingest_season_schedule_dlt(
    season_end_year: int,
    db_path: str | Path,
    *,
    start: Optional[date] = None,
    end: Optional[date] = None,
) -> None:
    """Ingest NBA season schedule data using dlt.

    Creates schedule, teams, and venues tables with proper schema.

    Args:
        season_end_year: NBA season end year (e.g., 2025 for 2024-25 season)
        db_path: Path to DuckDB database file
        start: Optional start date (defaults to Oct 1 of season start year)
        end: Optional end date (defaults to Jun 30 of season end year)
    """
    if start is None:
        start = date(season_end_year - 1, 10, 1)
    if end is None:
        end = date(season_end_year, 6, 30)

    logger.info(f"Ingesting season schedule from {start} to {end}")

    # Create dlt pipeline with DuckDB destination
    pipeline = dlt.pipeline(
        pipeline_name="nba_schedule",
        destination=dlt.destinations.duckdb(str(db_path)),
        dataset_name="main",  # Use main schema to match existing tables
    )

    # Run the pipeline with schedule and related data
    info = pipeline.run(
        [
            schedule_resource(start, end),  # Schedule with built-in dlt caching
            teams_resource(start, end),  # Extracted teams
            venues_resource(start, end),  # Extracted venues
        ]
    )

    # Log results
    logger.info(f"Schedule pipeline completed. Loaded {len(info.loads_ids)} loads.")
    if hasattr(info, "has_failed_jobs") and info.has_failed_jobs:
        logger.error("Pipeline had failed jobs - check dlt logs for details")


def ingest_date_dlt(
    target_date: date,
    db_path: str | Path,
    *,
    delay: float = 0.1,
) -> None:
    """Ingest NBA data for a specific date using dlt.

    Creates player_box_score and box_score tables for the given date.

    Args:
        target_date: Date to fetch data for
        db_path: Path to DuckDB database file
        delay: Request delay in seconds (dlt handles rate limiting automatically)
    """
    logger.info(f"Ingesting data for date {target_date}")

    # Create dlt pipeline with DuckDB destination
    pipeline = dlt.pipeline(
        pipeline_name="nba_daily",
        destination=dlt.destinations.duckdb(str(db_path)),
        dataset_name="main",  # Use main schema to match existing tables
    )

    # Run the pipeline with box score data (dlt handles API caching automatically)
    resources_to_run = [
        player_box_score_resource(target_date),
        box_score_resource(target_date),
    ]

    info = pipeline.run(resources_to_run)

    # Log results
    logger.info(f"Daily pipeline completed. Loaded {len(info.loads_ids)} loads.")
    if hasattr(info, "has_failed_jobs") and info.has_failed_jobs:
        logger.error("Pipeline had failed jobs - check dlt logs for details")


def backfill_box_scores_dlt(
    season_end_year: int,
    db_path: str | Path,
    *,
    start: Optional[date] = None,
    end: Optional[date] = None,
    delay: float = 0.1,
    skip_existing: bool = True,
) -> None:
    """Backfill team-level box scores using dlt.

    Processes dates individually but with efficient dlt batching and error handling.

    Args:
        season_end_year: NBA season end year (e.g., 2025 for 2024-25)
        db_path: Path to DuckDB database file
        start: Optional start date (defaults to Oct 1 of season start year)
        end: Optional end date (defaults to Jun 30 of season end year)
        delay: Request delay in seconds
        skip_existing: If True, skip dates already present in box_score table
    """
    if start is None:
        start = date(season_end_year - 1, 10, 1)
    if end is None:
        end = date(season_end_year, 6, 30)

    logger.info(f"Backfilling box scores from {start} to {end}")

    # Check existing dates if skip_existing is True
    existing_dates: set[str] = set()
    if skip_existing:
        try:
            import duckdb

            conn = duckdb.connect(str(db_path))
            rows = conn.execute(
                "SELECT DISTINCT date FROM main.box_score WHERE date BETWEEN ? AND ?",
                [start.isoformat(), end.isoformat()],
            ).fetchall()
            conn.close()

            for (d,) in rows:
                if isinstance(d, str):
                    existing_dates.add(d)
                elif isinstance(d, (datetime, date)):
                    existing_dates.add(
                        (d if isinstance(d, date) else d.date()).isoformat()
                    )

        except Exception as e:
            logger.warning(f"Could not check existing dates: {e}")
            existing_dates = set()

    # Process each date
    processed_count = 0
    error_count = 0

    for current_date in _date_range(start, end):
        if skip_existing and current_date.isoformat() in existing_dates:
            logger.debug(f"Skipping {current_date} (already exists)")
            continue

        try:
            ingest_date_dlt(current_date, db_path, delay=delay)
            processed_count += 1
            logger.info(f"Processed {current_date} ({processed_count} completed)")

        except Exception as e:
            error_count += 1
            logger.error(f"Failed to process {current_date}: {e}")

    logger.info(
        f"Backfill completed: {processed_count} dates processed, {error_count} errors"
    )


__all__ = [
    # Core dlt pipeline functions
    "ingest_season_schedule_dlt",
    "ingest_date_dlt",
    "backfill_box_scores_dlt",
    # dlt resources (for advanced users)
    "schedule_resource",
    "teams_resource",
    "venues_resource",
    "player_box_score_resource",
    "box_score_resource",
]
