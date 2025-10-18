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

import logging
import time
from urllib.error import HTTPError, URLError

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

# API Rate limiting configuration
DEFAULT_API_DELAY = 0.5  # Default delay between API calls in seconds
MIN_API_DELAY = 0.1      # Minimum delay (for light usage)
MAX_API_DELAY = 5.0      # Maximum delay (for heavy/bulk operations)
BURST_LIMIT = 100         # Number of requests before enforcing longer delay
BURST_COOLDOWN = 1.0     # Longer delay after burst limit

# TODO: Handling covid bubble season dates

logger = logging.getLogger(__name__)


# Error handling and retry utilities
class IngestionError(Exception):
    """Custom exception for ingestion-related errors."""
    pass


class APIError(IngestionError):
    """Exception raised for API-related errors."""
    pass


class DataValidationError(IngestionError):
    """Exception raised for data validation errors."""
    pass


class RateLimiter:
    """Smart rate limiter for API calls with burst protection."""
    
    def __init__(self, base_delay: float = DEFAULT_API_DELAY, burst_limit: int = BURST_LIMIT):
        self.base_delay = base_delay
        self.burst_limit = burst_limit
        self.request_count = 0
        self.last_request_time = 0.0
        
    def wait_if_needed(self) -> None:
        """Wait if necessary to respect rate limits."""
        current_time = time.time()
        
        # Calculate time since last request
        time_since_last = current_time - self.last_request_time
        
        # Determine appropriate delay
        if self.request_count >= self.burst_limit:
            # Use burst cooldown after hitting burst limit
            required_delay = BURST_COOLDOWN
            self.request_count = 0  # Reset burst counter
            logger.debug(f"Burst limit reached, using {BURST_COOLDOWN}s cooldown")
        else:
            required_delay = self.base_delay
            
        # Wait if we haven't waited long enough
        if time_since_last < required_delay:
            wait_time = required_delay - time_since_last
            logger.debug(f"Rate limiting: waiting {wait_time:.2f}s")
            time.sleep(wait_time)
            
        self.request_count += 1
        self.last_request_time = time.time()


# Global rate limiter instance
_rate_limiter = RateLimiter()


def retry_on_failure(max_retries: int = 3, delay: float = 1.0, backoff: float = 2.0) -> Any:
    """Decorator to retry function calls with exponential backoff.
    
    Args:
        max_retries: Maximum number of retry attempts
        delay: Initial delay between retries in seconds
        backoff: Multiplier for delay after each retry
    """
    def decorator(func: Any) -> Any:
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            last_exception = None
            current_delay = delay
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except (HTTPError, URLError, ConnectionError, TimeoutError) as e:
                    last_exception = e
                    if attempt == max_retries:
                        logger.error(f"Function {func.__name__} failed after {max_retries} retries: {e}")
                        raise APIError(f"API request failed after {max_retries} retries: {e}") from e
                    
                    logger.warning(f"Attempt {attempt + 1} failed for {func.__name__}: {e}. Retrying in {current_delay:.1f}s...")
                    time.sleep(current_delay)
                    current_delay *= backoff
                except Exception as e:
                    # For non-retryable exceptions, fail immediately
                    logger.error(f"Non-retryable error in {func.__name__}: {e}")
                    raise
                    
            # This shouldn't be reached, but just in case
            if last_exception:
                raise last_exception
            else:
                raise APIError("Retry decorator failed unexpectedly")
        return wrapper
    return decorator


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
    primary_key=["espn_event_id", "player_id"],  # Simplified primary key
    max_table_nesting=3,  # Minimal schema rule: max nesting level is 3
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
            # Minimal extraction - just add metadata and let dlt/dbt handle player stats
            boxscore = summary_data.get("boxscore") or {}
            players = boxscore.get("players") or []

            for i, player_data in enumerate(players):
                if not isinstance(player_data, dict):
                    continue

                # Create minimal player record with raw data
                yield {
                    "espn_event_id": event_id,
                    "player_id": f"{event_id}_{i:03d}",  # Simple unique ID
                    "date": game_date,
                    "away_team": away_team,
                    "home_team": home_team,
                    "player_data": player_data,  # Raw player data for dlt to unpack
                }


@dlt.resource(
    name="box_score",
    write_disposition="merge",
    primary_key=["espn_event_id"],
    max_table_nesting=3,  # Minimal schema rule: max nesting level is 3
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
            # Minimal extraction - just add metadata and let dlt/dbt handle the rest
            yield {
                "espn_event_id": event_id,
                "date": game_date,
                "away_team": away_team,
                "home_team": home_team,
                "boxscore": summary_data.get("boxscore"),  # Raw boxscore data for dlt to unpack
                "header": summary_data.get("header"),      # Game header info
                "rosters": summary_data.get("rosters"),    # Player rosters
            }


@retry_on_failure(max_retries=3, delay=1.0, backoff=2.0)
def ingest_season_schedule(
    season_end_year: int,
    db_path: str | Path,
    tables: Optional[set[str]] = None,
    *,
    start: Optional[date] = None,
    end: Optional[date] = None,
) -> None:
    """Ingest NBA season schedule data using dlt.

    Creates schedule, teams, and venues tables with proper schema.

    Args:
        season_end_year: NBA season end year (e.g., 2025 for 2024-25 season)
        db_path: Path to DuckDB database file
        tables: Optional set of tables to ingest. Available: {'schedule', 'teams', 'venues'}
        start: Optional start date (defaults to Oct 1 of season start year)
        end: Optional end date (defaults to Jun 30 of season end year)
    """
    if start is None:
        start = date(season_end_year - 1, 10, 1)
    if end is None:
        end = date(season_end_year, 6, 30)

    # Default to all schedule-related tables if none specified
    if tables is None:
        tables = {'schedule', 'teams', 'venues'}
    
    # Filter to only schedule-related tables
    schedule_tables = {'schedule', 'teams', 'venues'}
    selected_tables = tables.intersection(schedule_tables)
    
    if not selected_tables:
        logger.warning(f"No valid schedule tables selected from {tables}. Available: {schedule_tables}")
        return

    logger.info(f"Ingesting season schedule from {start} to {end}")
    logger.info(f"Selected tables: {', '.join(sorted(selected_tables))}")

    # Create dlt pipeline with DuckDB destination
    pipeline = dlt.pipeline(
        pipeline_name="nba_schedule",
        destination=dlt.destinations.duckdb(str(db_path)),
        dataset_name="main",  # Use main schema to match existing tables
    )

    # Build resources list based on selected tables
    resources_to_run = []
    if 'schedule' in selected_tables:
        resources_to_run.append(schedule_resource(start, end))
    if 'teams' in selected_tables:
        resources_to_run.append(teams_resource(start, end))
    if 'venues' in selected_tables:
        resources_to_run.append(venues_resource(start, end))

    # Run the pipeline with selected resources
    info = pipeline.run(resources_to_run)

    # Log results
    logger.info(f"Schedule pipeline completed. Loaded {len(info.loads_ids)} loads.")
    if hasattr(info, "has_failed_jobs") and info.has_failed_jobs:
        logger.error("Pipeline had failed jobs - check dlt logs for details")


@retry_on_failure(max_retries=3, delay=1.0, backoff=2.0)
def ingest_date(
    target_date: date,
    db_path: str | Path,
    tables: Optional[set[str]] = None,
    *,
    delay: float = DEFAULT_API_DELAY,
) -> None:
    """Ingest NBA data for a specific date using dlt.

    Creates player_box_score and box_score tables for the given date.

    Args:
        target_date: Date to fetch data for
        db_path: Path to DuckDB database file
        tables: Optional set of tables to ingest. Available: {'box_score', 'player_box_score'}
        delay: Base delay between API calls in seconds
    """
    # Default to all daily tables if none specified
    if tables is None:
        tables = {'box_score', 'player_box_score'}
    
    # Filter to only daily tables
    daily_tables = {'box_score', 'player_box_score'}
    selected_tables = tables.intersection(daily_tables)
    
    if not selected_tables:
        logger.warning(f"No valid daily tables selected from {tables}. Available: {daily_tables}")
        return

    logger.info(f"Ingesting data for date {target_date}")
    logger.info(f"Selected tables: {', '.join(sorted(selected_tables))}")
    
    # Configure rate limiter for this operation
    _rate_limiter.base_delay = max(MIN_API_DELAY, min(delay, MAX_API_DELAY))
    logger.debug(f"Rate limiter configured with {_rate_limiter.base_delay}s base delay")

    # Create dlt pipeline with DuckDB destination
    pipeline = dlt.pipeline(
        pipeline_name="nba_daily",
        destination=dlt.destinations.duckdb(str(db_path)),
        dataset_name="main",  # Use main schema to match existing tables
    )

    # Build resources list based on selected tables
    resources_to_run = []
    if 'player_box_score' in selected_tables:
        resources_to_run.append(player_box_score_resource(target_date))
    if 'box_score' in selected_tables:
        resources_to_run.append(box_score_resource(target_date))

    info = pipeline.run(resources_to_run)

    # Log results
    logger.info(f"Daily pipeline completed. Loaded {len(info.loads_ids)} loads.")
    if hasattr(info, "has_failed_jobs") and info.has_failed_jobs:
        logger.error("Pipeline had failed jobs - check dlt logs for details")


def backfill_box_scores(
    season_end_year: int,
    db_path: str | Path,
    *,
    start: Optional[date] = None,
    end: Optional[date] = None,
    tables: Optional[set[str]] = None,
    delay: float = DEFAULT_API_DELAY,
    skip_existing: bool = True,
) -> None:
    """Backfill team-level box scores using dlt.

    Processes dates individually but with efficient dlt batching and error handling.

    Args:
        season_end_year: NBA season end year (e.g., 2025 for 2024-25)
        db_path: Path to DuckDB database file
        start: Optional start date (defaults to Oct 1 of season start year)
        end: Optional end date (defaults to Jun 30 of season end year)
        tables: Optional set of tables to ingest. Available: {'box_score', 'player_box_score'}
        delay: Request delay in seconds
        skip_existing: If True, skip dates already present in box_score table
    """
    if start is None:
        start = date(season_end_year - 1, 10, 1)
    if end is None:
        end = date(season_end_year, 6, 30)

    # Default to all daily tables if none specified
    if tables is None:
        tables = {'box_score', 'player_box_score'}
    
    # Filter to only daily tables
    daily_tables = {'box_score', 'player_box_score'}
    selected_tables = tables.intersection(daily_tables)
    
    if not selected_tables:
        logger.warning(f"No valid daily tables selected from {tables}. Available: {daily_tables}")
        return

    logger.info(f"Backfilling box scores from {start} to {end}")
    logger.info(f"Selected tables: {', '.join(sorted(selected_tables))}")

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
            ingest_date(current_date, db_path, selected_tables, delay=delay)
            processed_count += 1
            logger.info(f"Processed {current_date} ({processed_count} completed)")

        except APIError as e:
            error_count += 1
            logger.error(f"API error processing {current_date}: {e}")
            # For API errors, we might want to continue with other dates
            continue
            
        except DataValidationError as e:
            error_count += 1
            logger.error(f"Data validation error for {current_date}: {e}")
            # Continue processing other dates even if one has bad data
            continue
            
        except Exception as e:
            error_count += 1
            logger.error(f"Unexpected error processing {current_date}: {e}", exc_info=True)
            
            # For unexpected errors, we might want to fail fast or continue
            # Let's continue but log the full traceback for debugging
            continue

    logger.info(
        f"Backfill completed: {processed_count} dates processed, {error_count} errors"
    )


__all__ = [
    # Core pipeline functions
    "ingest_season_schedule",
    "ingest_date",
    "backfill_box_scores",
    # Exception classes
    "IngestionError",
    "APIError", 
    "DataValidationError",
    # Rate limiting
    "RateLimiter",
    # Constants
    "DEFAULT_API_DELAY",
    "MIN_API_DELAY", 
    "MAX_API_DELAY",
    # dlt resources (for advanced users)
    "schedule_resource",
    "teams_resource",
    "venues_resource",
    "player_box_score_resource",
    "box_score_resource",
]
