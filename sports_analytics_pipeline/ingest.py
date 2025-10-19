"""ESPN NBA data ingestion using dlt (data load tool).

This module provides a complete dlt-based data ingestion pipeline for ESPN's NBA AP.
It handles scoreboard, summary, and player data ingestion with built-in transformations
to match the expected schema.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Generator, Iterator, Optional, cast

import logging
import time
from urllib.error import HTTPError, URLError

import dlt
from dlt.sources.rest_api import rest_api_source, RESTAPIConfig
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, before_sleep_log

from .config import ENVIRONMENT, get_motherduck_database


# User-Agent for API requests
USER_AGENT = "sports-analytics-pipeline-dlt/0.1 (+https://github.com/jstnck)"

# ESPN API configuration
BASE_URL = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba"

# API Rate limiting configuration - faster, less polite settings
DEFAULT_API_DELAY = 0.2  # Default delay between API calls in seconds (was 0.5)
MIN_API_DELAY = 0.05     # Minimum delay (was 0.1) 
MAX_API_DELAY = 2.0      # Maximum delay (was 5.0)
BURST_LIMIT = 200        # Number of requests before enforcing longer delay (was 100)
BURST_COOLDOWN = 0.5     # Longer delay after burst limit (was 1.0)

logger = logging.getLogger(__name__)


# Error handling
class IngestionError(Exception):
    """Custom exception for ingestion-related errors."""
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


class ESPNAPIResource:
    """Base class for ESPN API resources with DLT integration.
    Provides common REST API configuration, metadata handling, and response processing.
    """
    
    def __init__(self, name: str, write_disposition: str = "append"):
        self.name = name
        self.write_disposition = write_disposition
        self.rate_limiter = _rate_limiter
    
    def _create_api_source(self, resources_config: list[dict]) -> Any:
        """Create REST API source with standard ESPN configuration.
        
        Args:
            resources_config: List of resource endpoint configurations
            
        Returns:
            Configured REST API source
        """
        source_config = cast(
            RESTAPIConfig,
            {
                "client": {
                    "base_url": BASE_URL,
                    "headers": {"User-Agent": USER_AGENT},
                },
                "resources": resources_config,
            },
        )
        return rest_api_source(source_config)
    
    def _add_metadata(self, data: dict, **extra_metadata: Any) -> dict:
        """Add standard metadata to response data.
        
        Args:
            data: Original response data
            **extra_metadata: Additional metadata to include
            
        Returns:
            Data with metadata added
        """
        return {
            "ingested_at": datetime.now(timezone.utc).isoformat(),
            **extra_metadata,
            **data  # Original data comes last to avoid overwriting metadata
        }
    
    def _process_responses(self, api_source: Any, **metadata: Any) -> Iterator[dict]:
        """Standard response processing with metadata injection.
        
        Args:
            api_source: Configured REST API source
            **metadata: Additional metadata to add to each response
            
        Yields:
            Processed response data with metadata
        """
        for resource in api_source.resources.values():
            for response_data in resource:
                yield self._add_metadata(response_data, **metadata)


def get_dlt_destination(storage: str, db_path: Path | str) -> Any:
    """Create the appropriate dlt destination based on storage backend.
    
    Args:
        storage: Storage backend ('local' or 'motherduck')
        db_path: Database path (used for local storage, ignored for MotherDuck)
        
    Returns:
        DLT destination object
        
    Raises:
        IngestionError: If storage backend is invalid or MotherDuck config is missing
    """
    if storage == "local":
        return dlt.destinations.duckdb(str(db_path))
    elif storage == "motherduck":
        # For MotherDuck, use environment-configured database
        # The credentials are configured in .dlt/secrets.toml
        # Database name is determined by SPORTS_ANALYTICS_ENV (dev/prod)
        try:
            db_name = get_motherduck_database()
            logger.info(f"Using MotherDuck database: {db_name} (environment: {ENVIRONMENT})")
            
            # Create MotherDuck destination with dynamic database name
            return dlt.destinations.motherduck(database=db_name)
        except Exception as e:
            raise IngestionError(
                f"Failed to create MotherDuck destination: {e}. "
                "Ensure MotherDuck credentials are configured in .dlt/secrets.toml"
            ) from e
    else:
        raise IngestionError(f"Invalid storage backend: {storage}. Use 'local' or 'motherduck'")


# Global rate limiter instance
_rate_limiter = RateLimiter()


# Create a reusable retry decorator for API calls
api_retry = retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=retry_if_exception_type((HTTPError, URLError, ConnectionError, TimeoutError)),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True
)


def _date_range(start: date, end: date) -> Generator[date, None, None]:
    """Yield dates from start to end (inclusive)."""
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)


# Raw data resources for DLT ingestion


# dlt resources for raw data ingestion


@dlt.resource(
    name="scoreboard",
    write_disposition="append",
)
def scoreboard_resource(start_date: date, end_date: date) -> Iterator[Dict[str, Any]]:
    """Raw scoreboard data from ESPN API using dlt's built-in caching."""
    resource = ESPNAPIResource("scoreboard", "append")
    
    for dt in _date_range(start_date, end_date):
        date_str = dt.strftime("%Y%m%d")
        
        # Configure endpoint for this date
        resources_config = [
            {
                "name": "scoreboard_data",
                "endpoint": {
                    "path": "scoreboard",
                    "params": {"dates": date_str},
                    "paginator": None,
                    "data_selector": "$",
                },
            }
        ]
        
        # Create API source and process responses
        api_source = resource._create_api_source(resources_config)
        yield from resource._process_responses(api_source, date=date_str)


@dlt.resource(
    name="teams",
    write_disposition="replace",
)
def teams_resource() -> Iterator[Dict[str, Any]]:
    """Raw teams data from ESPN API - team reference information."""
    resource = ESPNAPIResource("teams", "replace")
    
    # Configure teams endpoint
    resources_config = [
        {
            "name": "teams_data",
            "endpoint": {
                "path": "teams",
                "paginator": None,
                "data_selector": "$.sports[0].leagues[0].teams",
            },
        }
    ]
    
    # Create API source and process responses
    api_source = resource._create_api_source(resources_config)
    yield from resource._process_responses(api_source)


@dlt.resource(
    name="rosters",
    write_disposition="replace",
)
def rosters_resource() -> Iterator[Dict[str, Any]]:
    """Raw roster data from ESPN API for all teams."""
    resource = ESPNAPIResource("rosters", "replace")
    
    # First get all teams to iterate through their rosters
    teams_data = list(teams_resource())
    
    for team_data in teams_data:
        team_id = team_data.get("team", {}).get("id")
        if not team_id:
            continue
            
        # Rate limiting for roster requests
        resource.rate_limiter.wait_if_needed()
        
        # Configure roster endpoint for this team
        resources_config = [
            {
                "name": f"roster_{team_id}",
                "endpoint": {
                    "path": f"teams/{team_id}/roster",
                    "paginator": None,
                    "data_selector": "$",
                },
            }
        ]
        
        # Create API source and process responses with team context
        api_source = resource._create_api_source(resources_config)
        yield from resource._process_responses(api_source, team_id=team_id)
@dlt.resource(
    name="game_summary",
    write_disposition="append",
)
def game_summary_resource(target_date: date) -> Iterator[Dict[str, Any]]:
    """Raw game summary data from ESPN API."""
    resource = ESPNAPIResource("game_summary", "append")
    
    # First get scoreboard for the date to find events
    scoreboard_data = list(scoreboard_resource(target_date, target_date))
    
    event_ids = []
    for scoreboard_record in scoreboard_data:
        events = scoreboard_record.get("events", [])
        for event in events:
            event_id = event.get("id")
            if event_id:
                event_ids.append(event_id)

    if not event_ids:
        return

    # Configure REST API source for summary data
    resources_config = []
    for i, event_id in enumerate(event_ids):
        resources_config.append({
            "name": f"summary_{i:03d}",
            "endpoint": {
                "path": "summary",
                "params": {"event": event_id},
                "paginator": None,
                "data_selector": "$",
            },
        })

    # Create API source
    api_source = resource._create_api_source(resources_config)

    # Yield raw summary data with event context
    for i, (resource_name, api_resource) in enumerate(api_source.resources.items()):
        event_id = event_ids[i] if i < len(event_ids) else None
        for summary_data in api_resource:
            yield resource._add_metadata(
                summary_data,
                date=target_date.strftime("%Y%m%d"),
                event_id=event_id
            )


# Main ingestion functions


@api_retry
def ingest_season_schedule(
    season_end_year: int,
    db_path: str | Path,
    tables: Optional[set[str]] = None,
    *,
    start: Optional[date] = None,
    end: Optional[date] = None,
    storage: str = "local",
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

    logger.info(f"Ingesting season schedule from {start} to {end}")
    if tables:
        logger.info(f"Selected resources: {', '.join(sorted(tables))}")

    # Create dlt pipeline with appropriate destination
    # Both local and MotherDuck use simple "ingest" schema name
    pipeline = dlt.pipeline(
        pipeline_name="nba_schedule",
        destination=get_dlt_destination(storage, db_path),
        dataset_name="ingest",
    )

    # Build resources list based on selected resources
    resources_to_run = []
    if tables is None or 'scoreboard' in tables:
        # Scoreboard resource contains raw schedule data
        resources_to_run.append(scoreboard_resource(start, end))

    # Run the pipeline with selected resources
    info = pipeline.run(resources_to_run)

    # Log results
    logger.info(f"Schedule pipeline completed. Loaded {len(info.loads_ids)} loads.")
    if hasattr(info, "has_failed_jobs") and info.has_failed_jobs:
        logger.error("Pipeline had failed jobs - check dlt logs for details")


@api_retry
def ingest_date(
    target_date: date,
    db_path: str | Path,
    tables: Optional[set[str]] = None,
    *,
    delay: float = DEFAULT_API_DELAY,
    storage: str = "local",
) -> None:
    """Ingest NBA data for a specific date using dlt.

    Creates player_box_score and box_score tables for the given date.

    Args:
        target_date: Date to fetch data for
        db_path: Path to DuckDB database file
        tables: Optional set of tables to ingest. Available: {'box_score', 'player_box_score'}
        delay: Base delay between API calls in seconds
    """
    logger.info(f"Ingesting data for date {target_date}")
    if tables:
        logger.info(f"Selected resources: {', '.join(sorted(tables))}")
    
    # Configure rate limiter for this operation
    _rate_limiter.base_delay = max(MIN_API_DELAY, min(delay, MAX_API_DELAY))
    logger.debug(f"Rate limiter configured with {_rate_limiter.base_delay}s base delay")

    # Create dlt pipeline with appropriate destination
    # Both local and MotherDuck use simple "ingest" schema name
    pipeline = dlt.pipeline(
        pipeline_name="nba_daily",
        destination=get_dlt_destination(storage, db_path),
        dataset_name="ingest",
    )

    # Build resources list based on selected resources
    resources_to_run = []
    if tables is None or 'scoreboard' in tables:
        # Scoreboard resource contains raw schedule data
        resources_to_run.append(scoreboard_resource(target_date, target_date))
    if tables is None or 'game_summary' in tables:
        # Game summary resource contains raw box score data
        resources_to_run.append(game_summary_resource(target_date))

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
    storage: str = "local",
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

    logger.info(f"Backfilling box scores from {start} to {end}")
    if tables:
        logger.info(f"Selected resources: {', '.join(sorted(tables))}")

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
            ingest_date(current_date, db_path, tables, delay=delay, storage=storage)
            processed_count += 1
            logger.info(f"Processed {current_date} ({processed_count} completed)")

        except IngestionError as e:
            error_count += 1
            logger.error(f"API error processing {current_date}: {e}")
            # For API errors, we might want to continue with other dates
            continue
            
        except IngestionError as e:
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


@api_retry
def ingest_reference_data(
    db_path: str | Path,
    tables: Optional[set[str]] = None,
    *,
    storage: str = "local",
) -> None:
    """Ingest NBA reference data (teams and rosters) using dlt.

    This function ingests relatively static reference data that doesn't change frequently.
    Teams data is fairly static, rosters change periodically with transactions.

    Args:
        db_path: Path to DuckDB database file
        tables: Optional set of specific tables to ingest ('teams', 'rosters')
        storage: Storage backend ('local' or 'motherduck')
    """
    logger.info("Ingesting NBA reference data")
    if tables:
        logger.info(f"Selected resources: {', '.join(sorted(tables))}")

    # Create dlt pipeline with appropriate destination
    pipeline = dlt.pipeline(
        pipeline_name="nba_reference",
        destination=get_dlt_destination(storage, db_path),
        dataset_name="ingest",
    )

    # Build resources list based on selected resources
    resources_to_run = []
    if tables is None or 'teams' in tables:
        logger.info("Adding teams resource")
        resources_to_run.append(teams_resource())
    if tables is None or 'rosters' in tables:
        logger.info("Adding rosters resource")
        resources_to_run.append(rosters_resource())

    if not resources_to_run:
        logger.warning("No resources selected for reference data ingestion")
        return

    info = pipeline.run(resources_to_run)

    # Log results
    logger.info(f"Reference data pipeline completed. Loaded {len(info.loads_ids)} loads.")
    if hasattr(info, "has_failed_jobs") and info.has_failed_jobs:
        logger.error("Pipeline had failed jobs - check dlt logs for details")


__all__ = [
    # Core pipeline functions
    "ingest_season_schedule",
    "ingest_date",
    "backfill_box_scores",
    "ingest_reference_data",
    # Exception classes
    "IngestionError",
    # Rate limiting
    "RateLimiter",
    # Constants
    "DEFAULT_API_DELAY",
    "MIN_API_DELAY", 
    "MAX_API_DELAY",
    # Raw data resources (for advanced users)
    "scoreboard_resource",
    "game_summary_resource",
    "teams_resource",
    "rosters_resource",
]
