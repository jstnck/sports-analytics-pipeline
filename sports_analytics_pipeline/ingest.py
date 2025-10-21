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
import requests
from urllib.error import HTTPError, URLError

import dlt
from dlt.sources.rest_api import rest_api_source, RESTAPIConfig
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

from .config import ENVIRONMENT, get_motherduck_database


# User-Agent for API requests
USER_AGENT = "sports-analytics-pipeline-dlt/0.1 (+https://github.com/jstnck)"


# ESPN API configuration
BASE_URL = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba"

# API Rate limiting configuration
DEFAULT_API_DELAY = 0.2  # Default delay between API calls in seconds
MIN_API_DELAY = 0.05  # Minimum delay
MAX_API_DELAY = 2.0  # Maximum delay

logger = logging.getLogger(__name__)


# Error handling
class IngestionError(Exception):
    """Custom exception for ingestion-related errors."""

    pass


def _setup_dlt_config_for_environment() -> None:
    """Setup DLT configuration directory based on current environment.
    
    Creates a symbolic link from .dlt to the appropriate environment-specific
    configuration directory (.dlt-dev or .dlt-prod). This allows us to have
    separate MotherDuck database configurations for dev and prod environments.
    
    The solution uses:
    - .dlt-dev/secrets.toml: contains database = "sports_analytics_dev"
    - .dlt-prod/secrets.toml: contains database = "sports_analytics_prod"
    - Symlink .dlt switches between them based on SPORTS_ANALYTICS_ENV
    """
    import os
    from pathlib import Path
    
    # Get current working directory (where .dlt directories are located)
    cwd = Path.cwd()
    dlt_link = cwd / ".dlt"
    
    # Determine target directory based on current environment (read fresh from env)
    current_env = os.environ.get("SPORTS_ANALYTICS_ENV", "dev").lower()
    if current_env == "prod":
        target_dir = cwd / ".dlt-prod"
    else:
        target_dir = cwd / ".dlt-dev"
    
    # Check that target directory exists
    if not target_dir.exists():
        raise FileNotFoundError(f"Environment directory {target_dir.name} not found. Please create it with appropriate secrets.toml file.")
    
    # Remove existing .dlt if it's a symlink or directory
    if dlt_link.exists() or dlt_link.is_symlink():
        if dlt_link.is_symlink():
            dlt_link.unlink()
        elif dlt_link.is_dir():
            # Only remove if it's our managed symlink (check if target exists)
            try:
                # Back up the original .dlt directory if it's not a symlink
                backup_dir = cwd / ".dlt-original"
                if not backup_dir.exists():
                    dlt_link.rename(backup_dir)
                    logger.info("Backed up original .dlt to .dlt-original")
                else:
                    # Remove the current .dlt since we have a backup
                    import shutil
                    shutil.rmtree(dlt_link)
            except Exception as e:
                logger.warning(f"Could not backup/remove .dlt directory: {e}")
                return
    
    # Create symbolic link to environment-specific config
    try:
        dlt_link.symlink_to(target_dir.name)
        logger.info(f"Switched DLT config to {target_dir.name} for {current_env} environment")
    except Exception as e:
        logger.error(f"Failed to create symlink to {target_dir}: {e}")
        raise IngestionError(f"Could not setup DLT configuration for {current_env} environment")


class RateLimiter:
    """Simple rate limiter for API calls."""

    def __init__(self, base_delay: float = DEFAULT_API_DELAY) -> None:
        self.base_delay = max(MIN_API_DELAY, min(base_delay, MAX_API_DELAY))
        self.last_call_time = 0.0

    def wait_if_needed(self) -> None:
        """Wait if needed to respect rate limits."""
        current_time = time.time()
        time_since_last_call = current_time - self.last_call_time

        if time_since_last_call < self.base_delay:
            sleep_time = self.base_delay - time_since_last_call
            logger.debug(f"Rate limiting: sleeping for {sleep_time:.3f}s")
            time.sleep(sleep_time)

        self.last_call_time = time.time()


class ESPNAPIResource:
    """Base class for ESPN API resources with DLT integration.
    Provides common REST API configuration, metadata handling, and response processing.
    """

    def __init__(self):
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
        """Add ingestion timestamp and optional context metadata to response data.

        Args:
            data: Original API response data
            **extra_metadata: Optional context (e.g., date, team_id, event_id)

        Returns:
            Data with ingestion timestamp and metadata added
        """
        return {
            "ingested_at": datetime.now(timezone.utc).isoformat(),
            **extra_metadata,
            **data,
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
        # MotherDuck destination setup with environment-specific database targeting
        try:
            db_name = get_motherduck_database()
            logger.info(
                f"Using MotherDuck database: {db_name} (environment: {ENVIRONMENT})"
            )

            # Switch to the appropriate .dlt configuration directory
            # This sets up the connection string (md:database_name) in secrets.toml
            _setup_dlt_config_for_environment()
            
            return dlt.destinations.motherduck()
        except Exception as e:
            raise IngestionError(
                f"Failed to create MotherDuck destination: {e}. "
                "Ensure MotherDuck credentials are configured in .dlt/secrets.toml"
            ) from e
    else:
        raise IngestionError(
            f"Invalid storage backend: {storage}. Use 'local' or 'motherduck'"
        )


# Global rate limiter instance
_rate_limiter = RateLimiter()


# Create a reusable retry decorator for API calls
api_retry = retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=retry_if_exception_type((HTTPError, URLError, ConnectionError, TimeoutError)),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)


# dlt resources for raw data ingestion


@dlt.resource(
    name="scoreboard",
    write_disposition="append",
)
def scoreboard_resource(start_date: date, end_date: date) -> Iterator[Dict[str, Any]]:
    """Raw scoreboard data from ESPN API using dlt's built-in caching."""
    resource = ESPNAPIResource()

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
    resource = ESPNAPIResource()

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
    write_disposition="merge",
)
def rosters_resource() -> Iterator[Dict[str, Any]]:
    """Raw roster data from ESPN API for all teams."""
    resource = ESPNAPIResource()

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
def game_summary_resource(start_date: date, end_date: Optional[date] = None) -> Iterator[Dict[str, Any]]:
    """Raw game summary data from ESPN API using dlt's built-in caching.
    
    Args:
        start_date: Starting date for game summary data
        end_date: Ending date for game summary data. If None, defaults to start_date
    """
    resource = ESPNAPIResource()
    
    # Default end_date to start_date for single-date processing
    if end_date is None:
        end_date = start_date

    # Process each date in the range
    for target_date in _date_range(start_date, end_date):
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
            continue

        # Configure REST API source for summary data
        resources_config = []
        for i, event_id in enumerate(event_ids):
            resources_config.append(
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

        # Create API source
        api_source = resource._create_api_source(resources_config)

        # Yield raw summary data with event context
        for i, (resource_name, api_resource) in enumerate(api_source.resources.items()):
            event_id = event_ids[i] if i < len(event_ids) else None
            for summary_data in api_resource:
                yield resource._add_metadata(
                    summary_data, date=target_date.strftime("%Y%m%d"), event_id=event_id
                )


@dlt.source(name="espn_nba")
def espn_nba_source(
    season_end_year: int = 2025,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    include_teams: bool = True,
    include_rosters: bool = True,
    include_schedule: bool = True,
    include_game_summary: bool = True,
    max_table_nesting: Optional[int] = None,
):
    """DLT source that combines all ESPN NBA resources.
    
    This source provides a unified interface to all ESPN NBA data resources,
    allowing for coordinated ingestion of teams, rosters, schedules, and game data.
    
    Args:
        season_end_year: Season to fetch data for (e.g., 2025 for 2024-25 season)
        start_date: Start date for date-range resources (schedule/game_summary). 
                   If None, uses season defaults
        end_date: End date for date-range resources. If None, defaults to start_date
        include_teams: Include teams reference data
        include_rosters: Include rosters reference data
        include_schedule: Include schedule/scoreboard data  
        include_game_summary: Include game summary/box score data
        max_table_nesting: Maximum table nesting level for JSON flattening.
                          If None, uses DLT default. Use 0 for no nesting, 1 for minimal nesting.
        
    Returns:
        DLT source with configured resources and table nesting settings
    """
    resources = []
    
    # Reference data - can include teams and/or rosters independently
    if include_teams:
        resources.append(teams_resource())
        
    if include_rosters:
        resources.append(rosters_resource())
    
    # Determine date range for schedule/game data
    if start_date is None or end_date is None:
        # Use season defaults if dates not specified
        season_start = date(season_end_year - 1, 10, 1)  # October 1st
        season_end = date(season_end_year, 6, 30)        # June 30th
        
        if start_date is None:
            start_date = season_start
        if end_date is None:
            end_date = season_end if start_date == season_start else start_date
    
    # Schedule/scoreboard data
    if include_schedule:
        resources.append(scoreboard_resource(start_date, end_date))
    
    # Game summary/box score data
    if include_game_summary:
        resources.append(game_summary_resource(start_date, end_date))
    
    # Apply max_table_nesting to all resources if specified
    if max_table_nesting is not None:
        for resource in resources:
            resource.max_table_nesting = max_table_nesting
    
    return resources


def run_espn_source(
    *,
    season_end_year: Optional[int] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    include_teams: bool = True,
    include_rosters: bool = True,
    include_schedule: bool = True,
    include_game_summary: bool = True,
    max_table_nesting: Optional[int] = None,
    db_path: str | Path,
    storage: str = "local",
) -> Any:
    """Run the ESPN NBA source with unified configuration.
    
    This is the modern way to run ESPN data ingestion, replacing the individual
    ingest_* functions with a unified source-based approach.
    
    Args:
        season_end_year: NBA season end year (e.g., 2025 for 2024-25 season)
        start_date: Start date for date-range resources (schedule/game_summary)
        end_date: End date for date-range resources  
        include_teams: Include teams reference data
        include_rosters: Include rosters reference data
        include_schedule: Include schedule/scoreboard data
        include_game_summary: Include game summary/box score data
        max_table_nesting: Maximum table nesting level for JSON flattening.
                          If None, uses DLT default. Use 0 for no nesting, 1 for minimal nesting.
        db_path: Path to database file
        storage: Storage backend ('local' or 'motherduck')
        
    Returns:
        Pipeline run info
    """
    import dlt
    
    # Use current season if not specified
    if season_end_year is None:
        season_end_year = 2025
    
    logger.info(f"Running ESPN NBA source (season {season_end_year-1}-{season_end_year%100:02d})")
    
    # Create the ESPN NBA source
    source = espn_nba_source(
        season_end_year=season_end_year,
        start_date=start_date,
        end_date=end_date,
        include_teams=include_teams,
        include_rosters=include_rosters,
        include_schedule=include_schedule,
        include_game_summary=include_game_summary,
        max_table_nesting=max_table_nesting,
    )
    
    logger.info(f"Resources: {list(source.resources.keys())}")
    
    # Create destination and pipeline
    destination = get_dlt_destination(storage, db_path)
    pipeline = dlt.pipeline(
        pipeline_name="nba_espn_source",
        destination=destination,
        dataset_name="raw",
    )
    
    # Run with recovery
    info = _run_pipeline_with_recovery(pipeline, source)
    
    logger.info(f"Pipeline completed. Loaded {len(info.loads_ids)} loads.")
    if hasattr(info, "has_failed_jobs") and info.has_failed_jobs:
        logger.warning(f"⚠️  Some jobs failed - check logs for details")
    
    return info


# Helper functions


def _run_pipeline_with_recovery(pipeline: dlt.Pipeline, resources: Any, max_attempts: int = 2) -> Any:
    """Run DLT pipeline with simplified recovery for source-based pipelines.
    
    Since @dlt.source() handles shared state better, we only need basic retry logic.
    
    Args:
        pipeline: DLT pipeline instance
        resources: DLT source or list of resources to run
        max_attempts: Maximum number of retry attempts (default: 2)
        
    Returns:
        Pipeline run info
        
    Raises:
        Exception: If pipeline still fails after retry attempts
    """
    last_exception = None
    
    for attempt in range(max_attempts):
        try:
            logger.debug(f"Running pipeline (attempt {attempt + 1}/{max_attempts})")
            return pipeline.run(resources)
            
        except Exception as e:
            last_exception = e
            
            if attempt < max_attempts - 1:  # Don't retry on last attempt
                logger.warning(f"Pipeline attempt {attempt + 1} failed: {e}")
                logger.info(f"Retrying... (attempt {attempt + 2}/{max_attempts})")
                
                # Brief pause before retry
                time.sleep(1.0)
                continue
            else:
                logger.error(f"Pipeline failed after {max_attempts} attempts: {e}")
                break
                
    raise last_exception


# Main ingestion functions


# Helper functions

def _date_range(start: date, end: date) -> Generator[date, None, None]:
    """Yield dates from start to end (inclusive)."""
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)


# Global rate limiter instance
_rate_limiter = RateLimiter()


def api_retry(
    max_retries: int = 3,
    backoff_factor: float = 2.0,
    exceptions: tuple = (requests.exceptions.RequestException, requests.exceptions.Timeout),
) -> Any:
    """Decorator for API call retry logic with exponential backoff."""

    def decorator(func: Any) -> Any:
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt == max_retries:
                        logger.error(f"Final retry failed for {func.__name__}: {e}")
                        break
                    
                    wait_time = backoff_factor ** attempt
                    logger.warning(
                        f"Attempt {attempt + 1} failed for {func.__name__}: {e}. "
                        f"Retrying in {wait_time}s..."
                    )
                    time.sleep(wait_time)
            
            raise last_exception
        return wrapper
    return decorator


__all__ = [
    # Modern DLT source approach
    "espn_nba_source",
    "run_espn_source",
    # Individual resources (for advanced users)
    "scoreboard_resource",
    "game_summary_resource", 
    "teams_resource",
    "rosters_resource",
    # Exception classes
    "IngestionError",
    # Rate limiting
    "RateLimiter",
    # Constants
    "DEFAULT_API_DELAY",
    "MIN_API_DELAY",
    "MAX_API_DELAY",
]
