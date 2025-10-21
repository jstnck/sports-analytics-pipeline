#!/usr/bin/env python3
"""Command-line interface for the sports analytics pipeline.

This provides a CLI for the dlt-based ingestion system with individual
function calls that can be easily orchestrated by external systems.
Each operation is atomic and can be called independently.
"""

from __future__ import annotations

import argparse
import logging
import os
from datetime import date
from pathlib import Path
from typing import Any

from sports_analytics_pipeline.ingest import (
    run_espn_source,
)
from sports_analytics_pipeline.config import (
    ENVIRONMENT,
    LOCAL_DB_PATH,
    get_motherduck_database,
)

# Set ENVIRONMENT variable for DLT to use for configuration selection
os.environ["ENVIRONMENT"] = ENVIRONMENT

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# Resource configuration constants
RESOURCE_CATEGORIES = {
    "all": {"scoreboard", "game_summary", "teams", "rosters"},
    "reference": {"teams", "rosters"},
}


def get_resource_selection(selected_tables: set[str] | None) -> dict[str, bool]:
    """Determine which resources to include based on table selection.
    
    Returns a dictionary with include_* flags for all resource types.
    """
    return {
        "include_teams": selected_tables is None or "teams" in selected_tables,
        "include_rosters": selected_tables is None or "rosters" in selected_tables,
        "include_schedule": selected_tables is None or "scoreboard" in selected_tables,
        "include_game_summary": selected_tables is None or "game_summary" in selected_tables,
    }


def parse_date_safe(date_str: str, field_name: str) -> date:
    """Parse date string with proper error handling."""
    try:
        return date.fromisoformat(date_str)
    except ValueError as e:
        raise ValueError(
            f"Invalid {field_name} format '{date_str}'. Use YYYY-MM-DD format."
        ) from e


def execute_source_operation(
    args: Any, 
    operation_name: str,
    **source_kwargs: Any
) -> None:
    """Execute ESPN source operation with simple logging and error handling."""
    logger.info(f"Starting {operation_name} | Storage: {args.storage}")
    
    try:
        run_espn_source(**source_kwargs)
        logger.info(f"✅ {operation_name} completed successfully")
    except Exception as e:
        logger.error(f"❌ {operation_name} failed: {e}")
        raise


def run_all_resources(args: Any, db_path: Path) -> None:
    """Run all ESPN API resources using the unified @dlt.source.
    
    Simply runs all resources in a single operation using the standard execution path.
    """
    logger.info("🚀 Running ALL ESPN API resources")
    
    # Get current season year (assuming it's 2025 for now)
    current_season = 2025
    
    execute_source_operation(
        args,
        "all resources",
        season_end_year=current_season,
        include_teams=True,
        include_rosters=True,
        include_schedule=True,
        include_game_summary=True,
        max_table_nesting=args.depth,
        db_path=db_path,
        storage=args.storage,
    )


def main() -> None:
    """Main entry point with command-line interface for dlt pipeline operations."""
    parser = argparse.ArgumentParser(
        description="NBA data ingestion pipeline using dlt (data load tool)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run all resources in sequence (teams, rosters, schedule, backfill)
  python main.py --all --storage motherduck

  # Ingest full 2024-25 season schedule
  python main.py --season-schedule 2025

    # Ingest data for a specific date (all resources)
  python main.py --date 2024-12-25

  # Ingest data for a specific date (specific resources)
  python main.py --date 2024-12-25 --tables scoreboard,game_summary
  python main.py --date 2024-12-25 --tables scoreboard

  # Backfill data for a date range
  python main.py --backfill 2025 --start 2024-10-01 --end 2024-12-31

  # Backfill specific resources only
  python main.py --backfill 2025 --start 2024-10-01 --end 2024-12-31 --tables game_summary

  # Demo with 3 days of data from October 2024
  python main.py --backfill 2025 --start 2024-10-29 --end 2024-10-31

  # Ingest reference data (teams and rosters)
  python main.py --reference

  # Ingest specific reference data only
  python main.py --reference --tables teams

Available resources: scoreboard, game_summary, teams, rosters
        """,
    )

    # Database configuration
    parser.add_argument(
        "--db-path",
        type=str,
        default=str(LOCAL_DB_PATH),
        help=f"Path to DuckDB database file for local storage (default: {LOCAL_DB_PATH}). Ignored for MotherDuck storage.",
    )

    # Storage backend configuration
    parser.add_argument(
        "--storage",
        type=str,
        choices=["local", "motherduck"],
        default="local",
        help="Storage backend: 'local' for local DuckDB file, 'motherduck' for MotherDuck cloud (default: local)",
    )

    # Table selection
    parser.add_argument(
        "--tables",
        type=str,
        help="Comma-separated list of resources to ingest (e.g., 'scoreboard,game_summary' or 'scoreboard'). "
        "Available resources: scoreboard, game_summary, teams, rosters. "
        "If not specified, all applicable resources will be ingested.",
    )

    # API rate limiting
    parser.add_argument(
        "--delay",
        type=float,
        default=0.2,
        help="Base delay between API calls in seconds (default: 0.2, min: 0.05, max: 2.0)",
    )

    # JSON flattening configuration
    parser.add_argument(
        "--depth",
        type=int,
        default=None,
        help="Maximum table nesting level for JSON flattening. "
        "If not specified, uses DLT default. Use 0 for no nesting, 1 for minimal nesting.",
    )

    # Operation type
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--season-schedule",
        type=int,
        help="Ingest full season schedule (provide season end year, e.g., 2025)",
    )
    group.add_argument(
        "--date",
        type=str,
        help="Ingest data for specific date (YYYY-MM-DD format)",
    )
    group.add_argument(
        "--backfill",
        type=int,
        help="Backfill box scores for season (provide season end year)",
    )
    group.add_argument(
        "--reference",
        action="store_true",
        help="Ingest reference data (teams and rosters)",
    )
    group.add_argument(
        "--all",
        action="store_true",
        help="Run all resources in sequence: teams, rosters, season schedule, and box score backfill",
    )

    # Date range options for backfill
    parser.add_argument(
        "--start",
        type=str,
        help="Start date for backfill (YYYY-MM-DD format)",
    )
    parser.add_argument(
        "--end",
        type=str,
        help="End date for backfill (YYYY-MM-DD format)",
    )

    args = parser.parse_args()

    # Show environment configuration
    logger.info(
        f"Environment: {ENVIRONMENT} | "
        f"MotherDuck DB: {get_motherduck_database()} | "
        f"Storage: {args.storage}"
    )
    if ENVIRONMENT == "prod":
        logger.warning(
            "⚠️  RUNNING IN PRODUCTION MODE - using sports_analytics_prod database"
        )

    # Parse table selection
    selected_tables = None
    if args.tables:
        available_tables = RESOURCE_CATEGORIES["all"]
        selected_tables = set(t.strip() for t in args.tables.split(","))
        invalid_tables = selected_tables - available_tables
        if invalid_tables:
            parser.error(
                f"Invalid resource(s): {', '.join(invalid_tables)}. "
                f"Available resources: {', '.join(sorted(available_tables))}"
                )

    # Validate that one operation is specified
    if not any(
        [
            args.season_schedule,
            args.date,
            args.backfill,
            args.reference,
            args.all,
        ]
    ):
        parser.error(
            "one of the arguments --season-schedule --date --backfill --reference --all is required"
        )

    try:
        db_path = Path(args.db_path)

        if args.season_schedule:
            if selected_tables and "scoreboard" not in selected_tables:
                logger.warning(
                    "Season schedule ingestion only affects the scoreboard resource"
                )

            execute_source_operation(
                args,
                "season schedule",
                season_end_year=args.season_schedule,
                db_path=db_path,
                storage=args.storage,
                include_teams=False,
                include_rosters=False,
                include_schedule=True,
                include_game_summary=False,
                max_table_nesting=args.depth,
            )

        elif args.date:
            target_date = parse_date_safe(args.date, "date")
            
            # Get resource selection based on table filters
            resource_config = get_resource_selection(selected_tables)
            
            execute_source_operation(
                args,
                "daily data",
                start_date=target_date,
                end_date=target_date,
                db_path=db_path,
                storage=args.storage,
                max_table_nesting=args.depth,
                **resource_config,
            )

        elif args.backfill:
            # Parse dates with error handling
            start_date = (
                parse_date_safe(args.start, "start date") if args.start else None
            )
            end_date = parse_date_safe(args.end, "end date") if args.end else None

            # Get resource selection based on table filters
            resource_config = get_resource_selection(selected_tables)

            execute_source_operation(
                args,
                "box score backfill",
                season_end_year=args.backfill,
                start_date=start_date,
                end_date=end_date,
                db_path=db_path,
                storage=args.storage,
                max_table_nesting=args.depth,
                **resource_config,
            )

        elif args.reference:
            # Get resource selection based on table filters (reference only)
            resource_config = get_resource_selection(selected_tables)
            # Force disable non-reference resources
            resource_config.update({
                "include_schedule": False,
                "include_game_summary": False,
            })
            
            if selected_tables:
                reference_tables = RESOURCE_CATEGORIES["reference"]
                invalid_reference = selected_tables - reference_tables
                if invalid_reference:
                    logger.warning(
                        f"Non-reference resources ignored: {', '.join(invalid_reference)}"
                    )

            execute_source_operation(
                args,
                "reference data",
                db_path=db_path,
                storage=args.storage,
                max_table_nesting=args.depth,
                **resource_config,
            )

        elif args.all:
            run_all_resources(args, db_path)

    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        raise


if __name__ == "__main__":
    main()
