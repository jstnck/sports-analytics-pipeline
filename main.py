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
    ingest_season_schedule,
    ingest_date,
    backfill_box_scores,
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


def parse_date_safe(date_str: str, field_name: str) -> date:
    """Parse date string with proper error handling."""
    try:
        return date.fromisoformat(date_str)
    except ValueError as e:
        raise ValueError(
            f"Invalid {field_name} format '{date_str}'. Use YYYY-MM-DD format."
        ) from e


def execute_cli_operation(
    args: Any, operation_name: str, operation_func: Any, **kwargs: Any
) -> None:
    """Execute operation with standardized logging and error handling."""
    # Log operation start
    logger.info(f"Ingesting {operation_name} to {args.storage}")

    # Log operation details
    details = {
        k: v for k, v in kwargs.items() if k.endswith("_date") or k == "season_end_year"
    }
    for key, value in details.items():
        if value is not None:
            logger.info(f"{key}: {value}")

    # Log selected resources
    tables = kwargs.get("tables")  # Fixed: use 'tables' not 'selected_tables'
    if tables:
        logger.info(f"Resources to ingest: {', '.join(sorted(tables))}")

    # Execute operation
    operation_func(**kwargs)

    # Log success
    logger.info(f"{operation_name} ingestion completed successfully")


def main() -> None:
    """Main entry point with command-line interface for dlt pipeline operations."""
    parser = argparse.ArgumentParser(
        description="NBA data ingestion pipeline using dlt (data load tool)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
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
  python main.py --demo

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

    # Demo mode
    parser.add_argument(
        "--demo",
        action="store_true",
        help="Run a quick demo ingesting last 3 days of data",
    )

    # Operation type
    group = parser.add_mutually_exclusive_group(
        required=False
    )  # Not required if --demo is used
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

    # Handle demo mode
    if args.demo:
        run_demo(args.db_path, selected_tables, delay=args.delay, storage=args.storage)
        return

    # Validate that one operation is specified
    if not any(
        [
            args.season_schedule,
            args.date,
            args.backfill,
            args.reference,
        ]
    ):
        parser.error(
            "one of the arguments --season-schedule --date --backfill --reference is required (or use --demo)"
        )

    try:
        db_path = Path(args.db_path)

        if args.season_schedule:
            if selected_tables and "scoreboard" not in selected_tables:
                logger.warning(
                    "Season schedule ingestion only affects the scoreboard resource"
                )

            execute_cli_operation(
                args,
                "season schedule",
                ingest_season_schedule,
                season_end_year=args.season_schedule,  # Note: parameter name is 'season_end_year'
                db_path=db_path,
                tables=selected_tables,  # Note: parameter name is 'tables'
                storage=args.storage,
            )

        elif args.date:
            target_date = parse_date_safe(args.date, "date")
            execute_cli_operation(
                args,
                "daily data",
                ingest_date,
                target_date=target_date,
                db_path=db_path,
                tables=selected_tables,  # Note: parameter name is 'tables'
                storage=args.storage,
            )

        elif args.backfill:
            # Parse dates with error handling
            start_date = (
                parse_date_safe(args.start, "start date") if args.start else None
            )
            end_date = parse_date_safe(args.end, "end date") if args.end else None

            execute_cli_operation(
                args,
                "box score backfill",
                backfill_box_scores,
                season_end_year=args.backfill,  # Note: parameter name is 'season_end_year'
                db_path=db_path,
                start=start_date,
                end=end_date,
                tables=selected_tables,  # Note: parameter name is 'tables'
                delay=args.delay,
                storage=args.storage,
            )

        elif args.reference:
            # Validate reference tables
            if selected_tables:
                reference_tables = RESOURCE_CATEGORIES["reference"]
                invalid_reference = selected_tables - reference_tables
                if invalid_reference:
                    logger.warning(
                        f"Non-reference resources ignored: {', '.join(invalid_reference)}"
                    )
                    selected_tables = selected_tables.intersection(reference_tables)

            from sports_analytics_pipeline.ingest import ingest_reference_data

            execute_cli_operation(
                args,
                "reference data",
                ingest_reference_data,
                db_path=db_path,
                tables=selected_tables,  # Note: parameter name is 'tables', not 'selected_tables'
                storage=args.storage,
            )

    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        raise


def run_demo(
    db_path: str,
    selected_tables: set[str] | None = None,
    delay: float = 0.2,
    storage: str = "local",
) -> None:
    """Run a quick demonstration of the dlt pipeline focused on last 3 days of October 2024."""
    logger.info("Running dlt pipeline demo (last 3 days of October 2024)...")

    db_path_obj = Path(db_path)

    # Focus on just the last 3 days of October 2024 for speed
    demo_dates = [
        date(2024, 10, 29),  # Oct 29
        date(2024, 10, 30),  # Oct 30
        date(2024, 10, 31),  # Oct 31 (last day of October)
    ]

    logger.info("Demo: Ingesting data for last 3 days of October 2024 only...")
    if selected_tables:
        logger.info(f"Demo: Resources to ingest: {', '.join(sorted(selected_tables))}")

    # Skip schedule ingestion entirely - just do daily data for the 3 dates

    logger.info("Demo: Ingesting daily data for last 3 days of October 2024...")
    for demo_date in demo_dates:
        logger.info(f"  Processing {demo_date}")
        try:
            ingest_date(
                demo_date, db_path_obj, selected_tables, delay=delay, storage=storage
            )
            logger.info(f"  ✓ Completed {demo_date}")
        except Exception as e:
            logger.warning(f"  ✗ Failed to process {demo_date}: {e}")

    # Show what was created (simplified)
    logger.info("Demo: Checking ingested data...")
    try:
        if storage == "local":
            import duckdb

            conn = duckdb.connect(str(db_path_obj))
            tables = [
                "scoreboard",
                "game_summary",
            ]  # Updated to match current raw data schema
            for table in tables:
                try:
                    result = conn.execute(
                        f"SELECT COUNT(*) FROM ingest.{table}"
                    ).fetchone()
                    if result:
                        count = result[0]
                        logger.info(f"  {table}: {count} rows")
                    else:
                        logger.info(f"  {table}: no data")
                except Exception:
                    logger.info(f"  {table}: not found")
            conn.close()
        else:
            logger.info("  MotherDuck: Check web interface for results")
    except Exception as e:
        logger.info(f"  Could not check results: {e}")

    logger.info("Demo completed!")
    if storage == "local":
        logger.info(f"Local database: {db_path_obj.absolute()}")
    else:
        logger.info(f"MotherDuck database: {get_motherduck_database()}.ingest")


if __name__ == "__main__":
    main()
