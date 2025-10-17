#!/usr/bin/env python3
"""Main entry point for the sports analytics pipeline.

This provides a command-line interface for the dlt-based ingestion system.
It includes examples of typical usage patterns and serves as a reference for
Dagster integration.
"""

from __future__ import annotations

import argparse
import logging
from datetime import date, timedelta
from pathlib import Path

from sports_analytics_pipeline.ingest import (
    ingest_season_schedule_dlt,
    ingest_date_dlt,
    backfill_box_scores_dlt,
    dagster_ingest_season_schedule,
    dagster_ingest_daily_data,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def main() -> None:
    """Main entry point with command-line interface for dlt pipeline operations."""
    parser = argparse.ArgumentParser(
        description="NBA data ingestion pipeline using dlt (data load tool)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Ingest full 2024-25 season schedule
  python main.py --season-schedule 2025

  # Ingest data for a specific date 
  python main.py --date 2024-12-25

  # Backfill box scores for date range
  python main.py --backfill 2025 --start 2024-10-01 --end 2024-12-31

  # Dagster-style function call (returns metadata dict)
  python main.py --dagster-schedule 2025
        """,
    )

    # Database configuration
    parser.add_argument(
        "--db-path",
        type=str,
        default="data/games.duckdb",
        help="Path to DuckDB database file (default: data/games.duckdb)",
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
        "--dagster-schedule",
        type=int,
        help="Run season schedule ingestion with Dagster-style return",
    )
    group.add_argument(
        "--dagster-daily",
        type=str,
        help="Run daily ingestion with Dagster-style return (YYYY-MM-DD format)",
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

    # Handle demo mode
    if args.demo:
        run_demo(args.db_path)
        return

    # Validate that one operation is specified (if not demo)
    if not any(
        [
            args.season_schedule,
            args.date,
            args.backfill,
            args.dagster_schedule,
            args.dagster_daily,
        ]
    ):
        parser.error(
            "one of the arguments --season-schedule --date --backfill --dagster-schedule --dagster-daily is required (or use --demo)"
        )

    try:
        db_path = Path(args.db_path)

        if args.season_schedule:
            logger.info(
                f"Ingesting season schedule for season ending {args.season_schedule}"
            )
            ingest_season_schedule_dlt(args.season_schedule, db_path)
            logger.info("Season schedule ingestion completed successfully")

        elif args.date:
            target_date = date.fromisoformat(args.date)
            logger.info(f"Ingesting data for date {target_date}")
            ingest_date_dlt(target_date, db_path)
            logger.info("Daily ingestion completed successfully")

        elif args.backfill:
            start_date = date.fromisoformat(args.start) if args.start else None
            end_date = date.fromisoformat(args.end) if args.end else None

            logger.info(f"Backfilling box scores for season ending {args.backfill}")
            if start_date:
                logger.info(f"Start date: {start_date}")
            if end_date:
                logger.info(f"End date: {end_date}")

            backfill_box_scores_dlt(
                args.backfill, db_path, start=start_date, end=end_date
            )
            logger.info("Box score backfill completed successfully")

        elif args.dagster_schedule:
            logger.info(
                f"Running Dagster-style schedule ingestion for season ending {args.dagster_schedule}"
            )
            result = dagster_ingest_season_schedule(args.dagster_schedule, db_path)
            print(f"Dagster result: {result}")

        elif args.dagster_daily:
            target_date = date.fromisoformat(args.dagster_daily)
            logger.info(f"Running Dagster-style daily ingestion for date {target_date}")
            result = dagster_ingest_daily_data(target_date, db_path)
            print(f"Dagster result: {result}")

    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        raise


def run_demo(db_path: str) -> None:
    """Run a quick demonstration of the dlt pipeline."""
    logger.info("Running dlt pipeline demo...")

    # Ingest last 3 days of NBA data
    today = date.today()
    dates_to_ingest = [today - timedelta(days=i) for i in range(1, 4)]

    db_path_obj = Path(db_path)

    logger.info("Demo: Ingesting schedule data for current season...")
    # Get current NBA season end year (if it's before July, use current year, else next year)
    current_season_end = today.year + 1 if today.month >= 7 else today.year
    ingest_season_schedule_dlt(current_season_end, db_path_obj)

    logger.info("Demo: Ingesting daily data for recent dates...")
    for demo_date in dates_to_ingest:
        logger.info(f"  Processing {demo_date}")
        try:
            ingest_date_dlt(demo_date, db_path_obj)
        except Exception as e:
            logger.warning(f"  Failed to process {demo_date}: {e}")

    # Show what was created
    try:
        import duckdb

        conn = duckdb.connect(str(db_path_obj))

        logger.info("Demo: Database contents after ingestion:")

        # Check schedule table
        try:
            schedule_count = conn.execute(
                "SELECT COUNT(*) FROM main.schedule"
            ).fetchone()[0]
            logger.info(f"  schedule: {schedule_count} rows")
        except Exception:
            logger.info("  schedule: table not found")

        # Check teams table
        try:
            teams_count = conn.execute("SELECT COUNT(*) FROM main.teams").fetchone()[0]
            logger.info(f"  teams: {teams_count} rows")
        except Exception:
            logger.info("  teams: table not found")

        # Check player_box_score table
        try:
            players_count = conn.execute(
                "SELECT COUNT(*) FROM main.player_box_score"
            ).fetchone()[0]
            logger.info(f"  player_box_score: {players_count} rows")
        except Exception:
            logger.info("  player_box_score: table not found")

        # Check box_score table
        try:
            box_score_count = conn.execute(
                "SELECT COUNT(*) FROM main.box_score"
            ).fetchone()[0]
            logger.info(f"  box_score: {box_score_count} rows")
        except Exception:
            logger.info("  box_score: table not found")

        conn.close()

    except Exception as e:
        logger.warning(f"Could not inspect database: {e}")

    logger.info("Demo completed! Check the database file for results.")
    logger.info(f"Database location: {db_path_obj.absolute()}")


if __name__ == "__main__":
    main()
