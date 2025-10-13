def main(run_ingest: bool = False) -> None:
    """Entry point for the package.

    If run_ingest is True, run the season ingest for 2025-2026 and print
    the first few rows. Otherwise print a greeting.
    """
    if run_ingest:
        # Local import to keep startup light when not ingesting
        from sports_analytics_pipeline.ingest import ingest_season

        df = ingest_season(2026)
        print(df.head())
    else:
        print("Hello from sports-analytics-pipeline!")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--ingest", action="store_true", help="Run the 2025-2026 season ingest"
    )
    args = parser.parse_args()
    main(run_ingest=args.ingest)
