def main(run_scrape: bool = False) -> None:
    """Entry point for the package.

    If run_scrape is True, run the season scraper for 2025-2026 and print
    the first few rows. Otherwise print a greeting.
    """
    if run_scrape:
        # Local import to keep startup light when not scraping
        from sports_analytics_pipeline.scraper import scrape_season

        df = scrape_season(2026)
        print(df.head())
    else:
        print("Hello from sports-analytics-pipeline!")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--scrape", action="store_true", help="Run the 2025-2026 season scraper"
    )
    args = parser.parse_args()
    main(run_scrape=args.scrape)
