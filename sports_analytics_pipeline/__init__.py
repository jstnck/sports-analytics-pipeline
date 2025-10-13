"""sports_analytics_pipeline package exports.

This package mirrors the project modules and exposes a stable import name
`sports_analytics_pipeline` for consumers.
"""

from .scraper import scrape_season

__all__ = ["scrape_season"]
