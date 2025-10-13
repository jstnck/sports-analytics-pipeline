"""bball_season package exports.

This package intentionally mirrors the project modules so consumers can
`import bball_season` regardless of the project directory name.
"""

from .scraper import scrape_season

__all__ = ["scrape_season"]
