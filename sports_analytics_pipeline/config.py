"""Simple environment configuration for dev/prod database selection."""

import os
from pathlib import Path

# Environment (dev/prod)
ENVIRONMENT = os.environ.get("SPORTS_ANALYTICS_ENV", "dev").lower()

# Local database path
LOCAL_DB_PATH = Path("data/sports_analytics.duckdb")


def get_motherduck_database() -> str:
    """Get the MotherDuck database name for current environment."""
    if ENVIRONMENT == "prod":
        return "sports_analytics_prod"
    return "sports_analytics_dev"  # Default to dev for safety
