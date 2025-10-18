# Sports Analytics Pipeline

NBA data ingestion pipeline that collects game data from ESPN's public API and stores it in DuckDB for analysis.

## Features

- Ingest season schedules, team info, and game statistics
- Store data in DuckDB with minimal transformations
- CLI interface for running ingestion tasks
- Table-specific ingestion (schedule, teams, venues, box scores, player stats)

## Quick Start

```bash
# Initialize database
python main.py --init-db

# Ingest current season schedule
python main.py --season-schedule 2025

# Ingest data for specific date
python main.py --date 2024-12-25

# Run demo (last 3 days)
python main.py --demo
```

## Development

```bash
# Install dependencies
uv sync

# Run tests
python -m pytest

# Format and lint
ruff format . && ruff check .

# Type check
python -m mypy sports_analytics_pipeline tests
```

## Data

Data is stored in `data/games.duckdb` with tables for schedules, teams, venues, box scores, and player statistics.
