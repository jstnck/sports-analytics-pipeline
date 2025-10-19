# Sports Analytics Pipeline

This app is a NBA data ingestion pipeline that collects game data from ESPN's public API and stores it in a DuckDB / Motherduck staging layer. It handles ingestion for a single source as part of a larger Sports Analytics DataWarehouse project. 

## Features

- **Data ingestion**: Season schedules, teams, venues, box scores, and player statistics
- **Dual storage**: Local DuckDB files or MotherDuck cloud database
- **Environment separation**: Dev/prod database isolation via `SPORTS_ANALYTICS_ENV`
- **CLI interface**: Command-line tools for all ingestion operations
- **Selective ingestion**: Choose specific tables and date ranges

## Quick Start

```bash
# Initialize database
python main.py --init-db

# Ingest current season schedule
python main.py --season-schedule 2025

# Ingest data for specific date
python main.py --date 2024-12-25

# Run demo
python main.py --demo
```

## Development

```bash
# Install dependencies
uv sync

# Run tests (includes live MotherDuck integration tests)
uv run pytest

# Run demo
uv run python main.py --demo
```

## Data Storage

### Local Storage
Data is stored in `data/sports_analytics.duckdb` with all tables in the `ingest` schema.

### MotherDuck Cloud Storage
- **Development**: `sports_analytics_dev.ingest` (default)
- **Production**: `sports_analytics_prod.ingest` 
- Database selection controlled by `SPORTS_ANALYTICS_ENV` environment variable
- Credentials configured in `.dlt/secrets.toml`

## Available Tables

- `schedule`: Game schedules with dates, teams, and venues
- `teams`: Team information and metadata  
- `venues`: Arena/stadium information
- `box_score`: Team-level game statistics
- `player_box_score`: Individual player game statistics

New tables can be easily added using the current API -> DLT -> DuckDB archecture.