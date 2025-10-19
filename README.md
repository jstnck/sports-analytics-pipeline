# Sports Analytics Pipeline

This app is a NBA data ingestion pipeline that collects game data from ESPN's public API and stores it in a DuckDB / Motherduck staging layer. It handles ingestion for a single source as part of a larger Sports Analytics DataWarehouse project. 

## Features

- **Data ingestion**: Scoreboards, game summaries, teams, and rosters from ESPN API
- **Dual storage**: Local DuckDB files or MotherDuck cloud database
- **Environment separation**: Dev/prod database isolation via `SPORTS_ANALYTICS_ENV`
- **CLI interface**: Command-line tools for all ingestion operations
- **Selective ingestion**: Choose specific resources and date ranges

## Quick Start

```bash
# Run demo
python main.py --demo

# Ingest current season schedule
python main.py --season-schedule 2025

# Ingest data for specific date
python main.py --date 2024-12-25

# Ingest reference data (teams & rosters)
python main.py --reference

# Backfill box scores for date range
python main.py --backfill 2024 --start 2024-01-01 --end 2024-01-31

# Select specific resources only
python main.py --date 2024-12-25 --tables scoreboard,game_summary
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

## Available Resources

- `scoreboard`: Game schedules, scores, and status
- `game_summary`: Detailed game summaries and box scores
- `teams`: Team information and metadata  
- `rosters`: Player rosters by team


New tables can be easily added using the current API -> DLT -> DuckDB archecture.