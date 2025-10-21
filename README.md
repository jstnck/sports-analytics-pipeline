# Sports Analytics Pipeline

NBA data ingestion pipeline that collects data from ESPN's public API and loads it into DuckDB or MotherDuck cloud database.

## CLI Commands

The pipeline provides several ingestion modes:

**Reference Data** - Load static team and roster information:
```bash
uv run python main.py --reference [--tables teams,rosters]
```

**Season Schedule** - Load complete season schedule for a given year:
```bash
uv run python main.py --season-schedule 2025
```

**Daily Data** - Load scoreboard and game summaries for specific date:
```bash
uv run python main.py --date 2024-12-25 [--tables scoreboard,game_summary]
```

**Historical Backfill** - Load data for date ranges:
```bash
uv run python main.py --backfill 2025 --start 2024-10-29 --end 2024-10-31
```

**All Resources** - Load teams, rosters, schedule, and available games:
```bash
uv run python main.py --all [--storage motherduck] [--depth 2]
```

**Options:**
- `--storage`: Choose `duckdb` (default) or `motherduck`  
- `--tables`: Comma-separated list to filter resources
- `--depth`: dlt JSON nesting level

## Data & Configuration

**Resources:** `scoreboard` (schedules/scores), `game_summary` (detailed stats), `teams` (metadata), `rosters` (players)

**Storage:** Local DuckDB or MotherDuck cloud with 'dev' and 'prod' databases)

**Setup:** `uv sync` to install, configure MotherDuck credentials in `.dlt/secrets.toml`