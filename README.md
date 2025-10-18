Sports Analytics Pipeline

This application ingests data about NBA games from ESPN. It is the first step of a large data analytics pipeline.

This is the data ingestion section - using python to consume from the ESPN public API, or web-scrape data on games, schedules, players, teams, etc. 

NBA data will then be minimally transformed and saved in the staging layer of a DuckDB database

## Development

This project uses `uv` to manage dependencies and a lightweight dev toolchain:

- ruff: linter and formatter (fast, replaces flake8 + isort + some auto-fixes)
- mypy: static type checker

Install dev tools into the virtual environment:

```bash
cd sports-analytics-pipeline
uv add --dev mypy pandas-stubs types-requests ruff
source .venv/bin/activate
```

Run the checks and formatter:

```bash
# Format / auto-fix issues
ruff format .
# Lint (shows warnings/errors)
ruff check .
# Type check
python -m mypy sports_analytics_pipeline tests
```

Follow the coding conventions in `.copilot-instructions.md` (docstrings, type hints, pytest-style tests).

## Data model

A minimal description of the DuckDB data model is available in `docs/data-model.md`.

## Analysis
Ad-hoc analysis on the scraped data can be done using your tool of choice - here we use the DuckDB ui

## ToDo
Currently, dlt uses its own cache in its extract normalize load pipelines. Having a more accessible cache of raw data would be useful for faster backfills and reduced API calls.
