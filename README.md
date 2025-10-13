BBALL Scraper

This project scrapes data about the NBA from ESPN.com. It is the firt part of a larger project to use basketball for creating data science and data engineering portolio. 

This is the data ingestion section - using python to scrape data on games, schedules, players, teams, etc. 

This data will then be stored in a duckdb database for further analysis.

## Development

This project uses `uv` to manage dependencies and a lightweight dev toolchain:

- ruff: linter and formatter (fast, replaces flake8 + isort + some auto-fixes)
- mypy: static type checker

Install dev tools into the virtual environment:

```bash
cd bball-season
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
python -m mypy bball_season tests
```

Follow the coding conventions in `.copilot-instructions.md` (docstrings, type hints, pytest-style tests).

## Data model

A minimal description of the DuckDB data model is available in `docs/data-model.md`.

## Analysis
Ad-hoc analysis on the scraped data can be done using your tool of choice - here we use the DuckDB ui

