# Data model (minimal)

This file lists the DuckDB tables and their columns used by the project. It's intentionally minimal — see `bball_season/storage.py` for the authoritative SQL and ingestion behaviour.

## tables

### teams
Primary key: name (TEXT)

Columns:
- name (TEXT)
- city (TEXT)

### venues
Primary key: name (TEXT)

Columns:
- name (TEXT)
- city (TEXT)
- state (TEXT)

### schedule
Primary key: composite (date, away_team, home_team)

Columns:
- espn_event_id (TEXT) — external id from ESPN (may be NULL)
- date (DATE)
- start_time (TEXT)
- away_team (TEXT) (FK) — references teams(name)
- home_team (TEXT) (FK) — references teams(name)
- venue (TEXT) (FK) — references venues(name)
- status (TEXT)
- home_score (INTEGER)
- away_score (INTEGER)
- created_at (TIMESTAMP)
- game_type (TEXT) — one of: `pre-season`, `regular season`, `playoffs`, `in-season tournament`

### box_score
Primary key: composite (date, away_team, home_team)

Columns (team-level box score):
- espn_event_id (TEXT) (FK) — external id from ESPN (may be NULL)
- date (DATE)
- away_team (TEXT) (FK) — references teams(name)
- home_team (TEXT) (FK) — references teams(name)
- team (TEXT) (FK) — which team this row summarizes (references teams(name))
- points (INTEGER)
- rebounds (INTEGER)
- assists (INTEGER)
- stats_json (TEXT) — raw JSON blob for other metrics
- fouls (INTEGER)
- plus_minus (INTEGER)

### player_box_score
Primary key: composite (date, away_team, home_team, first_name, last_name)

Columns (player-level box score):
- espn_event_id (TEXT) (FK)
- date (DATE)
- away_team (TEXT) (FK) — references teams(name)
- home_team (TEXT) (FK) — references teams(name)
- first_name (TEXT)
- last_name (TEXT)
- team (TEXT) (FK) — player's team for the game (references teams(name))
- minutes_played (TEXT)
- points (INTEGER)
- rebounds (INTEGER)
- assists (INTEGER)
- stats_json (TEXT) — raw JSON blob for additional stats
- fouls (INTEGER)
- plus_minus (INTEGER)

### players
Primary key: (first_name, last_name, date_of_birth)

Columns:
- first_name (TEXT)
- last_name (TEXT)
- date_of_birth (DATE)
- season (TEXT)
- country_of_birth (TEXT)
