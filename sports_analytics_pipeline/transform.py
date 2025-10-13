from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import json
import pandas as pd

from sports_analytics_pipeline.ingest import _parse_scoreboard_json, _parse_players_from_summary


def schedule_from_scoreboard(payload: Dict[str, Any]) -> pd.DataFrame:
	"""Convert a scoreboard JSON payload to a schedule DataFrame.

	The function delegates to the ingest module's JSON parser and returns a DataFrame
	with columns: date, start_time, timestamp_utc, home_team, away_team, venue, event_id.
	"""
	rows = _parse_scoreboard_json(payload)
	df = pd.DataFrame(rows)
	cols = ["date", "start_time", "timestamp_utc", "home_team", "away_team", "venue", "event_id"]
	for c in cols:
		if c not in df.columns:
			df[c] = None
	return df[cols]


def players_from_summary(summary: Dict[str, Any], espn_event_id: Optional[str] = None) -> pd.DataFrame:
	"""Convert a summary payload (boxscore) into a player box score DataFrame.

	The DataFrame columns match the `player_box_score` table schema.
	"""
	parsed = _parse_players_from_summary(summary)
	if not parsed:
		return pd.DataFrame()
	df = pd.DataFrame(parsed)

	# Ensure standard columns exist
	expected = [
		"first_name",
		"last_name",
		"team",
		"minutes_played",
		"points",
		"rebounds",
		"assists",
		"fouls",
		"plus_minus",
		"stats_json",
	]
	for c in expected:
		if c not in df.columns:
			df[c] = None

	# Attach espn_event_id and normalize date/timestamp from header if present
	ev = espn_event_id
	header = summary.get("header") or {}
	date_str = header.get("competitions") and header["competitions"][0].get("date") if isinstance(header.get("competitions"), list) else None
	timestamp_utc = None
	if date_str:
		try:
			dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
			if dt.tzinfo is None:
				dt = dt.replace(tzinfo=timezone.utc)
			dt_utc = dt.astimezone(timezone.utc)
			df["date"] = dt_utc.date().isoformat()
			timestamp_utc = dt_utc.isoformat()
		except Exception:
			df["date"] = date_str
			timestamp_utc = None
	else:
		df["date"] = None

	# teams from competitions
	comps = summary.get("competitions") or []
	away = None
	home = None
	if comps:
		comp = comps[0]
		for comp_team in comp.get("competitors") or []:
			if comp_team.get("homeAway") == "home":
				home = (comp_team.get("team") or {}).get("displayName")
			elif comp_team.get("homeAway") == "away":
				away = (comp_team.get("team") or {}).get("displayName")

	df["away_team"] = away
	df["home_team"] = home
	df["espn_event_id"] = ev
	df["timestamp_utc"] = timestamp_utc
	return df


def team_box_from_summary(box: Dict[str, Any], espn_event_id: str, *, event_date: Optional[str] = None, away_team: Optional[str] = None, home_team: Optional[str] = None) -> pd.DataFrame:
	"""Convert a raw box dict or summary into a team-level DataFrame matching `box_score` schema.

	The function extracts common shapes (box['teams'] or box['teamStats']). For unknown
	shapes it returns a single row with the raw JSON in `stats_json`.
	"""
	rows: List[Dict[str, Any]] = []
	teams = box.get("teams") or box.get("teamStats") or []
	if isinstance(teams, list) and teams:
		for t in teams:
			team_name = t.get("team") or t.get("teamName") or (t.get("team", {}) or {}).get("displayName")
			stats = t.get("statistics") or t.get("stats") or {}
			points = None
			rebounds = None
			assists = None
			fouls = None
			plus_minus = None
			if isinstance(stats, dict):
				points = stats.get("points")
				rebounds = stats.get("rebounds")
				assists = stats.get("assists")
				fouls = stats.get("fouls")
			rows.append(
				{
					"espn_event_id": espn_event_id,
					"date": event_date,
					"away_team": away_team,
					"home_team": home_team,
					"team": team_name,
					"points": points,
					"rebounds": rebounds,
					"assists": assists,
					"fouls": fouls,
					"plus_minus": plus_minus,
					"stats_json": json.dumps(t),
				}
			)
	else:
		rows.append(
			{
				"espn_event_id": espn_event_id,
				"date": event_date,
				"away_team": away_team,
				"home_team": home_team,
				"team": None,
				"points": None,
				"rebounds": None,
				"assists": None,
				"fouls": None,
				"plus_minus": None,
				"stats_json": json.dumps(box),
			}
		)

	return pd.DataFrame(rows)

