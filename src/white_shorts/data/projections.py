from __future__ import annotations
import os
import datetime as dt
from typing import Optional, Tuple
import pandas as pd

try:
    import requests
except Exception as e:
    requests = None  # allow import even if requests isn't installed

def _fmt_sportsdata_date(date_str: str) -> str:
    # Expect 'YYYY-MM-DD' or similar; convert to 'YYYY-Mon-DD' (e.g., 2025-Oct-07)
    d = pd.to_datetime(date_str, errors="coerce")
    if pd.isna(d):
        # fall back to today
        d = pd.Timestamp.today()
    mon = d.strftime("%b")  # Oct
    return f"{d.year}-{mon}-{d.day:02d}"

def fetch_projections_by_date(date_str: str) -> pd.DataFrame:
    """Fetch active slate (players and games) from SportsData.io for a given date.

    Environment:
      - SPORTS_DATA_API_KEY: API key (required)
      - SPORTS_DATA_BASE: base URL (optional, defaults to 'https://api.sportsdata.io')

    Returns a DataFrame with at least:
      ['date','game_id','team','opponent','player_id','name']
    """
    api_key = os.getenv("SPORTS_DATA_API_KEY")
    if not api_key:
        raise RuntimeError("SPORTS_DATA_API_KEY is not set in environment")

    base = os.getenv("SPORTS_DATA_BASE", "https://api.sportsdata.io")
    dtoken = _fmt_sportsdata_date(date_str)

    # Endpoint from your earlier example (fantasy PlayerGameStatsByDate)
    url = f"{base}/api/nhl/fantasy/json/PlayerGameStatsByDate/{dtoken}?key={api_key}"
    if requests is None:
        raise RuntimeError("requests module not available to fetch projections")

    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    js = resp.json()

    # Normalize
    rows = []
    for r in js:
        # Defensive gets, with common SportsData fields
        game_id = r.get("GameID") or r.get("GameId") or r.get("GameId")
        team = r.get("Team") or r.get("TeamID") or r.get("TeamAbbreviation")
        opp = r.get("Opponent") or r.get("OpponentAbbreviation")
        pid = r.get("PlayerID") or r.get("PlayerId")
        name = r.get("Name") or r.get("PlayerName") or r.get("ShortName")
        rows.append({
            "date": pd.to_datetime(date_str),
            "game_id": game_id,
            "team": team,
            "opponent": opp,
            "player_id": pid,
            "name": name
        })
    df = pd.DataFrame(rows).dropna(subset=["player_id", "team"])
    # Coerce types to be merge-friendly with your YTD
    if "game_id" in df.columns:
        df["game_id"] = pd.to_numeric(df["game_id"], errors="coerce")
    df["player_id"] = df["player_id"].astype(str)
    df["team"] = df["team"].astype(str)
    if "opponent" in df.columns:
        df["opponent"] = df["opponent"].astype(str)
    return df
