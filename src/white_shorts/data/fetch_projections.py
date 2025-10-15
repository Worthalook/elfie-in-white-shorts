from __future__ import annotations
import os
import pandas as pd
import requests

API_KEY = os.getenv("SPORTSDATA_API_KEY", "")

def _get(url: str) -> list[dict]:
    headers = {"Ocp-Apim-Subscription-Key": API_KEY} if API_KEY else {}
    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    return r.json()

def fetch_player_projections_by_date(date_str: str) -> pd.DataFrame:
    if not API_KEY:
        return pd.DataFrame(columns=["date","game_id","team","opponent","player_id","name"])
    base = "https://api.sportsdata.io/api/nhl"
    urls = [
        f"{base}/fantasy/json/PlayerGameProjectionStatsByDate/{date_str}",
        f"{base}/fantasy/json/PlayerGameStatsByDate/{date_str}",
    ]
    data = None
    for u in urls:
        try:
            data = _get(u)
            if data is not None:
                break
        except Exception:
            continue
    if not data:
        return pd.DataFrame(columns=["date","game_id","team","opponent","player_id","name"])
    rows = []
    for d in data:
        rows.append({
            "date": date_str,
            "game_id": str(d.get("GameID", d.get("GameId", ""))),
            "team": str(d.get("Team", d.get("TeamAbbreviation", ""))).strip(),
            "opponent": str(d.get("Opponent", d.get("OpponentAbbreviation", ""))).strip(),
            "player_id": str(d.get("PlayerID", d.get("PlayerId", ""))),
            "name": str(d.get("Name", d.get("PlayerName", ""))).strip(),
        })

    return pd.DataFrame(rows).drop_duplicates()

def naive_projections_from_recent(recent_df: pd.DataFrame) -> pd.DataFrame:
    if recent_df.empty:
        return pd.DataFrame(columns=["date","game_id","team","opponent","player_id","name"])
    key = ["date","game_id","team","opponent","player_id","name"]
    return recent_df[key].drop_duplicates().reset_index(drop=True)
