from __future__ import annotations
import os
from sys import api_version
import pandas as pd
import requests
import typer


API_KEY = os.getenv("SPORTSDATA_API_KEY", "")

def _fmt_sportsdata_date(date_str: str) -> tuple[str, pd.Timestamp]:
    import pandas as pd
    typer.echo(f"raw date - pre API prep: {date_str}")
    d = pd.to_datetime(date_str, dayfirst=True, errors="coerce")
    if pd.isna(d):
        raise ValueError(f"Unparseable date: {date_str}")
    # SportsData wants e.g. 2025-Oct-15
    mon = d.strftime("%b")  # Oct
    typer.echo(f"formated month (for API) part: {mon}")
    api_token = f"{d.year}-{mon}-{d.day:02d}"
    typer.echo(f"formated final date string for API: {api_token} and normalised: {d.normalize()}")
    return api_token, d.normalize()



def _get(url: str) -> list[dict]:
    headers = {"Ocp-Apim-Subscription-Key": API_KEY} if API_KEY else {}
    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    return r.json()

def fetch_player_projections_by_date(date_str: str) -> pd.DataFrame:
    api_token, slate_dt = _fmt_sportsdata_date(date_str)
    url = f"{base}/api/nhl/fantasy/json/PlayerGameStatsByDate/{api_token}?key={api_key}"
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
            "date": f"{slate_dt.year}-{slate_dt.month}-{slate_dt.day:02d}",  # ← force slate date here too
            "game_id": game_id,
            "team": team,
            "opponent": opp,
            "player_id": pid,
            "name": name
        })


    return pd.DataFrame(rows).drop_duplicates()

def naive_projections_from_recent(recent_df: pd.DataFrame) -> pd.DataFrame:
    if recent_df.empty:
        return pd.DataFrame(columns=["date","game_id","team","opponent","player_id","name"])
    key = ["date","game_id","team","opponent","player_id","name"]
    return recent_df[key].drop_duplicates().reset_index(drop=True)

