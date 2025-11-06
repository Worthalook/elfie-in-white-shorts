from __future__ import annotations
import os, typer
import pandas as pd

try:
    import requests
except Exception:
    requests = None

def _parse_date(date_str: str) -> pd.Timestamp:
    try:
        d = pd.to_datetime(date_str, format='%Y-%m-%d')
        typer.echo(f"PARSE DATE: {d}")
    except Exception:
        try:
            d = pd.to_datetime(date_str, format='%yyyy-mm-d')
        except Exception:
            try:
                d = pd.to_datetime(date_str, format='%yyyy-m-dd')
            except Exception:
                d = pd.to_datetime(date_str, format='%yyyy-m-d')
                
        
    if pd.isna(d):
        raise ValueError(f"Unparseable date: {date_str}")
    return d#.normalize()

def _sportsdata_token(d: pd.Timestamp) -> str:
    mon = d.strftime("%b")
    typer.echo(f"API Token DATE: {d}")
    token = f"{d.year}-{mon}-{d.day:02d}"
    typer.echo(f"API Token: {token}.")
    return token

def fetch_projections_by_date(date_str: str) -> pd.DataFrame:
    """Authoritative slate from SportsData.io PlayerGameProjectionStatsByDate.
    Returns columns: ['date','game_id','team','opponent','player_id','name']

    Requires env: SPORTS_DATA_API_KEY
    Optional: SPORTS_DATA_BASE
    """
    if requests is None:
        raise RuntimeError("requests not installed; cannot fetch projections")

    api_key = os.getenv("SPORTS_DATA_API_KEY")
    if not api_key:
        raise RuntimeError("SPORTS_DATA_API_KEY is not set")

    base = os.getenv("SPORTS_DATA_BASE", "https://api.sportsdata.io")
    d = _parse_date(date_str)
    token = _sportsdata_token(d)
    url = f"{base}/api/nhl/fantasy/json/PlayerGameProjectionStatsByDate/{token}?key={api_key}"

    r = requests.get(url, timeout=30)
    r.raise_for_status()
    js = r.json()

    rows = []
    for rec in js:
        game_id = rec.get("GameID") or rec.get("GameId")
        team = rec.get("Team") or rec.get("TeamAbbreviation")
        opp  = rec.get("Opponent") or rec.get("OpponentAbbreviation")
        pid  = rec.get("PlayerID") or rec.get("PlayerId")
        name = rec.get("Name") or rec.get("ShortName") or rec.get("PlayerName")
        rows.append({
            "date": d,
            "game_id": pd.to_numeric(game_id, errors="coerce"),
            "team": None if team is None else str(team),
            "opponent": None if opp is None else str(opp),
            "player_id": None if pid is None else str(pid),
            "name": name,
        })
    df = pd.DataFrame(rows)
    df = df.dropna(subset=["player_id","team"]).reset_index(drop=True)
    return df
