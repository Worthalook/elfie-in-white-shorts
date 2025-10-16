from __future__ import annotations
import os
import pandas as pd

try:
    import requests
except Exception:
    requests = None

CURRENT_SEASON_PATH = os.getenv("WS_CURRENT_SEASON_PARQUET", "data/current_season.parquet")

def _parse_date(date_str: str) -> pd.Timestamp:
    return pd.to_datetime(date_str, dayfirst=True, errors="coerce").normalize()

def _sportsdata_token(d: pd.Timestamp) -> str:
    mon = d.strftime("%b")
    return f"{d.year}-{mon}-{d.day:02d}"

def fetch_player_stats_by_date(date_str: str) -> pd.DataFrame:
    """Fetch PlayerGameStatsByDate (actuals)."""
    if requests is None:
        raise RuntimeError("requests not installed; cannot fetch actuals")

    api_key = os.getenv("SPORTS_DATA_API_KEY")
    if not api_key:
        raise RuntimeError("SPORTS_DATA_API_KEY is not set")

    base = os.getenv("SPORTS_DATA_BASE", "https://api.sportsdata.io")
    d = _parse_date(date_str)
    token = _sportsdata_token(d)
    url = f"{base}/api/nhl/fantasy/json/PlayerGameStatsByDate/{token}?key={api_key}"

    r = requests.get(url, timeout=30)
    r.raise_for_status()
    js = r.json()

    rows = []
    for rec in js:
        rows.append({
            "date": d,
            "game_id": pd.to_numeric(rec.get("GameID") or rec.get("GameId"), errors="coerce"),
            "team": rec.get("Team") or rec.get("TeamAbbreviation"),
            "opponent": rec.get("Opponent") or rec.get("OpponentAbbreviation"),
            "player_id": str(rec.get("PlayerID") or rec.get("PlayerId")) if rec.get("PlayerID") or rec.get("PlayerId") else None,
            "name": rec.get("Name") or rec.get("ShortName") or rec.get("PlayerName"),
            "minutes": rec.get("Minutes"),
            "points": rec.get("FantasyPoints") or rec.get("Points"),
            "goals": rec.get("Goals"),
            "assists": rec.get("Assists"),
            "shots_on_goal": rec.get("ShotsOnGoal") or rec.get("Shots"),
            "home_or_away": 1 if rec.get("HomeOrAway") in ("HOME","Home","H") else 0,
            "power_play_assists": rec.get("PowerPlayAssists"),
            "power_play_goals": rec.get("PowerPlayGoals"),
            "goal_tending_goals_against": rec.get("GoalsAgainst"),
        })
    df = pd.DataFrame(rows).dropna(subset=["player_id","team"])
    df["player_id"] = df["player_id"].astype(str)
    df["team"] = df["team"].astype(str)
    df["opponent"] = df["opponent"].astype(str)
    return df

def upsert_current_season(df_new: pd.DataFrame, path: str = CURRENT_SEASON_PATH) -> str:
    if os.path.exists(path):
        try:
            df_cur = pd.read_parquet(path)
        except Exception:
            df_cur = pd.DataFrame()
    else:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        df_cur = pd.DataFrame()

    key_cols = ["date","game_id","player_id"]
    for c in key_cols:
        if c == "date":
            df_new[c] = pd.to_datetime(df_new[c], errors="coerce").dt.normalize()
        elif c == "game_id":
            df_new[c] = pd.to_numeric(df_new[c], errors="coerce")
        else:
            df_new[c] = df_new[c].astype(str)

    if not df_cur.empty:
        for c in key_cols:
            if c == "date":
                df_cur[c] = pd.to_datetime(df_cur[c], errors="coerce").dt.normalize()
            elif c == "game_id":
                df_cur[c] = pd.to_numeric(df_cur[c], errors="coerce")
            else:
                df_cur[c] = df_cur[c].astype(str)

        idx = pd.MultiIndex.from_frame(df_new[key_cols])
        mask = ~pd.MultiIndex.from_frame(df_cur[key_cols]).isin(idx)
        df_cur = df_cur[mask]

        df_out = pd.concat([df_cur, df_new], ignore_index=True)
    else:
        df_out = df_new.copy()

    df_out.to_parquet(path, index=False)
    return path

def load_current_season(path: str = CURRENT_SEASON_PATH) -> pd.DataFrame:
    if not os.path.exists(path):
        return pd.DataFrame()
    return pd.read_parquet(path)
