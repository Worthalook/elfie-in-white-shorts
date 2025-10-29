# src/white_shorts/cli/update_history.py
from __future__ import annotations
import os
import json
import requests
import pandas as pd
import duckdb
import typer
from ..config import settings

app = typer.Typer(help="Update current-season history (actuals) into DuckDB")

# --- Adjust these if your API fields differ ---
_FIELD_MAP = {
    "Date": "date",
    "GameID": "game_id",
    "Team": "team",
    "Opponent": "opponent",
    "PlayerID": "player_id",
    "Name": "name",
    "Points": "points",
    "Goals": "goals",
    "Assists": "assists",
    "ShotsOnGoal": "shots_on_goal",
}

_REQUIRED = ["date", "game_id", "team", "opponent", "player_id", "name",
             "points", "goals", "assists", "shots_on_goal"]

def _fetch_actuals_json(date_str: str) -> list[dict]:
    base = os.getenv("SPORTS_DATA_BASE", "https://api.sportsdata.io")
    key = os.getenv("SPORTS_DATA_API_KEY", "")
    # Use your actual stats endpoint (not projections)
    url = f"{base}/api/nhl/fantasy/json/PlayerGameStatsByDate/{date_str}?key={key}"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    data = r.json()
    if isinstance(data, dict):
        # API sometimes wraps results; make it list-like
        data = data.get("results") or data.get("data") or []
    return data or []

def _normalize_frame(raw: list[dict]) -> pd.DataFrame:
    if not raw:
        return pd.DataFrame(columns=_REQUIRED)
    df = pd.DataFrame(raw)

    # Rename known fields
    for src, dst in _FIELD_MAP.items():
        if src in df.columns and dst not in df.columns:
            df = df.rename(columns={src: dst})

    # Ensure required columns exist
    for col in _REQUIRED:
        if col not in df.columns:
            df[col] = None

    # Type coercions
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    for c in ("game_id", "player_id"):
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
    for c in ("team", "opponent", "name"):
        df[c] = df[c].astype(str).fillna("").str.strip()

    # Targets as numeric
    for c in ("points", "goals", "assists", "shots_on_goal"):
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(float)

    # Keep only required
    df = df[_REQUIRED].dropna(subset=["date", "game_id", "team", "opponent", "player_id"])
    return df

def _to_long(df: pd.DataFrame) -> pd.DataFrame:
    # Convert wide → long with canonical 'target' + 'actual'
    long = df.melt(
        id_vars=["date","game_id","team","opponent","player_id","name"],
        value_vars=["points","goals","assists","shots_on_goal"],
        var_name="target",
        value_name="actual",
    )
    # Basic sanity
    long = long.dropna(subset=["date","game_id","team","opponent","player_id","target"])
    return long

@app.command()
def main(date: str):
    """Upsert actuals for a single YYYY-MM-DD date into DuckDB.fact_actuals."""
    # Accept both Y-m-d and d/m/Y
    d = pd.to_datetime(date, dayfirst=True, errors="coerce")
    if pd.isna(d):
        raise typer.BadParameter(f"Unparseable date: {date}")
    date_str = d.strftime("%Y-%m-%d")

    raw = _fetch_actuals_json(date_str)
    df = _normalize_frame(raw)
    long = _to_long(df)

    con = duckdb.connect(settings.DUCKDB_PATH)
    try:
        # Create table if not exists
        con.execute("""
            CREATE TABLE IF NOT EXISTS fact_actuals (
              date DATE,
              game_id BIGINT,
              team VARCHAR,
              opponent VARCHAR,
              player_id BIGINT,
              name VARCHAR,
              target VARCHAR,
              actual DOUBLE
            )
        """)

        # Upsert semantics: delete duplicates for (date,game_id,team,opponent,player_id,target) then insert
        con.execute("""
            DELETE FROM fact_actuals USING long
            WHERE fact_actuals.date = long.date
              AND fact_actuals.game_id = long.game_id
              AND fact_actuals.team = long.team
              AND fact_actuals.opponent = long.opponent
              AND fact_actuals.player_id = long.player_id
              AND fact_actuals.target = long.target
        """)  # 'long' is auto-registered when we pass pandas df in the next line
        con.execute("INSERT INTO fact_actuals SELECT * FROM long", {"long": long})
    finally:
        con.close()

    typer.echo(f"Upserted actuals for {date_str}: {len(long)} rows")
