from __future__ import annotations
import os, requests, pandas as pd, duckdb, typer, json
from ..config import settings

app = typer.Typer(help="Update current-season history (actuals) into DuckDB")

# SportsData.IO → canonical
FIELD_MAP = {
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

REQUIRED = ["date","game_id","team","opponent","player_id","name","points","goals","assists","shots_on_goal"]

def _fetch_actuals(date_str_iso: str) -> list[dict]:
    """Fetch ACTUALS for a date; try 'YYYY-Mon-DD' then 'YYYY-MM-DD'."""
    base = os.getenv("SPORTS_DATA_BASE", "https://api.sportsdata.io")
    key  = os.getenv("SPORTS_DATA_API_KEY", "")
    if not key:
        raise RuntimeError("SPORTS_DATA_API_KEY missing!")

    d = pd.to_datetime(date_str_iso, dayfirst=True, errors="coerce")
    if pd.isna(d):
        raise typer.BadParameter(f"Unparseable date: {date_str_iso}")

    # SportsData NHL often expects 'YYYY-Mon-DD' (e.g., 2025-Oct-07)
    candidates = [d.strftime("%Y-%b-%d"), d.strftime("%Y-%m-%d")]
    for ds in candidates:
        url = f"{base}/api/nhl/fantasy/json/PlayerGameStatsByDate/{ds}?key={key}"
        r = requests.get(url, timeout=30)
        if r.status_code == 200:
            try:
                data = r.json()
                if isinstance(data, dict):
                    data = data.get("results") or data.get("data") or []
                return data or []
            except Exception:
                pass
    return []

def _normalize(raw: list[dict]) -> pd.DataFrame:
    if not raw:
        return pd.DataFrame(columns=REQUIRED)
    df = pd.DataFrame(raw)

    # rename
    for src, dst in FIELD_MAP.items():
        if src in df.columns and dst not in df.columns:
            df = df.rename(columns={src: dst})

    # ensure required cols exist
    for c in REQUIRED:
        if c not in df.columns:
            df[c] = None

    # types
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    for c in ("game_id","player_id"):
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
    for c in ("team","opponent","name"):
        df[c] = df[c].astype(str).str.strip()
    for c in ("points","goals","assists","shots_on_goal"):
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(float)

    # drop rows missing identifiers
    df = df[REQUIRED].dropna(subset=["date","game_id","team","opponent","player_id"])
    return df

def _to_long(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["date","game_id","team","opponent","player_id","name","target","actual"])
    long = df.melt(
        id_vars=["date","game_id","team","opponent","player_id","name"],
        value_vars=["points","goals","assists","shots_on_goal"],
        var_name="target",
        value_name="actual",
    )
    # normalize team/opponent for stable joins (upper/trim)
    long["team"] = long["team"].str.upper().str.strip()
    long["opponent"] = long["opponent"].str.upper().str.strip()
    return long

@app.command()
def main(date: str):
    """Upsert actuals for a single date (accepts YYYY-MM-DD or DD/MM/YYYY)."""
    d = pd.to_datetime(date, dayfirst=True, errors="coerce")
    typer.echo(f"update_history DATE: {d}")
    if pd.isna(d):
        raise typer.BadParameter(f"Unparseable date: {date}")
    iso = d.strftime("%Y-%m-%d")

    raw = _fetch_actuals(iso)
    print(f"Fetched {len(raw)} rows from API for {iso}")
    df = _normalize(raw)
    print(f"Normalized rows: {len(df)}")
    if not df.empty:
        print("Sample:", df.head(3).to_dict(orient="records"))

    long = _to_long(df)
    print(f"Long rows (targets expanded): {len(long)}")

    con = duckdb.connect(settings.DUCKDB_PATH)
    try:
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
        # Upsert by replacing existing rows for the (date, keys, target)
        con.execute("""
            DELETE FROM fact_actuals
            WHERE date = ?
        """, [pd.Timestamp(iso).date()])
        typer.echo(f"update_history CLOSING: {pd.Timestamp(iso).date()}")
        if not long.empty:
            typer.echo(f"update_history PRE_DB_Execute")
            con.execute("INSERT INTO fact_actuals SELECT * FROM long", {"long": long})
            typer.echo(f"update_history POST_DB_Execute")
            # ensure physical write
            con.execute("CHECKPOINT")
            typer.echo(f"update_history POST_DB_CheckPoint")
    finally:
        typer.echo(f"update_history CLOSING")
        con.close()

    print(f"Upserted actuals for {iso}: {len(long)} rows")
