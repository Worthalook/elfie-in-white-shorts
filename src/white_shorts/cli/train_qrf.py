# src/white_shorts/cli/train_qrf.py
from __future__ import annotations
import os
from typing import Optional, Iterable

import typer
import pandas as pd
import duckdb
import requests

from ..config import settings
from ..data.load_ytd import load_ytd
from ..features.engineer import engineer_minimal
from ..features.registry import PLAYER_FEATURES
from ..modeling.trainers_qrf import train_player_qrf
from ..modeling.io_qrf import save_qrf

app = typer.Typer(help="Train Quantile Random Forest models for player targets")

# ----------------------------
# Utilities: current-season actuals
# ----------------------------

_TARGETS = ("points", "goals", "assists", "shots_on_goal")

_FIELD_MAP = {
    # SportsData.io -> internal
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
    "Minutes" : "minutes"
}

_REQUIRED_BASE = ["date", "game_id", "team", "opponent", "player_id", "name", "minutes"]
_REQUIRED_WIDE = list(_REQUIRED_BASE) + list(_TARGETS)


def _normalize_wide(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize API frame to the same 'wide' schema as YTD CSV."""
    if df is None or df.empty:
        return pd.DataFrame(columns=_REQUIRED_WIDE)

    # rename incoming columns if needed
    for src, dst in _FIELD_MAP.items():
        if src in df.columns and dst not in df.columns:
            df = df.rename(columns={src: dst})

    # ensure required columns
    for c in _REQUIRED_WIDE:
        if c not in df.columns:
            df[c] = None

    # types & cleaning
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    for c in ("game_id", "player_id"):
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")

    for c in ("team", "opponent", "name"):
        df[c] = df[c].astype(str).str.strip()

    for c in _TARGETS:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0).astype(float)

    # normalize team/opponent to abbrev-caps if you want stricter joins later
    df["team"] = df["team"].str.upper().str.strip()
    df["opponent"] = df["opponent"].str.upper().str.strip()

    # drop rows missing identifiers
    df = df.dropna(subset=["date", "game_id", "team", "opponent", "player_id"])
    # keep only the columns we care about
    return df[_REQUIRED_WIDE].copy()


def _pivot_long_to_wide(long_df: pd.DataFrame) -> pd.DataFrame:
    """If your DuckDB stores fact_actuals in long format (target, actual),
    pivot to the wide player row schema."""
    if long_df is None or long_df.empty:
        return pd.DataFrame(columns=_REQUIRED_WIDE)

    base_cols = ["date", "game_id", "team", "opponent", "player_id", "name"]
    if not {"target", "actual"}.issubset(long_df.columns):
        # Already wide (or nothing to do)
        expected = set(base_cols + list(_TARGETS))
        cols = [c for c in long_df.columns if c in expected]
        return long_df[cols].copy()

    wide = (
        long_df
        .pivot_table(
            index=base_cols,
            columns="target",
            values="actual",
            aggfunc="max",  # safe if duplicates
        )
        .reset_index()
        .rename_axis(None, axis=1)
    )

    # Ensure all target columns exist
    for t in _TARGETS:
        if t not in wide.columns:
            wide[t] = 0.0

    # ordering
    wide = wide[base_cols + list(_TARGETS)].copy()
    # enforce types similar to _normalize_wide
    wide["date"] = pd.to_datetime(wide["date"], errors="coerce").dt.date
    for c in ("game_id", "player_id"):
        wide[c] = pd.to_numeric(wide[c], errors="coerce").astype("Int64")
    for t in _TARGETS:
        wide[t] = pd.to_numeric(wide[t], errors="coerce").fillna(0.0).astype(float)
    wide["team"] = wide["team"].astype(str).str.upper().str.strip()
    wide["opponent"] = wide["opponent"].astype(str).str.upper().str.strip()
    return wide


def _load_current_from_duckdb(days: Optional[int] = 90) -> pd.DataFrame:
    """Load current-season actuals from DuckDB (fact_actuals)."""
    path = os.getenv("DUCKDB_PATH", settings.DUCKDB_PATH)
    if not os.path.exists(path):
        return pd.DataFrame(columns=_REQUIRED_WIDE)
    con = duckdb.connect(path)
    try:
        exists = con.execute(
            "SELECT COUNT(*)>0 FROM information_schema.tables WHERE table_name = 'fact_actuals'"
        ).fetchone()[0]
        if not exists:
            return pd.DataFrame(columns=_REQUIRED_WIDE)

        where = ""
        if days and days > 0:
            where = f"WHERE date >= (CURRENT_DATE - INTERVAL {int(days)} DAY)"

        # expect long format in fact_actuals
        q = f"""
            SELECT
              CAST(date AS DATE)        AS date,
              CAST(game_id AS BIGINT)   AS game_id,
              UPPER(TRIM(team))         AS team,
              UPPER(TRIM(opponent))     AS opponent,
              CAST(player_id AS BIGINT) AS player_id,
              COALESCE(name,'')         AS name,
              target,
              CAST(actual AS DOUBLE)    AS actual,
              15 AS minutes
            FROM fact_actuals
            {where}
        """
        long_df = con.execute(q).fetchdf()
    finally:
        con.close()

    return _pivot_long_to_wide(long_df)


def _fetch_actuals_for_dates(dates: Iterable[pd.Timestamp | str]) -> pd.DataFrame:
    """Pull actuals from SportsData.io for a date list; return wide schema."""
    base = os.getenv("SPORTS_DATA_BASE", "https://api.sportsdata.io")
    key = os.getenv("SPORTS_DATA_API_KEY", "")
    if not key:
        # no key → nothing fetched
        return pd.DataFrame(columns=_REQUIRED_WIDE)

    frames: list[pd.DataFrame] = []
    for d in dates:
        ts = pd.to_datetime(d, dayfirst=True, errors="coerce")
        if pd.isna(ts):
            continue
        # try both formats; keep first non-empty
        for fmt in ("%Y-%b-%d", "%Y-%m-%d"):
            ds = ts.strftime(fmt)
            url = f"{base}/api/nhl/fantasy/json/PlayerGameStatsByDate/{ds}?key={key}"
            try:
                r = requests.get(url, timeout=25)
                if r.status_code != 200:
                    continue
                data = r.json()
                if not isinstance(data, list) or len(data) == 0:
                    continue
                frames.append(_normalize_wide(pd.DataFrame(data)))
                break  # got data for this date; next date
            except Exception:
                continue

    if not frames:
        return pd.DataFrame(columns=_REQUIRED_WIDE)
    return pd.concat(frames, ignore_index=True)


def _assemble_training_frame(
    ytd_csv: str,
    use_duckdb_days: int = 120,
    api_backfill_days: int = 30,
) -> pd.DataFrame:
    """
    Build training frame = YTD (2023/24) + current-season actuals.
    Priority:
      1) Pull from DuckDB (last N days) if exists.
      2) If DuckDB missing/empty, fetch from API for last M days.
    """
    # 1) Base: prior season CSV
    df_ytd = load_ytd(ytd_csv)  # already returns wide with targets
    # Normalize a bit (just in case)
    df_ytd["team"] = df_ytd["team"].astype(str).str.upper().str.strip()
    df_ytd["opponent"] = df_ytd["opponent"].astype(str).str.upper().str.strip()

    # 2) Current: from DuckDB or API
    cur = _load_current_from_duckdb(days=use_duckdb_days)
    print(f"Db Records used for Training: {len(cur)}")
    if cur.empty and api_backfill_days and api_backfill_days > 0:
        # fetch from API for last api_backfill_days (skip today)
        today = pd.Timestamp.utcnow().normalize()
        days = [today - pd.Timedelta(days=i) for i in range(1, api_backfill_days + 1)]
        cur = _fetch_actuals_for_dates(days)

    # 3) Combine
    if cur.empty:
        combined = df_ytd
    else:
        # Make columns consistent and concat
        keep_cols = [c for c in _REQUIRED_WIDE if c in df_ytd.columns or c in cur.columns]
        combined = pd.concat(
            [df_ytd[keep_cols], cur[keep_cols]],
            ignore_index=True,
        ).drop_duplicates(subset=["date", "game_id", "player_id", "team", "opponent"])

    return combined


# ----------------------------
# Training
# ----------------------------

def _train_one(df_feat: pd.DataFrame, target: str, version: str = "0.3.0") -> str:
    bundle = train_player_qrf(df_feat, PLAYER_FEATURES, target=target, version=version)
    path = save_qrf(bundle)
    return path


@app.command()
def all(
    ytd_csv: Optional[str] = typer.Option(None, help="Path to YTD CSV (defaults to env WS_YTD_CSV or data/NHL_2023_24.csv)"),
    version: str = typer.Option("0.3.0", help="Model version tag to embed in filenames"),
    use_duckdb_days: int = typer.Option(120, help="Pull recent actuals from DuckDB for this many days if present"),
    api_backfill_days: int = typer.Option(30, help="If DuckDB empty, fetch this many days from API"),
):
    """
    Train QRF for points, goals, assists, shots_on_goal using:
      YTD CSV (prior season) + current-season actuals from DuckDB (preferred) or API fallback.
    """
    csv_path = ytd_csv or os.getenv("WS_YTD_CSV", "data/NHL_2023_24.csv")

    # Build the raw training table (wide)
    df_raw = _assemble_training_frame(
        csv_path,
        use_duckdb_days=use_duckdb_days,
        api_backfill_days=api_backfill_days,
    )

    if df_raw.empty:
        raise typer.Exit(code=1)

    # Feature engineering (your existing minimal pipeline)
    df_feat = engineer_minimal(df_raw)

    results = {}
    for tgt in _TARGETS:
        path = _train_one(df_feat, tgt, version=version)
        results[tgt] = path
        typer.echo(f"Trained & saved QRF for {tgt} → {path}")
    return results


@app.command()
def target(
    name: str = typer.Argument(..., help="One of: points, goals, assists, shots_on_goal"),
    ytd_csv: Optional[str] = typer.Option(None, help="Path to YTD CSV (defaults to env WS_YTD_CSV or data/NHL_2023_24.csv)"),
    version: str = typer.Option("0.3.0", help="Model version tag to embed in filenames"),
    use_duckdb_days: int = typer.Option(120, help="Pull recent actuals from DuckDB for this many days if present"),
    api_backfill_days: int = typer.Option(30, help="If DuckDB empty, fetch this many days from API"),
):
    """Train QRF for a single target with the same combined data logic."""
    if name not in set(_TARGETS):
        raise typer.BadParameter("name must be one of: points, goals, assists, shots_on_goal")

    csv_path = ytd_csv or os.getenv("WS_YTD_CSV", "data/NHL_2023_24.csv")

    df_raw = _assemble_training_frame(
        csv_path,
        use_duckdb_days=use_duckdb_days,
        api_backfill_days=api_backfill_days,
    )

    if df_raw.empty:
        raise typer.Exit(code=1)

    df_feat = engineer_minimal(df_raw)
    path = _train_one(df_feat, name, version=version)
    typer.echo(f"Trained & saved QRF for {name} → {path}")


if __name__ == "__main__":
    app()
