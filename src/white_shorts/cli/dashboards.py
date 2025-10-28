from __future__ import annotations

import os
import datetime as dt
import pandas as pd
import duckdb
import typer

from ..config import settings
from ..modeling.evaluation import rmse, coverage

app = typer.Typer(help="Metrics extraction and dashboard artifact writer")

def _now_date_str() -> str:
    return dt.datetime.utcnow().strftime("%Y-%m-%d")

def _ensure_tables(con: duckdb.DuckDBPyConnection) -> dict[str, bool]:
    """Detect table existence; return map."""
    exists = {}
    for t in ("fact_predictions", "fact_actuals"):
        exists[t] = bool(con.execute(
            "SELECT COUNT(*)>0 FROM information_schema.tables WHERE table_name = ?",
            [t]
        ).fetchone()[0])
    return exists

def _eval_frame(con: duckdb.DuckDBPyConnection, days: int) -> pd.DataFrame:
    """Return a joined long frame with columns:
       [target, date, game_id, team, opponent, player_id, name, mu, q10, q90, actual, p_ge_k_json]
    """
    exists = _ensure_tables(con)

    # Base predictions (authoritative for target)
    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW preds_win AS
        SELECT
            target AS p_target,
            date, game_id, team, opponent, player_id, name,
            lambda_or_mu AS mu, q10, q90, p_ge_k_json
        FROM fact_predictions
        WHERE date >= (CURRENT_DATE - INTERVAL {int(days)} DAY)
    """)

    # Actuals source: prefer fact_actuals; otherwise derive from current_season.parquet
    if exists.get("fact_actuals", False):
        con.execute(f"""
    CREATE OR REPLACE TEMP VIEW acts_long AS
    SELECT
        target, date, game_id, team, opponent, player_id, name,
        actual
    FROM fact_actuals
    WHERE date >= (CURRENT_DATE - INTERVAL {int(days)} DAY)
""")
    else:
        # Derive long actuals from the parquet store
        # Adjust path/column names if your store differs.
        cur_path = os.getenv("WS_CURRENT_SEASON_PARQUET", "data/current_season.parquet")
        con.execute(f"""
            CREATE OR REPLACE TEMP VIEW cur_raw AS
            SELECT *
            FROM read_parquet('{cur_path}')
            WHERE date >= (CURRENT_DATE - INTERVAL {days} DAY)
        """)
        con.execute("""
            CREATE OR REPLACE TEMP VIEW acts_long AS
            SELECT 'points'         AS target, date, game_id, team, opponent, player_id, name, CAST(points         AS DOUBLE) AS actual FROM cur_raw
            UNION ALL
            SELECT 'goals'          AS target, date, game_id, team, opponent, player_id, name, CAST(goals          AS DOUBLE) AS actual FROM cur_raw
            UNION ALL
            SELECT 'assists'        AS target, date, game_id, team, opponent, player_id, name, CAST(assists        AS DOUBLE) AS actual FROM cur_raw
            UNION ALL
            SELECT 'shots_on_goal'  AS target, date, game_id, team, opponent, player_id, name, CAST(shots_on_goal  AS DOUBLE) AS actual FROM cur_raw
        """)

    # Join by identifiers + target
    q = """
        SELECT
            p.p_target AS target,
            p.date, p.game_id, p.team, p.opponent, p.player_id, p.name,
            p.mu, p.q10, p.q90, p.p_ge_k_json,
            a.actual
        FROM preds_win p
        LEFT JOIN acts_long a
          ON a.target = p.p_target
         AND a.date = p.date
         AND a.game_id = p.game_id
         AND a.team = p.team
         AND a.opponent = p.opponent
         AND a.player_id = p.player_id
         AND COALESCE(a.name,'') = COALESCE(p.name,'')
    """
    return con.execute(q).fetchdf()

@app.command()
def build(
    days: int = typer.Option(14, help="Rolling window (days) to evaluate"),
    out: str = typer.Option("data/dashboards", help="Output directory for artifacts"),
    echo_table: bool = typer.Option(True, help="Print summary table to stdout"),
) -> None:
    """Build rolling metrics, write CSV artifacts, and optionally echo a table."""
    os.makedirs(out, exist_ok=True)

    con = duckdb.connect(settings.DUCKDB_PATH)
    try:
        df = _eval_frame(con, days)
        # --- normalize/repair columns so 'target' is guaranteed to exist -----------
        # lower-case and strip any whitespace from all column names
        df.columns = [str(c).strip().lower() for c in df.columns]

        # if 'target' still missing, try common alternates then rename -> 'target'
        if "target" not in df.columns:
            for alt in ("p_target", "variable", "outcome", "label"):
                if alt in df.columns:
                    df = df.rename(columns={alt: "target"})
                    break

        # if still missing, print what we do have and stop gracefully
        if "target" not in df.columns:
          typer.echo(f"Joined frame is missing 'target'. Columns found: {list(df.columns)}")
          return

    finally:
        con.close()

    if df.empty or "target" not in df.columns:
        typer.echo("No data for window (or missing required columns).")
        return

    rows = []
    for t, g in df.groupby("target"):
        n_total = len(g)
        n_actual = int(g["actual"].notna().sum())
        if n_actual == 0:
            continue

        y   = g["actual"].fillna(0.0)
        mu  = g["mu"].fillna(0.0)
        q10 = g["q10"].fillna(0.0)
        q90 = g["q90"].fillna(0.0)

        rows.append({
            "target": t,
            "n_preds": n_total,
            "n_with_actual": n_actual,
            "rmse": rmse(y, mu),
            "coverage_10_90": coverage(y, q10, q90),
            "avg_width": float((q90 - q10).mean()),
            "as_of_utc": _now_date_str(),
            "window_days": days,
        })

    out_df = pd.DataFrame(rows).sort_values(["target"]).reset_index(drop=True)
    if out_df.empty:
        typer.echo("No metrics computed (no actuals in the selected window).")
        return

    date_tag = _now_date_str()
    metrics_csv = os.path.join(out, f"metrics_{date_tag}_last_{days}d.csv")
    raw_csv     = os.path.join(out, f"eval_raw_{date_tag}_last_{days}d.csv")

    out_df.to_csv(metrics_csv, index=False)
    df.to_csv(raw_csv, index=False)

    if echo_table:
        typer.echo(out_df.to_string(index=False))

    typer.echo(f"\nWrote metrics → {metrics_csv}")
    typer.echo(f"Wrote raw join  → {raw_csv}")

@app.command()
def rolling_metrics(days: int = typer.Option(14, help="Rolling window (days) to evaluate")) -> None:
    """Back-compat console-only view."""
    con = duckdb.connect(settings.DUCKDB_PATH)
    try:
        df = _eval_frame(con, days)
         # --- normalize/repair columns so 'target' is guaranteed to exist -----------
        # lower-case and strip any whitespace from all column names
        df.columns = [str(c).strip().lower() for c in df.columns]

        # if 'target' still missing, try common alternates then rename -> 'target'
        if "target" not in df.columns:
            for alt in ("p_target", "variable", "outcome", "label"):
                if alt in df.columns:
                    df = df.rename(columns={alt: "target"})
                    break

        # if still missing, print what we do have and stop gracefully
        if "target" not in df.columns:
          typer.echo(f"Joined frame is missing 'target'. Columns found: {list(df.columns)}")
          return
    finally:
        con.close()

    if df.empty or "target" not in df.columns:
        typer.echo("No data for window.")
        return

    results = []
    for t, g in df.groupby("target"):
        if g["actual"].notna().sum() == 0:

if __name__ == "__main__":
    app()
