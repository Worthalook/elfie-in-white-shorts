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

@app.command()
def build(
    days: int = typer.Option(14, help="Rolling window (days) to evaluate"),
    out: str = typer.Option("data/dashboards", help="Output directory for artifacts"),
    echo_table: bool = typer.Option(True, help="Print summary table to stdout"),
) -> None:
    """
    Build rolling metrics over the last N days, write CSV artifacts, and (optionally) echo a table.
    Expected tables in DuckDB:
      - fact_predictions (required)
      - fact_actuals (preferred; else you'll see 'No data for window.')
    """
    os.makedirs(out, exist_ok=True)

    con = duckdb.connect(settings.DUCKDB_PATH)
    try:
        q = f"""
        WITH preds AS (
          SELECT *
          FROM fact_predictions
          WHERE date >= DATE '1970-01-01'
        ),
        acts AS (
          SELECT *
          FROM fact_actuals
          WHERE date >= DATE '1970-01-01'
        )
        SELECT
          p.target, p.date, p.game_id, p.team, p.opponent, p.player_id, p.name,
          p.lambda_or_mu AS mu, p.q10, p.q90, p.p_ge_k_json,
          a.actual
        FROM preds p
        LEFT JOIN acts a
          USING (target, date, game_id, team, opponent, player_id, name)
        WHERE p.date >= (CURRENT_DATE - INTERVAL {days} DAY)
        """
        df = con.execute(q).fetchdf()
    finally:
        con.close()

    if df.empty:
        typer.echo("No data for window. (Check that fact_predictions/fact_actuals are populated.)")
        return

    rows = []
    for t, g in df.groupby("target"):
        n_total = len(g)
        n_actual = int(g["actual"].notna().sum())
        if n_actual == 0:
            continue
        mu  = g["mu"].fillna(0)
        y   = g["actual"].fillna(0)
        q10 = g["q10"].fillna(0)
        q90 = g["q90"].fillna(0)

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

    date_tag   = _now_date_str()
    metrics_csv = os.path.join(out, f"metrics_{date_tag}_last_{days}d.csv")
    raw_csv     = os.path.join(out, f"eval_raw_{date_tag}_last_{days}d.csv")

    out_df.to_csv(metrics_csv, index=False)
    df.to_csv(raw_csv, index=False)

    if echo_table:
        typer.echo(out_df.to_string(index=False))

    typer.echo(f"\nWrote metrics → {metrics_csv}")
    typer.echo(f"Wrote raw join  → {raw_csv}")


@app.command()
def rolling_metrics(
    days: int = typer.Option(14, help="Rolling window (days) to evaluate")
) -> None:
    """
    Backward-compatible: prints the summary table only (no artifacts).
    """
    con = duckdb.connect(settings.DUCKDB_PATH)
    try:
        q = f"""
        WITH preds AS (
          SELECT * FROM fact_predictions WHERE date >= DATE '1970-01-01'
        ), acts AS (
          SELECT * FROM fact_actuals WHERE date >= DATE '1970-01-01'
        )
        SELECT p.target, p.date, p.game_id, p.team, p.opponent, p.player_id, p.name,
               p.lambda_or_mu AS mu, p.q10, p.q90, p.p_ge_k_json,
               a.actual
        FROM preds p LEFT JOIN acts a
        USING(target, date, game_id, team, opponent, player_id, name)
        WHERE p.date >= (CURRENT_DATE - INTERVAL {days} DAY);
        """
        df = con.execute(q).fetchdf()
    finally:
        con.close()

    if df.empty:
        typer.echo("No data for window.")
        return

    results = []
    for t, g in df.groupby("target"):
        if g["actual"].notna().sum() == 0:
            continue
        results.append({"target": t, "metric": "rmse",
                        "value": rmse(g["actual"].fillna(0), g["mu"].fillna(0))})
        results.append({"target": t, "metric": "coverage_10_90",
                        "value": coverage(g["actual"].fillna(0), g["q10"].fillna(0), g["q90"].fillna(0))})

    out = pd.DataFrame(results)
    typer.echo(out.to_string(index=False) if not out.empty else "No metrics computed.")


if __name__ == "__main__":
    app()
