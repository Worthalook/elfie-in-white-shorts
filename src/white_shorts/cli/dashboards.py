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
    exists = {}
    for t in ("fact_predictions", "fact_actuals"):
        exists[t] = bool(con.execute(
            "SELECT COUNT(*)>0 FROM information_schema.tables WHERE table_name = ?",
            [t]
        ).fetchone()[0])
    return exists

def _eval_frame(con: duckdb.DuckDBPyConnection, days: int) -> pd.DataFrame:
    exists = _ensure_tables(con)

    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW preds_win AS
        SELECT
            target AS p_target,
            date, game_id, team, opponent, player_id, name,
            lambda_or_mu AS mu, q10, q90, p_ge_k_json
        FROM fact_predictions
        WHERE date >= (CURRENT_DATE - INTERVAL {int(days)} DAY)
    """)

    if exists.get("fact_actuals", False):
        con.execute(f"""
            CREATE OR REPLACE TEMP VIEW acts_long AS
            SELECT target, date, game_id, team, opponent, player_id, name, actual
            FROM fact_actuals
            WHERE date >= (CURRENT_DATE - INTERVAL {int(days)} DAY)
        """)
    else:
        cur_path = os.getenv("WS_CURRENT_SEASON_PARQUET", "data/current_season.parquet")
        con.execute(f"""
            CREATE OR REPLACE TEMP VIEW cur_raw AS
            SELECT * FROM read_parquet('{cur_path}')
            WHERE date >= (CURRENT_DATE - INTERVAL {int(days)} DAY)
        """)
        con.execute("""
            CREATE OR REPLACE TEMP VIEW acts_long AS
            SELECT 'points' AS target, date, game_id, team, opponent, player_id, name, CAST(points AS DOUBLE) AS actual FROM cur_raw
            UNION ALL
            SELECT 'goals', date, game_id, team, opponent, player_id, name, CAST(goals AS DOUBLE) FROM cur_raw
            UNION ALL
            SELECT 'assists', date, game_id, team, opponent, player_id, name, CAST(assists AS DOUBLE) FROM cur_raw
            UNION ALL
            SELECT 'shots_on_goal', date, game_id, team, opponent, player_id, name, CAST(shots_on_goal AS DOUBLE) FROM cur_raw
        """)

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

def _consistency_table(df: pd.DataFrame) -> pd.DataFrame:
    scales = (
        df.dropna(subset=["actual"])
          .groupby("target")["actual"]
          .quantile(0.95)
          .rename("s_scale")
    )
    out = []
    for t, g in df.groupby("target"):
        n_total = len(g)
        ymask = g["actual"].notna()
        n_actual = int(ymask.sum())
        if n_actual == 0:
            continue
        y   = g.loc[ymask, "actual"].fillna(0.0)
        mu  = g.loc[ymask, "mu"].fillna(0.0)
        q10 = g.loc[ymask, "q10"].fillna(0.0)
        q90 = g.loc[ymask, "q90"].fillna(0.0)

        cov = coverage(y, q10, q90)
        cal_err = abs(cov - 0.80)

        width = float((q90 - q10).mean())
        s_scale = float(scales.get(t, max(1.0, y.quantile(0.95))))
        sharp_norm = 1.0 - min(1.0, width / s_scale)

        def pinball(y, q, tau):
            d = y - q
            return float(((tau * d).clip(lower=0) + ((tau - 1) * d).clip(upper=0)).mean())

        pin_10 = pinball(y, q10, 0.10)
        pin_90 = pinball(y, q90, 0.90)
        pin_avg = 0.5 * (pin_10 + pin_90)

        naive = pinball(y, pd.Series([y.mean()] * len(y), index=y.index), 0.5)
        denom = max(1e-6, naive)
        pinball_norm = max(0.0, min(1.0, 1.0 - (pin_avg / denom)))

        daily = (0.5 * (
            (0.10 * (y - q10)).clip(lower=0) + ((0.10 - 1) * (y - q10)).clip(upper=0) +
            (0.90 * (y - q90)).clip(lower=0) + ((0.90 - 1) * (y - q90)).clip(upper=0)
        ))
        st = float(daily.std()) if len(daily) > 1 else 0.0
        stability = 1.0 / (1.0 + st)

        score = 100.0 * (0.40 * (1 - cal_err) + 0.30 * sharp_norm + 0.20 * pinball_norm + 0.10 * stability)
        out.append({
            "target": t,
            "n_preds": n_total,
            "n_with_actual": n_actual,
            "coverage_10_90": cov,
            "avg_width": width,
            "sharp_norm": sharp_norm,
            "pinball_norm": pinball_norm,
            "stability": stability,
            "consistency_score": score,
        })
    if not out:
        return pd.DataFrame(columns=["target","n_preds","n_with_actual","coverage_10_90","avg_width","sharp_norm","pinball_norm","stability","consistency_score"])
    return pd.DataFrame(out).sort_values("target").reset_index(drop=True)

@app.command()
def build(
    days: int = typer.Option(14, help="Rolling window (days) to evaluate"),
    out: str = typer.Option("data/dashboards", help="Output directory for artifacts"),
    echo_table: bool = typer.Option(True, help="Print summary table to stdout"),
) -> None:
    os.makedirs(out, exist_ok=True)
    con = duckdb.connect(settings.DUCKDB_PATH)
    try:
        df = _eval_frame(con, days)
    finally:
        con.close()

    df.columns = [str(c).strip().lower() for c in df.columns]
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

    date_tag = _now_date_str()
    raw_csv = os.path.join(out, f"eval_raw_{date_tag}_last_{days}d.csv")
    df.to_csv(raw_csv, index=False)

    if not rows:
        typer.echo("No metrics computed (no rows with actuals in the selected window).")
        typer.echo(f"Wrote raw join  → {raw_csv}")
        return

    metrics_df = pd.DataFrame(rows).sort_values("target").reset_index(drop=True)
    metrics_csv = os.path.join(out, f"metrics_{date_tag}_last_{days}d.csv")
    metrics_df.to_csv(metrics_csv, index=False)

    kpi_df = _consistency_table(df)
    consistency_csv = os.path.join(out, f"consistency_{date_tag}_last_{days}d.csv")
    kpi_df.to_csv(consistency_csv, index=False)

    html_path = os.path.join(out, f"summary_{date_tag}_last_{days}d.html")
    with open(html_path, "w", encoding="utf-8") as f:
        f.write("<html><head><meta charset='utf-8'><title>WhiteShorts Dash</title></head><body>")
        f.write(f"<h2>WhiteShorts metrics — last {days} days (UTC as of {date_tag})</h2>")
        f.write("<h3>Per-target summary</h3>")
        f.write(metrics_df.to_html(index=False, float_format=lambda x: f\"{x:.4f}\" if isinstance(x, float) else x))
        f.write("<h3>Consistency (headline KPI)</h3>")
        f.write(kpi_df.to_html(index=False, float_format=lambda x: f\"{x:.4f}\" if isinstance(x, float) else x))
        f.write(f"<p><em>Raw join: {os.path.basename(raw_csv)}</em></p>")
        f.write("</body></html>")

    if echo_table:
        typer.echo(metrics_df.to_string(index=False))
        typer.echo("\nConsistency KPI:")
        typer.echo(kpi_df.to_string(index=False))

    typer.echo(f"\nWrote metrics     → {metrics_csv}")
    typer.echo(f"Wrote consistency → {consistency_csv}")
    typer.echo(f"Wrote HTML        → {html_path}")
    typer.echo(f"Wrote raw join    → {raw_csv}")

@app.command()
def rolling_metrics(days: int = typer.Option(14, help="Rolling window (days) to evaluate")) -> None:
    con = duckdb.connect(settings.DUCKDB_PATH)
    try:
        df = _eval_frame(con, days)
    finally:
        con.close()

    df.columns = [str(c).strip().lower() for c in df.columns]
    if df.empty or "target" not in df.columns:
        typer.echo("No data for window.")
        return

    results = []
    for t, g in df.groupby("target"):
        if g["actual"].notna().sum() == 0:
            continue
        results.append({
            "target": t,
            "metric": "rmse",
            "value": rmse(g["actual"].fillna(0), g["mu"].fillna(0))
        })
        results.append({
            "target": t,
            "metric": "coverage_10_90",
            "value": coverage(g["actual"].fillna(0), g["q10"].fillna(0), g["q90"].fillna(0))
        })

    if not results:
        typer.echo("No metrics computed (no rows with actuals).")
        return

    out = pd.DataFrame(results).sort_values("target").reset_index(drop=True)
    typer.echo(out.to_string(index=False))

if __name__ == "__main__":
    app()
