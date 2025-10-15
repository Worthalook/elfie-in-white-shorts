from __future__ import annotations
import typer, duckdb, pandas as pd
from ..config import settings
from ..modeling.evaluation import rmse, coverage
app = typer.Typer(help="Metrics extraction (scaffold)")
@app.command()
def rolling_metrics(days: int = 14):
    con = duckdb.connect(settings.DUCKDB_PATH)
    q = f"""
    WITH preds AS (
      SELECT * FROM fact_predictions WHERE date >= DATE '1970-01-01'
    ), acts AS (
      SELECT * FROM fact_actuals WHERE date >= DATE '1970-01-01'
    )
    SELECT p.target, p.date, p.game_id, p.team, p.opponent, p.player_id, p.name,
           p.lambda_or_mu as mu, p.q10, p.q90, p.p_ge_k_json,
           a.actual
    FROM preds p LEFT JOIN acts a USING(target,date,game_id,team,opponent,player_id,name)
    WHERE p.date >= (CURRENT_DATE - INTERVAL {days} DAY);
    """
    df = con.execute(q).fetchdf(); con.close()
    if df.empty: return typer.echo("No data for window.")
    results = []
    for t, g in df.groupby("target"):
        if g["actual"].notna().sum() == 0: continue
        results.append({"target": t, "metric": "rmse", "value": rmse(g["actual"].fillna(0), g["mu"].fillna(0))})
        results.append({"target": t, "metric": "coverage_10_90", "value": coverage(g["actual"].fillna(0), g["q10"].fillna(0), g["q90"].fillna(0))})
    out = pd.DataFrame(results)
    typer.echo(out.to_string(index=False) if not out.empty else "No metrics computed.")
