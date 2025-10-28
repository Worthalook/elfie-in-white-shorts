from __future__ import annotations
import glob, os
import duckdb
import typer
from ..config import settings

app = typer.Typer(help="Audit recent ETL & training provenance")

@app.command()
def models_meta(models_dir: str = typer.Option("models", help="Models directory")):
    """Print the latest model meta sidecars (*.meta.json) if present."""
    metas = sorted(glob.glob(os.path.join(models_dir, "*.meta.json")))
    if not metas:
        typer.echo("No model meta files found (e.g., *.meta.json).")
        return
    for m in metas[-10:]:
        print("="*80)
        print(m)
        try:
            print(open(m, "r", encoding="utf-8").read())
        except Exception as e:
            print(f"[warn] failed to read {m}: {e}")

@app.command()
def duckdb_status():
    """Print max dates and row counts from DuckDB prediction/actual tables."""
    con = duckdb.connect(settings.DUCKDB_PATH)
    try:
        def _exists(tbl: str) -> bool:
            return con.execute(
                f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{tbl}'"
            ).fetchone()[0] > 0

        if _exists("fact_predictions"):
            print("-- fact_predictions --")
            print(con.execute(
                "SELECT COUNT(*) AS n, MIN(date) AS min_date, MAX(date) AS max_date FROM fact_predictions"
            ).fetchdf().to_string(index=False))
        else:
            print("fact_predictions not found")

        if _exists("fact_actuals"):
            print("-- fact_actuals --")
            print(con.execute(
                "SELECT COUNT(*) AS n, MIN(date) AS min_date, MAX(date) AS max_date FROM fact_actuals"
            ).fetchdf().to_string(index=False))
        else:
            print("fact_actuals not found")
    finally:
        con.close()

if __name__ == "__main__":
    app()
