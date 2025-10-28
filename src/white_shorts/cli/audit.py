from __future__ import annotations
import glob, json, os
import pandas as pd
import duckdb
import typer
from ..config import settings

app = typer.Typer(help="Training provenance audit utilities")

@app.command()
def print_latest(models_dir: str = typer.Option("models", help="Folder with *.meta.json")):
    files = sorted(glob.glob(os.path.join(models_dir, "*.meta.json")))
    if not files:
        typer.echo("No model meta files found.")
        raise typer.Exit(code=0)
    latest = files[-1]
    typer.echo(f"LATEST META → {latest}")
    typer.echo(open(latest, "r", encoding="utf-8").read())

@app.command()
def persist(models_dir: str = typer.Option("models", help="Folder with *.meta.json")):
    """Load all model meta JSONs and persist a flat audit table in DuckDB."""
    files = sorted(glob.glob(os.path.join(models_dir, "*.meta.json")))
    if not files:
        typer.echo("No model meta files found; nothing to persist.")
        raise typer.Exit(code=0)

    rows = []
    for p in files:
        try:
            js = json.load(open(p, "r", encoding="utf-8"))
            js["_path"] = p
            rows.append(js)
        except Exception as e:
            typer.echo(f"Skipping {p}: {e}", err=True)
    if not rows:
        typer.echo("No readable meta files.")
        raise typer.Exit(code=0)

    df = pd.DataFrame(rows)
    con = duckdb.connect(settings.DUCKDB_PATH)
    try:
        con.execute("CREATE TABLE IF NOT EXISTS training_audit AS SELECT * FROM df LIMIT 0")
        con.execute("INSERT INTO training_audit SELECT * FROM df")
    finally:
        con.close()

    typer.echo(f"Persisted {len[df]} rows into training_audit")
    cols = [c for c in df.columns if c in ("created_ts","model_name","model_version","target","train_rows","train_cutoff_max_date","features_hash")]
    if cols:
        typer.echo(df[cols].sort_values("created_ts").tail(10).to_string(index=False))

if __name__ == "__main__":
    app()
