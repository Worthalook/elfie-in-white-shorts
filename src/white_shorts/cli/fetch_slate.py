from __future__ import annotations
import os
import typer
import pandas as pd
from ..data.projections import fetch_projections_by_date

app = typer.Typer(help="Fetch projections slate (players+games)")

@app.command()
def by_date(date: str = typer.Argument(..., help="YYYY-MM-DD or DD/MM/YYYY")):
    df = fetch_projections_by_date(date)
    if df.empty:
        typer.echo("No projections returned.")
        return
    out_dir = os.getenv("WS_SLATES_DIR", "data/slates")
    os.makedirs(out_dir, exist_ok=True)
    dd = pd.to_datetime(date, dayfirst=True).date()
    out_path = os.path.join(out_dir, f"slate_{dd.year}-{dd.month}-{dd.day:02d}.parquet")
    df.to_parquet(out_path, index=False)
    typer.echo(f"Saved slate → {out_path} (players: {len(df)})")

if __name__ == "__main__":
    app()
