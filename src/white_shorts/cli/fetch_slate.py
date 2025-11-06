from __future__ import annotations
import os
import typer
import pandas as pd
from ..data.projections import fetch_projections_by_date


app = typer.Typer(help="Fetch projections slate (players+games)")

def _parse_date(date_str: str) -> pd.Timestamp:
    #try:
    d = pd.to_datetime(date_str, format='%Y-%m-%d')
    typer.echo(f"PARSE DATE: {d}")
    # except Exception:
        
    if pd.isna(d):
        raise ValueError(f"Unparseable date: {date_str}")
    return d.normalize()

@app.command()
def by_date(date: str = typer.Argument(..., help="YYYY-MM-DD or DD/MM/YYYY")):
    df = fetch_projections_by_date(date)
    if df.empty:
        typer.echo("No projections returned.")
        return
    out_dir = os.getenv("WS_SLATES_DIR", "data/slates")
    os.makedirs(out_dir, exist_ok=True)
    dd = _parse_date(date)
    slate_name = f"slate_{dd.year}-{dd.month}-{dd.day}.parquet"
    typer.echo(f"Saved slate (debug) → {slate_name}")
    out_path = os.path.join(out_dir, slate_name) #f"slate_{dd.year}-{dd.month}-{dd.day:02d}.parquet")
    df.to_parquet(out_path, index=False)
    typer.echo(f"Saved slate → {out_path} (players: {len(df)})")

if __name__ == "__main__":
    app()
