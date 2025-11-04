from __future__ import annotations
import os
import typer
import pandas as pd
from ..data.projections import fetch_projections_by_date

app = typer.Typer(help="Fetch projections slate (players+games)")

@app.command()
def by_date(date: str = typer.Argument(..., help="YYYY-MM-DD or DD/MM/YYYY")):
    #df = fetch_projections_by_date(date)
    # top: import pandas as pd, from pathlib import Path

# ... inside your main(date: str) / handler:
    parsed = pd.to_datetime(date, dayfirst=True, errors="coerce")
    if pd.isna(parsed):
        raise typer.BadParameter(f"Unparseable date: {date}")

    iso = parsed.date().isoformat()                # ← ALWAYS 'YYYY-MM-DD'
    out_dir = os.getenv("WS_SLATES_DIR", "data/slates")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"slate_{iso}.parquet"

    df.to_parquet(out_path, index=False)
    typer.echo(f"[fetch_slate] Wrote {len(df)} rows → {out_path}")


if __name__ == "__main__":
    app()
