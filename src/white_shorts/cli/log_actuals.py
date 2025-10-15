from __future__ import annotations
import typer, pandas as pd
from ..data.persist import append
app = typer.Typer(help="Actuals logging")
@app.command()
def from_csv(csv_path: str):
    df = pd.read_csv(csv_path, parse_dates=["date"])
    append("fact_actuals", df)
    typer.echo(f"Logged {len(df)} actual rows")
