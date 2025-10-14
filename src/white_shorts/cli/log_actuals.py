from __future__ import annotations
import typer
import pandas as pd
from ..data.persist import append

app = typer.Typer(help="Actuals logging")

@app.command()
def from_csv(csv_path: str):
    """Log actuals from a csv with columns: target,date,game_id,team,opponent,player_id,name,actual"""
    df = pd.read_csv(csv_path, parse_dates=["date"])  # minimal validation
    append("fact_actuals", df)
    typer.echo(f"Logged {len(df)} actual rows")
