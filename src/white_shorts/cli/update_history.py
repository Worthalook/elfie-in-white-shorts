from __future__ import annotations
import typer
from ..data.update_history import fetch_player_stats_by_date, upsert_current_season

app = typer.Typer(help="Fetch PlayerGameStatsByDate and update current-season store")

@app.command()
def by_date(date: str = typer.Argument(..., help="YYYY-MM-DD or DD/MM/YYYY")):
    df = fetch_player_stats_by_date(date)
    if df.empty:
        typer.echo("No actuals returned.")
        return
    path = upsert_current_season(df)
    typer.echo(f"Updated current-season store → {path} (+{len(df)} rows)")

if __name__ == "__main__":
    app()
