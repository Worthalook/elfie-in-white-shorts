from __future__ import annotations
import os
import typer
import pandas as pd
from typing import Optional
from ..data.load_ytd import load_ytd
from ..features.engineer import engineer_minimal
from ..features.registry import PLAYER_FEATURES
from ..modeling.trainers_qrf import train_player_qrf
from ..modeling.io_qrf import save_qrf

app = typer.Typer(help="Train Quantile Random Forest models for player targets")

def _train_one(df_feat: pd.DataFrame, target: str, version: str = "0.3.0") -> str:
    bundle = train_player_qrf(df_feat, PLAYER_FEATURES, target=target, version=version)
    path = save_qrf(bundle)
    return path

@app.command()
def all(ytd_csv: Optional[str] = typer.Option(None, help="Path to YTD CSV (defaults to env WS_YTD_CSV or data/NHL_YTD.csv)"),
        version: str = typer.Option("0.3.0", help="Model version tag to embed in filenames")):
    """Train QRF for points, goals, assists, shots_on_goal."""
    csv_path = ytd_csv or os.getenv("WS_YTD_CSV", "data/NHL_YTD.csv")
    df_ytd = load_ytd(csv_path)
    df_feat = engineer_minimal(df_ytd)

    results = {}
    for tgt in ["points", "goals", "assists", "shots_on_goal"]:
        path = _train_one(df_feat, tgt, version=version)
        results[tgt] = path
        typer.echo(f"Trained & saved QRF for {tgt} → {path}")
    return results

@app.command()
def target(name: str = typer.Argument(..., help="One of: points, goals, assists, shots_on_goal"),
           ytd_csv: Optional[str] = typer.Option(None, help="Path to YTD CSV (defaults to env WS_YTD_CSV or data/NHL_YTD.csv)"),
           version: str = typer.Option("0.3.0", help="Model version tag to embed in filenames")):
    """Train QRF for a single target."""
    if name not in {"points","goals","assists","shots_on_goal"}:
        raise typer.BadParameter("name must be one of: points, goals, assists, shots_on_goal")
    csv_path = ytd_csv or os.getenv("WS_YTD_CSV", "data/NHL_YTD.csv")
    df_ytd = load_ytd(csv_path)
    df_feat = engineer_minimal(df_ytd)
    path = _train_one(df_feat, name, version=version)
    typer.echo(f"Trained & saved QRF for {name} → {path}")

if __name__ == "__main__":
    app()
