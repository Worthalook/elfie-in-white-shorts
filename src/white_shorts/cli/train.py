from __future__ import annotations
import typer
import pandas as pd
from ..config import settings
from ..data.load_ytd import load_ytd
from ..data.persist import init_db
from ..features.engineer import engineer_minimal
from ..features.registry import PLAYER_FEATURES, TEAM_FEATURES
from ..modeling.trainers import train_player_count, train_team_goals
from ..modeling.targets import Target

app = typer.Typer(help="Training commands")

@app.command()
def all(csv_path: str = typer.Option("data/NHL_YTD.csv", help="Path to last season CSV")):
    """Train minimal models for player counts and team goals (for totals)."""
    init_db()
    df = load_ytd(csv_path)
    df_feat = engineer_minimal(df)

    # Player models
    for t in [Target.POINTS, Target.GOALS, Target.ASSISTS, Target.SHOTS]:
        _ = train_player_count(df_feat, PLAYER_FEATURES, t.value,
                               sample_weight=None, version=settings.MODEL_VERSION_TAG)
        typer.echo(f"Trained lgbm_poisson_{t.value} with {len(PLAYER_FEATURES)} features")

    # Team goals (naive): aggregate per team-game as target
    team = df_feat.groupby(["date","game_id","team","opponent","home_or_away"], as_index=False)["points"].sum()
    team = team.rename(columns={"points":"team_goals"})  # TODO: replace with real team goals if available
    _ = train_team_goals(team, TEAM_FEATURES, target="team_goals", version=settings.MODEL_VERSION_TAG)

    typer.echo("Training complete (scaffold). Persist models using joblib if desired (TODO).")
