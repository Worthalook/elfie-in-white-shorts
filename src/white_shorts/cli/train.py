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
def all(csv_path: str = typer.Option("data/NHL_2023_24.csv", help="Path to last season CSV")):
    """Train minimal models for player counts and team goals (for totals)."""
    init_db()
    df = load_ytd(csv_path)
    df_feat = engineer_minimal(df)

    
    # AFTER (keeps TEAM_FEATURES)
    keys = ["date","game_id","team","opponent","home_or_away"]

    # Target: proxy using summed points (replace with real team GF when available)
    team_target = (
        df_feat.groupby(keys, as_index=False)["points"].sum()
               .rename(columns={"points": "team_goals"})
    )

    # Features: aggregate engineered columns at team-game level
    team_feats = (
        df_feat.groupby(keys, as_index=False)
               .agg({
                   "home_or_away": "first",
                   "days_off_team": "max",
                   "team_gf_5": "mean",
                   "team_ga_5": "mean",
                   "opp_team_gf_5": "mean",
                   "opp_team_ga_5": "mean",
                   "opp_goalie_ga_smooth": "mean",
               })
    )

    team_df = team_target.merge(team_feats, on=keys, how="left")

    bteam = train_team_goals(team_df, TEAM_FEATURES, target="team_goals", version=settings.MODEL_VERSION_TAG)
    path_team = save_model(bteam)

    typer.echo(f"Trained & saved team goals model → {path_team}")
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


def save_model(bteam ):
    return        