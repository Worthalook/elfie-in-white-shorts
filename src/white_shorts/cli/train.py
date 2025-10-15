from __future__ import annotations
import typer
from ..config import settings
from ..data.load_ytd import load_ytd
from ..data.persist import init_db
from ..features.engineer import engineer_minimal
from ..features.registry import PLAYER_FEATURES, TEAM_FEATURES
from ..modeling.trainers import train_player_count, train_team_goals
from ..modeling.targets import Target
from ..modeling.io import save_model

app = typer.Typer(help="Training commands")

@app.command()
def all(csv_path: str = typer.Option("data/NHL_YTD.csv", help="Path to last season CSV")):
    """Train and save all models (players and team totals)."""
    init_db()
    df = load_ytd(csv_path)
    df_feat = engineer_minimal(df)

    # --- Player-level models ---
    for t in [Target.POINTS, Target.GOALS, Target.ASSISTS, Target.SHOTS]:
        b = train_player_count(
            df_feat,
            PLAYER_FEATURES,
            t.value,
            sample_weight=None,
            version=settings.MODEL_VERSION_TAG,
        )
        path = save_model(b)
        typer.echo(f"Trained & saved {b.model_name} → {path}")

    # --- Team-level model (with features) ---
    keys = ["date","game_id","team","opponent","home_or_away"]

    team_target = (
        df_feat.groupby(keys, as_index=False)["points"].sum()
               .rename(columns={"points": "team_goals"})
    )

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

    bteam = train_team_goals(
        team_df,
        TEAM_FEATURES,
        target="team_goals",
        version=settings.MODEL_VERSION_TAG,
    )
    path_team = save_model(bteam)
    typer.echo(f"Trained & saved team goals model → {path_team}")
