from __future__ import annotations
import typer
import pandas as pd
import uuid
from ..config import settings
from ..data.load_ytd import load_ytd
from ..data.fetch_recent import fetch_recent
from ..data.fetch_projections import naive_projections_from_recent
from ..data.persist import init_db, append
from ..features.engineer import engineer_minimal
from ..features.registry import PLAYER_FEATURES, TEAM_FEATURES
from ..modeling.trainers import train_player_count, train_team_goals
from ..modeling.predictors import predict_player_counts, predict_match_totals

app = typer.Typer(help="Prediction commands")

@app.command()
def tomorrow(
    ytd_csv: str = typer.Option("data/NHL_YTD.csv", help="Path to last season CSV"),
    date: str = typer.Option(None, help="Prediction slate date (YYYY-MM-DD)"),
):
    """Scaffold end-to-end predict: trains quickly from YTD + predicts for (naive) slate from recent."""
    init_db()
    run_id = str(uuid.uuid4())

    # Load and feature engineer
    df_ytd = load_ytd(ytd_csv)
    df_feat = engineer_minimal(df_ytd)

    # Train minimal models inline (scaffold: replace with loaded models later)
    bundle_points = train_player_count(df_feat, PLAYER_FEATURES, target="points")
    bundle_goals = train_player_count(df_feat, PLAYER_FEATURES, target="goals")
    bundle_assists = train_player_count(df_feat, PLAYER_FEATURES, target="assists")
    bundle_shots = train_player_count(df_feat, PLAYER_FEATURES, target="shots_on_goal")

    team = df_feat.groupby(["date","game_id","team","opponent","home_or_away"], as_index=False)["points"].sum()
    team = team.rename(columns={"points":"team_goals"})
    bundle_team_home = train_team_goals(team, TEAM_FEATURES, target="team_goals")
    bundle_team_away = bundle_team_home

    # Build naive slate from "recent" (empty by default; in real use, call projections API)
    recent = fetch_recent([date] if date else [])
    slate = naive_projections_from_recent(recent)
    if slate.empty:
        # Fallback to last YTD date subset so the pipeline produces output
        slate = df_feat[["date","game_id","team","opponent","player_id","name"]].drop_duplicates().tail(100)

    # Join features for these players
    feat_cols = ["date","game_id","team","opponent","player_id","name"] + PLAYER_FEATURES
    player_rows = slate.merge(df_feat[feat_cols].drop_duplicates(), on=["date","game_id","team","opponent","player_id","name"], how="left").fillna(0)

    preds_points = predict_player_counts(bundle_points, player_rows, run_id, target="points")
    preds_goals = predict_player_counts(bundle_goals, player_rows, run_id, target="goals")
    preds_assists = predict_player_counts(bundle_assists, player_rows, run_id, target="assists")
    preds_shots = predict_player_counts(bundle_shots, player_rows, run_id, target="shots_on_goal")

    # Match totals: collapse to team rows (use last known features as proxy)
    team_rows = team[["date","game_id","team","opponent","home_or_away"] + TEAM_FEATURES].drop_duplicates()
    preds_totals = predict_match_totals(bundle_team_home, bundle_team_away, team_rows, run_id)

    TEAM_KEYS = ["date","game_id","team","opponent","home_or_away"]

    # Build team target (not strictly needed for prediction, but harmless)
    team_target = (
        df_feat.groupby(TEAM_KEYS, as_index=False)["points"]
          .sum()
          .rename(columns={"points":"team_goals"})
)

# Team-level features aggregated from player rows
team_features = (
    df_feat.groupby(TEAM_KEYS, as_index=False)[TEAM_FEATURES]
          .mean()
)

# Rows used for match totals prediction (must include TEAM_FEATURES)
team_rows = team_features  # already has TEAM_KEYS + TEAM_FEATURES

preds_totals = predict_match_totals(bundle_team_home, bundle_team_away, team_rows, run_id)
TEAM_KEYS = ["date","game_id","team","opponent","home_or_away"]

    
# Build team target (not strictly needed for prediction, but harmless) team_target = (
df_feat.groupby(TEAM_KEYS, as_index=False)["points"].sum().rename(columns={"points":"team_goals"})
#)

# Team-level features aggregated from player rows
team_features = (
    df_feat.groupby(TEAM_KEYS, as_index=False)[TEAM_FEATURES]
          .mean()
)

# Rows used for match totals prediction (must include TEAM_FEATURES)
team_rows = team_features  # already has TEAM_KEYS + TEAM_FEATURES

preds_totals = predict_match_totals(bundle_team_home, bundle_team_away, team_rows, run_id)
    # Append to DuckDB
all_preds = pd.concat([preds_points, preds_goals, preds_assists, preds_shots, preds_totals], ignore_index=True)
append("fact_predictions", all_preds)

# Emit CSV for quick inspection
out_csv = f"{settings.PARQUET_DIR}/predictions_{run_id}.csv"
os.makedirs(settings.PARQUET_DIR, exist_ok=True)
all_preds.to_csv(out_csv, index=False)
typer.echo(f"Wrote predictions → {out_csv}\nRun ID: {run_id}")