from __future__ import annotations
import typer, uuid, os
import pandas as pd
from ..config import settings
from ..data.load_ytd import load_ytd
from ..data.fetch_recent import fetch_recent
from ..data.fetch_projections import naive_projections_from_recent, fetch_player_projections_by_date
from ..data.persist import init_db, append
from ..features.engineer import engineer_minimal
from ..features.registry import PLAYER_FEATURES, TEAM_FEATURES
from ..modeling.trainers import train_player_count, train_team_goals
from ..modeling.predictors import predict_player_counts, predict_match_totals
from ..modeling.io import load_model, latest_model_path

app = typer.Typer(help="Prediction commands")
def _check_features(df, required, where=""):
    missing = [c for c in required if c not in df.columns]
    if missing:
       raise KeyError(f"Missing features {missing} in {where}. Present sample: {df.columns.tolist()[:25]}")

@app.command()
def tomorrow(
    
    ytd_csv: str = typer.Option("data/NHL_2023_24.csv", help="Path to last season CSV"),
    date: str = typer.Option(None, help="Prediction slate date (YYYY-MM-DD)"),
):
    init_db()
    run_id = str(uuid.uuid4())
    df_ytd = load_ytd(ytd_csv)
    df_feat = engineer_minimal(df_ytd)

    _check_features(df_feat, TEAM_FEATURES, where="df_feat (engineered)")

    def _bundle_from_loaded(d):
        class B: pass
        b = B(); b.model = d["model"]; b.features = d["features"]; b.target = d["target"]; b.model_name = d["model_name"]; b.model_version = d["model_version"]; return b
    def _load_or_train(prefix, trainer, *args, **kwargs):
        path = latest_model_path(prefix)
        if path:
            d = load_model(path)
            return _bundle_from_loaded(d)
        return trainer(*args, **kwargs)

    bundle_points  = _load_or_train("lgbm_poisson_points",         train_player_count, df_feat, PLAYER_FEATURES, target="points")
    bundle_goals   = _load_or_train("lgbm_poisson_goals",          train_player_count, df_feat, PLAYER_FEATURES, target="goals")
    bundle_assists = _load_or_train("lgbm_poisson_assists",        train_player_count, df_feat, PLAYER_FEATURES, target="assists")
    bundle_shots   = _load_or_train("lgbm_poisson_shots_on_goal",  train_player_count, df_feat, PLAYER_FEATURES, target="shots_on_goal")

    team = df_feat.groupby(["date","game_id","team","opponent","home_or_away"], as_index=False)["points"].sum()
    team = team.rename(columns={"points":"team_goals"})
    bundle_team_home = _load_or_train("lgbm_poisson_team_goals", train_team_goals, team, TEAM_FEATURES, target="team_goals")
    bundle_team_away = bundle_team_home

    slate = fetch_player_projections_by_date(date) if date else None
    if slate is None or slate.empty:
        recent = fetch_recent([date] if date else [])
        slate = naive_projections_from_recent(recent)
    if slate.empty:
        slate = df_feat[["date","game_id","team","opponent","player_id","name"]].drop_duplicates().tail(100)
    # --- Ensure consistent datetime type for merges ---
    df_feat["date"] = pd.to_datetime(df_feat["date"], errors="coerce")
    slate["date"]   = pd.to_datetime(slate["date"], errors="coerce")

    for c in ["game_id","team","opponent","player_id","name"]:
        if c in df_feat.columns:
            df_feat[c] = df_feat[c].astype(str).str.strip()
        if c in slate.columns:
            slate[c] = slate[c].astype(str).str.strip() 

    # now safe to merge
        
    feat_cols = ["date","game_id","team","opponent","player_id","name"] + PLAYER_FEATURES
    player_rows = slate.merge(df_feat[feat_cols].drop_duplicates(), on=["date","game_id","team","opponent","player_id","name"], how="left").fillna(0)

    preds_points  = predict_player_counts(bundle_points,  player_rows, run_id, target="points")
    preds_goals   = predict_player_counts(bundle_goals,   player_rows, run_id, target="goals")
    preds_assists = predict_player_counts(bundle_assists, player_rows, run_id, target="assists")
    preds_shots   = predict_player_counts(bundle_shots,   player_rows, run_id, target="shots_on_goal")

    keys = ["date","game_id","team","opponent","home_or_away"]

    team_target = (
        df_feat.groupby(keys, as_index=False)["points"]
               .sum()
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
                   "opp_goalie_ga_smooth": "mean", # if THIS is your name, use this instead (don’t have both)
               })
    )

    # IMPORTANT: ensure you have only one goalie feature name; pick ONE and use it everywhere.
    # e.g., use "opp_goalie_ga_smooth" consistently:
    team_feats = team_feats.drop(columns=[c for c in ["goal_tending_goals_against"] if c in team_feats])

    team_df = team_target.merge(team_feats, on=keys, how="left")

    # the rows you pass forward can keep identifiers + features (no duplicates)
    team_rows = team_df[keys + TEAM_FEATURES].drop_duplicates()

    missing = [c for c in TEAM_FEATURES if c not in team_rows.columns]
    if missing:
        raise KeyError(f"Missing {missing} in team_rows (predict)")

    preds_totals = predict_match_totals(bundle_team_home, bundle_team_away, team_rows, run_id)


    all_preds = pd.concat([preds_points, preds_goals, preds_assists, preds_shots, preds_totals], ignore_index=True)
    append("fact_predictions", all_preds)

    os.makedirs(settings.PARQUET_DIR, exist_ok=True)
    out_csv = f"{settings.PARQUET_DIR}/predictions_{run_id}.csv"
    all_preds.to_csv(out_csv, index=False)
    typer.echo(f"Wrote predictions → {out_csv}\nRun ID: {run_id}")
