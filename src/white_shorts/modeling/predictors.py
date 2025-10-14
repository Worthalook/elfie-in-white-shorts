from __future__ import annotations
import pandas as pd
from datetime import datetime
from .poisson import poisson_quantiles, p_ge_k_json

def predict_player_counts(model_bundle, df_features: pd.DataFrame, run_id: str, target: str) -> pd.DataFrame:
    X = df_features[model_bundle.features].fillna(0)
    lam = model_bundle.model.predict(X)
    q10, q90 = zip(*[poisson_quantiles(float(l)) for l in lam]) if len(lam) else ([], [])

    out = df_features[["date","game_id","team","opponent","player_id","name"]].copy()
    out["target"] = target
    out["model_name"] = model_bundle.model_name
    out["model_version"] = model_bundle.model_version
    out["distribution"] = "poisson"
    out["lambda_or_mu"] = lam
    out["q10"] = list(map(float, q10)) if q10 else []
    out["q90"] = list(map(float, q90)) if q90 else []
    out["p_ge_k_json"] = [p_ge_k_json(float(l), 10) for l in lam]
    out["run_id"] = run_id
    out["created_ts"] = datetime.utcnow()
    return out

def predict_match_totals(home_bundle, away_bundle, df_match_rows: pd.DataFrame, run_id: str) -> pd.DataFrame:
    # df_match_rows: one row per (game_id, team, opponent, home_or_away) with engineered team features
    Xh = df_match_rows[home_bundle.features].fillna(0)
    Xa = df_match_rows[away_bundle.features].fillna(0)
    lam_h = home_bundle.model.predict(Xh)
    lam_a = away_bundle.model.predict(Xa)
    lam_total = lam_h + lam_a

    q10, q90 = zip(*[poisson_quantiles(float(l)) for l in lam_total]) if len(lam_total) else ([], [])
    out = df_match_rows[["date","game_id","team","opponent"]].copy()
    out["player_id"] = None
    out["name"] = None
    out["target"] = "total_goals"
    out["model_name"] = "poisson_sum_team_goals"
    out["model_version"] = home_bundle.model_version
    out["distribution"] = "poisson"
    out["lambda_or_mu"] = lam_total
    out["q10"] = list(map(float, q10)) if q10 else []
    out["q90"] = list(map(float, q90)) if q90 else []
    out["p_ge_k_json"] = [p_ge_k_json(float(l), 15) for l in lam_total]
    out["run_id"] = run_id
    out["created_ts"] = pd.Timestamp.utcnow()
    return out
