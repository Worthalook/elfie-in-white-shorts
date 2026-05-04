from __future__ import annotations
import numpy as np
import pandas as pd
from datetime import datetime
from .distributions import compute_quantiles, compute_p_ge_k

# Fallback clip ranges when no adapter is supplied (matches NHL defaults).
_DEFAULT_CLIPS: dict[str, tuple[float, float]] = {
    "points":        (0.0, 5.0),
    "goals":         (0.0, 5.0),
    "assists":       (0.0, 5.0),
    "shots_on_goal": (0.0, 15.0),
    "total_goals":   (0.0, 12.0),
}


def _normalize_target(val) -> str:
    if hasattr(val, "value"):
        val = val.value
    s = str(val)
    if s.startswith("Target."):
        s = s.split(".", 1)[1]
    return s.lower()


def _dist_for(target: str, adapter) -> str:
    """Look up the distribution name for *target* from the adapter."""
    if adapter is None:
        return "poisson"
    return adapter.target_distributions.get(str(target), "poisson")


def predict_player_counts(
    model_bundle,
    df_features: pd.DataFrame,
    run_id: str,
    target,
    adapter=None,
) -> pd.DataFrame:
    X = df_features[model_bundle.features].fillna(0)
    lam = model_bundle.model.predict(X)

    clips = adapter.clip_ranges if adapter is not None else _DEFAULT_CLIPS
    lo, hi = clips.get(str(model_bundle.target), (0.0, 99.0))
    lam = np.clip(lam, lo, hi)

    target_str = _normalize_target(target)
    dist = _dist_for(target_str, adapter)

    quantile_pairs = [compute_quantiles(float(l), dist) for l in lam]
    q10 = [pair[0] for pair in quantile_pairs]
    q90 = [pair[1] for pair in quantile_pairs]
    p_ge_k = [compute_p_ge_k(float(l), 10, dist) for l in lam]

    out = df_features[["date", "game_id", "team", "opponent", "player_id", "name"]].copy()
    out["target"] = target_str
    out["model_name"] = model_bundle.model_name
    out["model_version"] = model_bundle.model_version
    out["distribution"] = dist
    out["lambda_or_mu"] = lam
    out["q10"] = q10
    out["q90"] = q90
    out["p_ge_k_json"] = p_ge_k
    out["run_id"] = run_id
    out["created_ts"] = datetime.utcnow()
    return out


def predict_match_totals(
    home_bundle,
    away_bundle,
    df_match_rows: pd.DataFrame,
    run_id: str,
    adapter=None,
) -> pd.DataFrame:
    req = list(home_bundle.features)
    lam_h = home_bundle.model.predict(df_match_rows[req].copy())
    lam_a = away_bundle.model.predict(df_match_rows[req].copy())
    lam_total = lam_h + lam_a

    clips = adapter.clip_ranges if adapter is not None else _DEFAULT_CLIPS
    lo, hi = clips.get("total_goals", (0.0, 99.0))
    lam_total = np.clip(lam_total, lo, hi)

    dist = _dist_for("total_goals", adapter)

    quantile_pairs = [compute_quantiles(float(l), dist) for l in lam_total]
    q10 = [pair[0] for pair in quantile_pairs]
    q90 = [pair[1] for pair in quantile_pairs]
    p_ge_k = [compute_p_ge_k(float(l), 15, dist) for l in lam_total]

    out = df_match_rows[["date", "game_id", "team", "opponent"]].copy()
    out["player_id"] = None
    out["name"] = None
    out["target"] = "total_goals"
    out["model_name"] = "poisson_sum_team_goals"
    out["model_version"] = home_bundle.model_version
    out["distribution"] = dist
    out["lambda_or_mu"] = lam_total
    out["q10"] = q10
    out["q90"] = q90
    out["p_ge_k_json"] = p_ge_k
    out["run_id"] = run_id
    out["created_ts"] = pd.Timestamp.utcnow().to_pydatetime()
    return out
