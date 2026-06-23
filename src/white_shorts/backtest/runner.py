"""Single-date backtest runner.

Core flow:
  1. Build weighted training slice (last season + current season before bt_date)
  2. Engineer features
  3. Train QRF on points
  4. Slate = actual players who played on bt_date (from current season data)
  5. Predict λ, q10, q90, P(≥1) for each player
  6. Join to actuals
  7. Compute all elfies variants + metrics
"""
from __future__ import annotations
import numpy as np
import pandas as pd
from datetime import datetime

from ..features.engineer import engineer_minimal
from ..features.registry import PLAYER_FEATURES
from ..modeling.trainers_qrf import train_player_qrf, qrf_predict_with_quantiles
from ..config import settings
from .metrics import add_all_elfies_variants, compute_day_metrics


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _build_training_slice(
    df_last: pd.DataFrame,
    df_current: pd.DataFrame,
    bt_date: pd.Timestamp,
) -> pd.DataFrame:
    """Concat last-season + current-season rows before bt_date.

    Attaches a '_season' column so sample weights can be recovered after
    engineer_minimal() reorders rows. df_last may be empty — handled gracefully.
    """
    frames = []

    if df_last is not None and not df_last.empty:
        df_l = df_last.copy()
        df_l["_season"] = "last"
        frames.append(df_l)

    df_c = df_current[df_current["date"] < bt_date].copy()
    df_c["_season"] = "current"
    frames.append(df_c)

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _season_weights(df_feat: pd.DataFrame) -> np.ndarray | None:
    """Build a sample_weight array aligned to df_feat rows."""
    if "_season" not in df_feat.columns:
        return None
    last_w = settings.LAST_SEASON_SAMPLE_WEIGHT
    curr_w = settings.CURRENT_SEASON_SAMPLE_WEIGHT
    # If both weights are equal, skip weighting (no-op for the model)
    if abs(last_w - curr_w) < 1e-9:
        return None
    return np.where(df_feat["_season"] == "current", curr_w, last_w)


def _compute_player_baselines(df_feat: pd.DataFrame) -> pd.DataFrame:
    """Rolling mean of points per player over the training window."""
    return (
        df_feat.groupby("player_id", as_index=False)["points"]
        .mean()
        .rename(columns={"points": "player_baseline"})
    )


def _qrf_p_ge_1(bundle, X: pd.DataFrame) -> np.ndarray:
    """Empirical P(score ≥ 1) from QRF tree predictions."""
    X_arr = X[bundle.features].fillna(0).to_numpy()
    tree_preds = np.stack([t.predict(X_arr) for t in bundle.model.estimators_], axis=1)
    return (tree_preds >= 1.0).mean(axis=1)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def run_single_date(
    bt_date: str,
    df_last_season: pd.DataFrame,
    df_current_season: pd.DataFrame,
    bt_id: str,
    alpha: float = 1.0,
    beta: float = 1.0,
    version: str = "0.3.0",
) -> tuple[pd.DataFrame, dict]:
    """Run backtest for one date.

    Parameters
    ----------
    bt_date:           The date to predict/evaluate (YYYY-MM-DD).
    df_last_season:    Pre-loaded last-season CSV (e.g. NHL_2324.csv).
    df_current_season: Pre-loaded current-season CSV (e.g. NHL_2526.csv), full season.
    bt_id:             UUID identifying this backtest sweep.
    alpha, beta:       Elfies v2 exponent parameters (λ^α × P(≥1)^β).
    version:           Model version tag.

    Returns
    -------
    (results_df, summary_dict)
        results_df: one row per player with predictions, actuals, all metrics
        summary_dict: aggregated day metrics (for the progress log)
    """
    bt_ts = pd.Timestamp(bt_date).normalize()

    # --- 1. Build training slice -------------------------------------------
    df_train = _build_training_slice(df_last_season, df_current_season, bt_ts)

    if len(df_train) < 30:
        return pd.DataFrame(), {
            "error": f"only {len(df_train)} training rows (need ≥30)",
            "bt_date": str(bt_ts.date()),
        }

    # --- 2. Feature engineering -------------------------------------------
    df_feat = engineer_minimal(df_train)
    df_feat["player_id"] = df_feat["player_id"].astype(str)
    df_feat["date"] = pd.to_datetime(df_feat["date"], errors="coerce").dt.normalize()

    # --- 3. Player baselines from training window --------------------------
    baselines = _compute_player_baselines(df_feat)

    # --- 4. Train QRF on 'points' -----------------------------------------
    available = [f for f in PLAYER_FEATURES if f in df_feat.columns]
    sw = _season_weights(df_feat)

    bundle = train_player_qrf(
        df_feat, available, target="points",
        version=version, sample_weight=sw,
    )

    # --- 5. Build slate from current-season actuals -----------------------
    slate_cols = ["date", "game_id", "team", "opponent", "player_id", "name", "home_or_away"]
    avail_slate = [c for c in slate_cols if c in df_current_season.columns]
    slate = (
        df_current_season[df_current_season["date"] == bt_ts][avail_slate]
        .drop_duplicates(subset=["player_id", "team"])
        .reset_index(drop=True)
    )

    if slate.empty:
        return pd.DataFrame(), {"error": "no game data for date", "bt_date": str(bt_ts.date())}

    slate["player_id"] = slate["player_id"].astype(str)

    # --- 6. Build feature rows: last snapshot per player before bt_date ---
    # Exclude features already present in slate to avoid _x/_y column conflicts
    slate_cols_set = set(slate.columns)
    snap_extra = [f for f in available if f not in slate_cols_set]

    feat_snap = (
        df_feat[df_feat["date"] < bt_ts]
        .sort_values("date")
        .groupby(["player_id", "team"], as_index=False)
        .tail(1)[["player_id", "team"] + snap_extra]
    )
    player_rows = slate.merge(feat_snap, on=["player_id", "team"], how="left")
    # Fill any missing feature columns with zeros
    for f in available:
        if f not in player_rows.columns:
            player_rows[f] = 0.0
    player_rows[available] = player_rows[available].fillna(0.0)

    if player_rows.empty:
        return pd.DataFrame(), {"error": "no feature rows after merge", "bt_date": str(bt_ts.date())}

    # --- 7. Predict -------------------------------------------------------
    X = player_rows[bundle.features].fillna(0)
    mu, q10, q90 = qrf_predict_with_quantiles(bundle, X)
    p_ge_1 = _qrf_p_ge_1(bundle, X)

    id_cols = [c for c in ["date", "game_id", "team", "opponent", "player_id", "name"] if c in player_rows.columns]
    results = player_rows[id_cols].copy()
    results["target"]       = "points"
    results["lambda_or_mu"] = mu
    results["q10"]          = q10
    results["q90"]          = q90
    results["p_ge_1"]       = p_ge_1
    results["p_ge_k_json"]  = ""

    # --- 8. Join actuals ---------------------------------------------------
    actual_rows = (
        df_current_season[df_current_season["date"] == bt_ts]
        [["player_id", "team", "points"]]
        .rename(columns={"points": "actual"})
    )
    actual_rows["player_id"] = actual_rows["player_id"].astype(str)
    results = results.merge(actual_rows, on=["player_id", "team"], how="left")

    # --- 9. Player baseline -----------------------------------------------
    results = results.merge(baselines, on="player_id", how="left")
    results["player_baseline"] = results["player_baseline"].fillna(0.3)

    # --- 10. All elfies variants ------------------------------------------
    results = add_all_elfies_variants(results, alpha=alpha, beta=beta)

    # --- 11. Hit flags ----------------------------------------------------
    results["hit_1pt"]     = results["actual"] >= 1.0
    results["hit_2pt"]     = results["actual"] >= 2.0
    results["in_interval"] = (
        (results["actual"] >= results["q10"]) & (results["actual"] <= results["q90"])
    )
    results["abs_error"]   = (results["actual"] - results["lambda_or_mu"]).abs()

    # --- 12. BT metadata --------------------------------------------------
    results["bt_id"]         = bt_id
    results["bt_date"]       = bt_ts.date()
    results["alpha"]         = alpha
    results["beta"]          = beta
    results["season_weight"] = (
        f"last={settings.LAST_SEASON_SAMPLE_WEIGHT:.2f},"
        f"curr={settings.CURRENT_SEASON_SAMPLE_WEIGHT:.2f}"
    )
    results["created_ts"] = datetime.utcnow()

    # --- 13. Day summary --------------------------------------------------
    summary = {"bt_date": str(bt_ts.date()), "n_players": len(results)}
    summary.update(compute_day_metrics(results))

    return results, summary
