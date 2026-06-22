from __future__ import annotations
from dataclasses import dataclass, field
from typing import List

import numpy as np
import pandas as pd
import typer
from sklearn.ensemble import RandomForestRegressor


@dataclass
class ModelBundle:
    model: object
    features: List[str]
    target: str
    model_name: str
    model_version: str
    sport: str = "nhl"
    extra_meta: dict = field(default_factory=dict)


def _holdout_eval_qrf(
    df: pd.DataFrame,
    features: List[str],
    target: str,
    holdout_frac: float = 0.20,
    q_low: float = 0.10,
    q_high: float = 0.90,
) -> dict:
    """Time-based holdout for QRF: reports RMSE + empirical interval coverage."""
    if "date" not in df.columns or len(df) < 20:
        return {}

    dates = pd.to_datetime(df["date"], errors="coerce")
    cutoff = dates.quantile(1.0 - holdout_frac)
    train_mask = dates <= cutoff
    val_mask = dates > cutoff

    if train_mask.sum() < 10 or val_mask.sum() < 5:
        return {}

    avail = [f for f in features if f in df.columns]
    X_tr = df.loc[train_mask, avail].fillna(0)
    y_tr = pd.to_numeric(df.loc[train_mask, target], errors="coerce").fillna(0.0)
    X_val = df.loc[val_mask, avail].fillna(0)
    y_val = pd.to_numeric(df.loc[val_mask, target], errors="coerce").fillna(0.0)

    rf = RandomForestRegressor(
        n_estimators=200,
        min_samples_leaf=5,
        random_state=42,
        n_jobs=-1,
    )
    rf.fit(X_tr, y_tr)

    tree_preds = np.stack([t.predict(X_val.to_numpy()) for t in rf.estimators_], axis=1)
    mean_preds = tree_preds.mean(axis=1)
    q_lo_preds = np.quantile(tree_preds, q_low, axis=1)
    q_hi_preds = np.quantile(tree_preds, q_high, axis=1)

    y_arr = y_val.values
    rmse = float(np.sqrt(np.mean((y_arr - mean_preds) ** 2)))
    mae = float(np.mean(np.abs(y_arr - mean_preds)))
    coverage = float(np.mean((y_arr >= q_lo_preds) & (y_arr <= q_hi_preds)))

    return {
        "holdout_frac": holdout_frac,
        "holdout_cutoff_date": str(cutoff.date()),
        "holdout_n_val": int(val_mask.sum()),
        "holdout_rmse": round(rmse, 4),
        "holdout_mae": round(mae, 4),
        f"holdout_coverage_{int(q_low*100)}_{int(q_high*100)}": round(coverage, 4),
    }


def train_player_qrf(
    df: pd.DataFrame,
    features: list[str],
    target: str,
    version: str = "0.3.0",
    adapter=None,
    sample_weight=None,
) -> ModelBundle:
    from ..sports.nhl import NHL_ADAPTER
    adp = adapter if adapter is not None else NHL_ADAPTER

    available = [f for f in features if f in df.columns]
    missing = sorted(set(features) - set(available))

    if missing:
        typer.echo(
            f"[WARN] Missing feature columns in train_player_qrf for target={target}: {missing}",
            err=True,
        )

    if not available:
        raise ValueError(f"No usable feature columns for target={target}")

    holdout_meta = _holdout_eval_qrf(df, available, target)

    X = df[available].fillna(0)
    y_series = pd.to_numeric(df[target], errors="coerce")
    mask = y_series.notna()
    X = X.loc[mask]
    y = y_series.loc[mask].astype(float)

    sw = None
    if sample_weight is not None:
        sw_series = pd.Series(sample_weight, index=df.index)
        sw = sw_series.loc[mask].values

    rf = RandomForestRegressor(
        n_estimators=600,
        max_depth=None,
        min_samples_leaf=2,
        random_state=42,
        n_jobs=-1,
        bootstrap=True,
    )
    rf.fit(X, y, sample_weight=sw)

    return ModelBundle(
        model=rf,
        features=available,
        target=target,
        model_name=f"rf_qrf_{target}",
        model_version=version,
        sport=adp.sport,
        extra_meta=holdout_meta,
    )


def qrf_predict_with_quantiles(
    bundle: ModelBundle,
    X: pd.DataFrame,
    q_low: float = 0.10,
    q_high: float = 0.90,
):
    X_arr = X[bundle.features].fillna(0).to_numpy()
    est = np.stack([t.predict(X_arr) for t in bundle.model.estimators_], axis=1)
    mean = est.mean(axis=1)
    q10 = np.quantile(est, q_low, axis=1)
    q90 = np.quantile(est, q_high, axis=1)
    return mean, q10, q90
