from __future__ import annotations
import numpy as np
import pandas as pd
from dataclasses import dataclass, field
from typing import Any
from lightgbm import LGBMRegressor


@dataclass
class ModelBundle:
    model: Any
    features: list[str]
    target: str
    model_name: str
    model_version: str
    sport: str = "nhl"
    # Populated by trainers with holdout scores and other diagnostics.
    # Passed through to the sidecar .meta.json by save_model().
    extra_meta: dict = field(default_factory=dict)


def _holdout_eval_lgbm(
    df: pd.DataFrame,
    features: list[str],
    target: str,
    tweedie_variance_power: float,
    holdout_frac: float = 0.20,
) -> dict:
    """Time-based holdout: train on first (1-frac) of dates, score on last frac.

    Returns a dict of validation metrics to include in the sidecar.
    """
    if "date" not in df.columns or len(df) < 20:
        return {}

    dates = pd.to_datetime(df["date"], errors="coerce")
    cutoff = dates.quantile(1.0 - holdout_frac)
    train_mask = dates <= cutoff
    val_mask = dates > cutoff

    if train_mask.sum() < 10 or val_mask.sum() < 5:
        return {}

    X_tr = df.loc[train_mask, features].fillna(0)
    y_tr = df.loc[train_mask, target].astype(float)
    X_val = df.loc[val_mask, features].fillna(0)
    y_val = df.loc[val_mask, target].astype(float)

    m = LGBMRegressor(
        objective="tweedie",
        tweedie_variance_power=tweedie_variance_power,
        n_estimators=200,
        learning_rate=0.05,
        min_data_in_leaf=10,
        verbose=-1,
    )
    m.fit(X_tr, y_tr)
    preds = m.predict(X_val)

    rmse = float(np.sqrt(np.mean((y_val.values - preds) ** 2)))
    mae = float(np.mean(np.abs(y_val.values - preds)))

    return {
        "holdout_frac": holdout_frac,
        "holdout_cutoff_date": str(cutoff.date()),
        "holdout_n_val": int(val_mask.sum()),
        "holdout_rmse": round(rmse, 4),
        "holdout_mae": round(mae, 4),
    }


def train_player_count(
    df: pd.DataFrame,
    features: list[str],
    target: str,
    sample_weight=None,
    version: str = "0.3.0",
    adapter=None,
) -> ModelBundle:
    from ..sports.nhl import NHL_ADAPTER
    adp = adapter if adapter is not None else NHL_ADAPTER

    X = df[features].fillna(0)
    y = df[target].astype(float)

    holdout_meta = _holdout_eval_lgbm(
        df, features, target,
        tweedie_variance_power=adp.tweedie_variance_power,
    )

    m = LGBMRegressor(
        objective="tweedie",
        tweedie_variance_power=adp.tweedie_variance_power,
        n_estimators=400,
        learning_rate=0.03,
        max_depth=-1,
        subsample=0.8,
        colsample_bytree=0.8,
        min_data_in_leaf=20,
    )
    m.fit(X, y, sample_weight=sample_weight)

    return ModelBundle(
        model=m,
        features=features,
        target=target,
        model_name=f"lgbm_tweedie_{target}",
        model_version=version,
        sport=adp.sport,
        extra_meta=holdout_meta,
    )


def train_team_goals(
    df_team: pd.DataFrame,
    features: list[str],
    target: str = "team_goals",
    sample_weight=None,
    version: str = "0.3.0",
    adapter=None,
) -> ModelBundle:
    from ..sports.nhl import NHL_ADAPTER
    adp = adapter if adapter is not None else NHL_ADAPTER

    X = df_team[features].fillna(0)
    y = df_team[target].astype(float)

    m = LGBMRegressor(
        objective="poisson",
        n_estimators=300,
        learning_rate=0.05,
    )
    m.fit(X, y, sample_weight=sample_weight)

    return ModelBundle(
        model=m,
        features=features,
        target=target,
        model_name="lgbm_poisson_team_goals",
        model_version=version,
        sport=adp.sport,
    )
