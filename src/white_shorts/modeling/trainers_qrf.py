from __future__ import annotations
from dataclasses import dataclass
from typing import List
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestRegressor

@dataclass
class ModelBundle:
    model: object
    features: List[str]
    target: str
    model_name: str
    model_version: str

def train_player_qrf(df: pd.DataFrame, features: list[str], target: str, version: str = "0.3.0") -> ModelBundle:
    X = df[features].fillna(0)
    y = df[target].astype(float)
    rf = RandomForestRegressor(
        n_estimators=600,
        max_depth=None,
        min_samples_leaf=2,
        random_state=42,
        n_jobs=-1,
        bootstrap=True,
    )
    rf.fit(X, y)
    return ModelBundle(
        model=rf,
        features=features,
        target=target,
        model_name=f"rf_qrf_{target}",
        model_version=version,
    )

def qrf_predict_with_quantiles(bundle: ModelBundle, X: pd.DataFrame, q_low: float = 0.10, q_high: float = 0.90):
    X = X[bundle.features].fillna(0)
    est = np.stack([t.predict(X) for t in bundle.model.estimators_], axis=1)
    mean = est.mean(axis=1)
    q10 = np.quantile(est, q_low, axis=1)
    q90 = np.quantile(est, q_high, axis=1)
    return mean, q10, q90
