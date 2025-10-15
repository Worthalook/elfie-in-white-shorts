
from __future__ import annotations
import os

import pandas as pd
from dataclasses import dataclass
from typing import Any
from lightgbm import LGBMRegressor

@dataclass
class ModelBundle:
    model: Any
    features: list[str]
    target: str
    model_name: str
    model_version: str
    
def train_player_count(df, features, target, sample_weight=None, version="0.3.0"):
    X = df[features].fillna(0)
    y = df[target].astype(float)

    # Tweedie is much more stable for low-count data
    m = LGBMRegressor(
        objective="tweedie",
        tweedie_variance_power=1.1,  # close to Poisson
        n_estimators=400,
        learning_rate=0.03,
        max_depth=-1,
        subsample=0.8,
        colsample_bytree=0.8,
        min_data_in_leaf=20
    )
    m.fit(X, y, sample_weight=sample_weight)

    return ModelBundle(
        model=m,
        features=features,
        target=target,
        model_name=f"lgbm_tweedie_{target}",
        model_version=version
    )

def train_team_goals(df_team, features, target="team_goals", sample_weight=None, version="0.3.0"):
    X = df_team[features].fillna(0)
    y = df_team[target].astype(float)

    m = LGBMRegressor(
        objective="poisson",     # totals still suit Poisson
        n_estimators=300,
        learning_rate=0.05
    )
    m.fit(X, y, sample_weight=sample_weight)

    return ModelBundle(
        model=m,
        features=features,
        target=target,
        model_name="lgbm_poisson_team_goals",
        model_version=version
    )
