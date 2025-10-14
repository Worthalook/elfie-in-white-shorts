from __future__ import annotations
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

def train_player_count(df: pd.DataFrame, features: list[str], target: str, sample_weight: pd.Series | None = None,
                       version: str = "0.3.0") -> ModelBundle:
    X = df[features].fillna(0)
    y = df[target].astype(float)
    m = LGBMRegressor(objective="poisson", n_estimators=300, learning_rate=0.05, max_depth=-1)
    m.fit(X, y, sample_weight=sample_weight)
    return ModelBundle(model=m, features=features, target=target, model_name=f"lgbm_poisson_{target}", model_version=version)

# Placeholder for match totals — minimal approach: predict team goals and sum per game
# TODO: Improve with Tweedie/Skellam ensemble
def train_team_goals(df_team: pd.DataFrame, features: list[str], target: str = "team_goals",
                     sample_weight: pd.Series | None = None, version: str = "0.3.0") -> ModelBundle:
    X = df_team[features].fillna(0)
    y = df_team[target].astype(float)
    m = LGBMRegressor(objective="poisson", n_estimators=300, learning_rate=0.05)
    m.fit(X, y, sample_weight=sample_weight)
    return ModelBundle(model=m, features=features, target=target, model_name="lgbm_poisson_team_goals", model_version=version)
