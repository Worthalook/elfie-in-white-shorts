from __future__ import annotations
from dataclasses import dataclass
import numpy as np
import pandas as pd
try:
    from statsmodels.tsa.holtwinters import ExponentialSmoothing
    _SM_OK = True
except Exception:
    _SM_OK = False

@dataclass
class ETSTotals:
    team: str
    fitted: np.ndarray

def fit_team_ets(df_team_series: pd.DataFrame, team: str) -> ETSTotals:
    y = df_team_series["team_goals"].astype(float).values
    if len(y) < 3 or not _SM_OK:
        fitted = pd.Series(y).rolling(5, min_periods=1).mean().values
        return ETSTotals(team=team, fitted=fitted)
    model = ExponentialSmoothing(y, trend="add", damped_trend=True, seasonal=None)
    fit = model.fit(optimized=True)
    return ETSTotals(team=team, fitted=fit.fittedvalues)

def forecast_next(fit: ETSTotals) -> float:
    return float(fit.fitted[-1]) if len(fit.fitted) else float("nan")
