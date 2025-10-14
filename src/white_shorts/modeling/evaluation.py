from __future__ import annotations
import pandas as pd
import numpy as np

# Very light metrics scaffold. Extend with CRPS, calibration, etc.
def rmse(y_true: pd.Series, y_pred: pd.Series) -> float:
    return float(np.sqrt(np.mean((y_true - y_pred) ** 2)))

def brier_score(y_true_events: pd.Series, p_event: pd.Series) -> float:
    # For simple (>=1) event vs probability
    y = (y_true_events >= 1).astype(int)
    p = p_event.clip(0, 1)
    return float(np.mean((p - y) ** 2))

def coverage(y_true: pd.Series, q10: pd.Series, q90: pd.Series) -> float:
    inside = (y_true >= q10) & (y_true <= q90)
    return float(inside.mean())
