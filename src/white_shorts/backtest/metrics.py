"""Elfies formula variants and backtest quality metrics.

All functions accept a DataFrame with at minimum: lambda_or_mu, q10, q90.
The `compute_day_metrics` function additionally requires an `actual` column.
"""
from __future__ import annotations
import numpy as np
import pandas as pd

try:
    from scipy.stats import spearmanr
    _HAS_SCIPY = True
except ImportError:
    _HAS_SCIPY = False


def _spread(df: pd.DataFrame) -> pd.Series:
    return (df["q90"] - df["q10"]).clip(lower=0)


def elfies_v1(df: pd.DataFrame) -> pd.Series:
    """Current production formula: λ / (1 + spread)."""
    return df["lambda_or_mu"] / (1 + _spread(df))


def elfies_v2(df: pd.DataFrame) -> pd.Series:
    """Probability-weighted: λ × P(score ≥ 1).

    Uses p_ge_1 column if present (from QRF tree distribution).
    Falls back to Poisson approximation: 1 − e^{−λ}.
    """
    if "p_ge_1" in df.columns:
        p1 = pd.to_numeric(df["p_ge_1"], errors="coerce").fillna(0).clip(0, 1)
    else:
        lam = df["lambda_or_mu"].clip(lower=0)
        p1 = 1.0 - np.exp(-lam)
    return df["lambda_or_mu"] * p1


def elfies_v3(df: pd.DataFrame) -> pd.Series:
    """Normalised upside: (λ − q10) / spread.

    Measures how much the prediction exceeds the lower bound,
    normalised by uncertainty width.
    """
    sp = _spread(df).clip(lower=0.01)
    return (df["lambda_or_mu"] - df["q10"]) / sp


def elfies_v4(df: pd.DataFrame, alpha: float = 1.0, beta: float = 1.0) -> pd.Series:
    """Tunable exponents: λ^α / (1 + spread)^β.

    α > 1 rewards higher predictions more aggressively.
    β > 1 penalises uncertainty more aggressively.
    Start sweep with α ∈ [0.8, 1.5], β ∈ [0.5, 1.5].
    """
    lam = df["lambda_or_mu"].clip(lower=0)
    return (lam ** alpha) / ((1 + _spread(df)) ** beta)


def elfies_standout(df: pd.DataFrame) -> pd.Series:
    """v1 divided by player's rolling historical baseline.

    Normalises for player scoring frequency: a reliable 0.4pt/game player
    who is predicted at 0.6 is more 'standing out' than a 1.5pt/game star
    predicted at the same 0.6.
    """
    baseline = df["player_baseline"].clip(lower=0.05)
    return elfies_v1(df) / baseline


def add_all_elfies_variants(
    df: pd.DataFrame,
    alpha: float = 1.0,
    beta: float = 1.0,
) -> pd.DataFrame:
    """Compute all elfies variant columns and return a copy."""
    out = df.copy()
    out["elfies_v1"]       = elfies_v1(out)
    out["elfies_v2"]       = elfies_v2(out)
    out["elfies_v3"]       = elfies_v3(out)
    out["elfies_v4"]       = elfies_v4(out, alpha=alpha, beta=beta)
    out["elfies_standout"] = (
        elfies_standout(out) if "player_baseline" in out.columns else np.nan
    )
    return out


def compute_day_metrics(df: pd.DataFrame) -> dict:
    """All metrics for one backtest date.

    Requires 'actual' column. Returns a flat dict suitable for a summary CSV row.
    """
    out: dict = {"n_players": len(df)}
    if "actual" not in df.columns or df["actual"].isna().all():
        return out

    # Spearman rank correlation: each elfies variant vs actuals
    if _HAS_SCIPY:
        for var in ("elfies_v1", "elfies_v2", "elfies_v3", "elfies_v4", "elfies_standout"):
            if var in df.columns:
                valid = df[[var, "actual"]].dropna()
                if len(valid) >= 5:
                    rho, pval = spearmanr(valid[var], valid["actual"])
                    out[f"spearman_{var}"] = round(float(rho), 4)
                    out[f"spearman_{var}_pval"] = round(float(pval), 4)
    else:
        out["spearman_warning"] = "scipy not installed"

    # Threshold hit rates (based on elfies_v1 for comparability)
    if "elfies_v1" in df.columns:
        for elfies_thresh, actual_thresh, label in [
            (0.7, 1.0, "hit1_at_07"),
            (1.5, 2.0, "hit2_at_15"),
        ]:
            subset = df[df["elfies_v1"] > elfies_thresh]
            if len(subset) > 0:
                out[label] = round(float((subset["actual"] >= actual_thresh).mean()), 4)
                out[f"n_{label}"] = int(len(subset))
            else:
                out[label] = None
                out[f"n_{label}"] = 0

    # Calibration: % of actuals within [q10, q90]  (target ≈ 0.80 for an 80% interval)
    if "in_interval" in df.columns:
        out["calibration_80"] = round(float(df["in_interval"].mean()), 4)

    # MAE on points prediction
    if "abs_error" in df.columns:
        out["mae"] = round(float(df["abs_error"].mean()), 4)

    return out
