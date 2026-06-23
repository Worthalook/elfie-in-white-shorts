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


def _selection_metrics(
    df: pd.DataFrame,
    score_col: str,
    top_ns: tuple[int, ...] = (5, 10, 20),
) -> dict:
    """Selection-quality metrics: how does top-N by score_col perform vs the field?

    Primary question: when I pick the top N players, do I get better hit rates
    and higher average actual points than the field (all players that day)?

    Returns a flat dict of metrics, all keyed with the score_col name stripped
    to keep column names clean (e.g. 'elfies_v2' → 'v2').
    """
    out: dict = {}
    scored = df[[score_col, "actual"]].dropna()
    if len(scored) < 3:
        return out

    actuals = scored["actual"]

    # Field baselines (all players on this slate)
    field_hit1 = float((actuals >= 1.0).mean())
    field_hit2 = float((actuals >= 2.0).mean())
    field_avg  = float(actuals.mean())
    out["field_hit1_rate"]   = round(field_hit1, 4)
    out["field_hit2_rate"]   = round(field_hit2, 4)
    out["field_avg_actual"]  = round(field_avg, 4)
    out["n_field"]           = int(len(scored))

    # Top-N selection metrics
    ranked = scored.sort_values(score_col, ascending=False)
    for n in top_ns:
        if len(ranked) < n:
            continue
        top = ranked.head(n)["actual"]
        hit1 = float((top >= 1.0).mean())
        hit2 = float((top >= 2.0).mean())
        avg  = float(top.mean())
        out[f"top_{n}_hit1_rate"]  = round(hit1, 4)
        out[f"top_{n}_hit2_rate"]  = round(hit2, 4)
        out[f"top_{n}_avg_actual"] = round(avg,  4)
        # Lift: how much better than random field selection?
        out[f"top_{n}_lift_hit1"]  = round(hit1 / field_hit1, 4) if field_hit1 > 0 else None
        out[f"top_{n}_lift_hit2"]  = round(hit2 / field_hit2, 4) if field_hit2 > 0 else None
        out[f"top_{n}_lift_avg"]   = round(avg  / field_avg,  4) if field_avg  > 0 else None

    return out


def compute_day_metrics(df: pd.DataFrame, primary: str = "elfies_v2") -> dict:
    """All metrics for one backtest date.

    Primary metric: selection quality of the top-N players ranked by `primary`
    (default elfies_v2) vs the field. Spearman is retained as a diagnostic.

    Requires 'actual' column. Returns a flat dict suitable for a summary CSV row.
    """
    out: dict = {"n_players": len(df)}
    if "actual" not in df.columns or df["actual"].isna().all():
        return out

    # --- Primary: selection quality (the metric that matters for WhiteShorts) ---
    if primary in df.columns:
        out.update(_selection_metrics(df, score_col=primary))

    # --- Calibration: interval coverage ---
    if "in_interval" in df.columns:
        out["calibration_80"] = round(float(df["in_interval"].mean()), 4)

    # --- MAE: raw prediction error ---
    if "abs_error" in df.columns:
        out["mae"] = round(float(df["abs_error"].mean()), 4)

    # --- Spearman: kept as a diagnostic, not the target ---
    if _HAS_SCIPY:
        for var in (primary, "elfies_v1", "elfies_standout"):
            if var in df.columns:
                valid = df[[var, "actual"]].dropna()
                if len(valid) >= 5:
                    rho, _ = spearmanr(valid[var], valid["actual"])
                    out[f"spearman_{var}"] = round(float(rho), 4)

    return out
