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
    prefix: str = "",
) -> dict:
    """Selection-quality metrics: how does top-N by score_col perform vs the field?

    Primary question: when I pick the top N players, do I get better hit rates
    and higher average actual points than the field (all players that day)?

    Returns a flat dict of metrics. Field baselines are only included when
    prefix is empty (they are shared across all variants). Selection metrics
    are prefixed with `prefix` so multiple variants can coexist in one dict.
    """
    out: dict = {}
    scored = df[[score_col, "actual"]].dropna()
    if len(scored) < 3:
        return out

    actuals = scored["actual"]
    field_hit1 = float((actuals >= 1.0).mean())
    field_hit2 = float((actuals >= 2.0).mean())
    field_avg  = float(actuals.mean())

    # Field baselines only once (when no prefix — the primary variant call)
    if not prefix:
        out["field_hit1_rate"]  = round(field_hit1, 4)
        out["field_hit2_rate"]  = round(field_hit2, 4)
        out["field_avg_actual"] = round(field_avg,  4)
        out["n_field"]          = int(len(scored))

    ranked = scored.sort_values(score_col, ascending=False)
    for n in top_ns:
        if len(ranked) < n:
            continue
        top  = ranked.head(n)["actual"]
        hit1 = float((top >= 1.0).mean())
        hit2 = float((top >= 2.0).mean())
        avg  = float(top.mean())
        p = prefix
        out[f"{p}top_{n}_hit1_rate"]  = round(hit1, 4)
        out[f"{p}top_{n}_hit2_rate"]  = round(hit2, 4)
        out[f"{p}top_{n}_avg_actual"] = round(avg,  4)
        out[f"{p}top_{n}_lift_hit1"]  = round(hit1 / field_hit1, 4) if field_hit1 > 0 else None
        out[f"{p}top_{n}_lift_hit2"]  = round(hit2 / field_hit2, 4) if field_hit2 > 0 else None
        out[f"{p}top_{n}_lift_avg"]   = round(avg  / field_avg,  4) if field_avg  > 0 else None

    return out


def _bucket_metrics(
    df: pd.DataFrame,
    score_col: str,
    baseline_col: str = "player_baseline",
    top_n: int = 10,
) -> dict:
    """Selection lift broken down by player baseline scoring bucket.

    Buckets (pts/game historical average):
      rare       < 0.30  — role players, occasional contributors
      occasional 0.30–0.60
      reliable   0.60–1.00
      star       > 1.00

    For each bucket: what fraction of the field are they, and does the
    formula preferentially select them when they're in-form?
    """
    out: dict = {}
    if baseline_col not in df.columns or score_col not in df.columns:
        return out

    needed = [score_col, "actual", baseline_col]
    scored = df[needed].dropna()
    if len(scored) < 5:
        return out

    buckets = {
        "rare":       scored[scored[baseline_col] < 0.30],
        "occasional": scored[(scored[baseline_col] >= 0.30) & (scored[baseline_col] < 0.60)],
        "reliable":   scored[(scored[baseline_col] >= 0.60) & (scored[baseline_col] < 1.00)],
        "star":       scored[scored[baseline_col] >= 1.00],
    }

    # Top-N by score across the full slate
    top_selected = scored.sort_values(score_col, ascending=False).head(top_n)

    field_hit1 = float((scored["actual"] >= 1.0).mean())

    for bname, bdf in buckets.items():
        if bdf.empty:
            continue
        pct_field = round(len(bdf) / len(scored), 3)
        # How many of the top-N come from this bucket?
        in_top = top_selected[top_selected[baseline_col].isin(bdf[baseline_col])]
        pct_selected = round(len(in_top) / top_n, 3) if top_n > 0 else None
        bucket_hit1  = round(float((bdf["actual"] >= 1.0).mean()), 4) if len(bdf) > 0 else None
        out[f"bkt_{bname}_pct_field"]    = pct_field
        out[f"bkt_{bname}_pct_top{top_n}"] = pct_selected
        out[f"bkt_{bname}_hit1_rate"]    = bucket_hit1

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

    # --- Primary: v2 selection quality (field baselines included here) ---
    if primary in df.columns:
        out.update(_selection_metrics(df, score_col=primary))

    # --- Variant comparison: v1 and standout, prefixed so they sit alongside ---
    for variant, col in [("v1_", "elfies_v1"), ("std_", "elfies_standout")]:
        if col in df.columns:
            out.update(_selection_metrics(df, score_col=col, prefix=variant))

    # --- Player baseline bucket analysis ---
    if primary in df.columns:
        out.update(_bucket_metrics(df, score_col=primary))

    # --- Calibration: interval coverage ---
    if "in_interval" in df.columns:
        out["calibration_80"] = round(float(df["in_interval"].mean()), 4)

    # --- MAE: raw prediction error ---
    if "abs_error" in df.columns:
        out["mae"] = round(float(df["abs_error"].mean()), 4)

    # --- Spearman: diagnostic only ---
    if _HAS_SCIPY:
        for var in (primary, "elfies_v1", "elfies_standout"):
            if var in df.columns:
                valid = df[[var, "actual"]].dropna()
                if len(valid) >= 5:
                    rho, _ = spearmanr(valid[var], valid["actual"])
                    out[f"spearman_{var}"] = round(float(rho), 4)

    return out
