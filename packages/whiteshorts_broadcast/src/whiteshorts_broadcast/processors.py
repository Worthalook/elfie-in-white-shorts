# processors.py
import re
import math
import numpy as np
import pandas as pd
from typing import Dict, List

def _snake(s: str) -> str:
    return re.sub(r'[^0-9a-zA-Z]+', '_', s).strip('_').lower()

def normalize_columns(df: pd.DataFrame, rename_map: Dict[str,str]) -> pd.DataFrame:
    df = df.rename(columns=rename_map or {})
    df.columns = [_snake(c) for c in df.columns]
    return df



def coerce_types(df: pd.DataFrame) -> pd.DataFrame:
    """Light coercion: try numeric where sensible; keep strings otherwise."""
    for c in df.columns:
        # Try numeric; only keep if we didn’t blow away most values
        try:
            converted = pd.to_numeric(df[c], errors="coerce")
            if converted.notna().mean() >= 0.8:
                df[c] = converted
        except Exception:
            pass
    return df

def normalize_dates(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure date-like columns (e.g., game_date) are ISO yyyy-mm-dd strings."""
    for col in df.columns:
        if "date" in col:
            try:
                ser = pd.to_datetime(df[col], errors="coerce", utc=False).dt.date
                df[col] = ser.astype(str)  # 'YYYY-MM-DD' or 'NaT'→'NaT'
                df.loc[df[col].isin(["NaT", "nat", "None"]), col] = None
            except Exception:
                pass
    return df

def nullify_non_finite(df: pd.DataFrame) -> pd.DataFrame:
    """Replace NaN/±Inf with None so JSON is compliant."""
    df = df.copy()
    # First, uniform missing values
    df = df.where(pd.notna(df), None)

    for c in df.columns:
        # If column is numeric or mostly numeric, check for inf
        try:
            ser = pd.to_numeric(df[c], errors="coerce")
            mask_inf = ser.map(lambda x: (isinstance(x, float) and (math.isinf(x))), na_action='ignore')
            if mask_inf.any():
                df.loc[mask_inf, c] = None
        except Exception:
            # If not numeric, nothing to do
            pass
    return df

def drop_missing_required(df: pd.DataFrame, required_cols: List[str]) -> pd.DataFrame:
    return df.dropna(subset=required_cols) if required_cols else df

def filter_columns_by_range(df: pd.DataFrame, range_map: dict[str, tuple[float, float]]) -> pd.DataFrame:
    """
    Remove rows where any specified column value falls outside its (lo, hi) range.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame.
    range_map : dict[str, tuple[float, float]]
        Example: {"pred_mean": (0, 10), "pred_q10": (0, 10), "pred_q90": (0, 10)}

    Returns
    -------
    pd.DataFrame
        Filtered DataFrame containing only rows that fall within all provided ranges.
    """
    import pandas as pd
    import numpy as np

    if not range_map:
        return df

    mask = pd.Series(True, index=df.index)
    for col, (lo, hi) in range_map.items():
        if col not in df.columns:
            continue
        try:
            vals = pd.to_numeric(df[col], errors="coerce")
            # keep only rows within [lo, hi]
            within = vals.ge(lo) & vals.le(hi)
            # NaN values are treated as out-of-range → False
            within = within & vals.notna()
            mask &= within
        except Exception as e:
            print(f"[filter_columns_by_range] Skipping {col}: {e}")

    filtered = df.loc[mask].reset_index(drop=True)
    return filtered


# add to processors.py, called inside default_pipeline before nullify_non_finite
def clip_columns(df: pd.DataFrame, clip_map: dict[str, tuple[float,float]]) -> pd.DataFrame:
    # clip_map example: {"pred_mean": (0, 10), "pred_q10": (0, 10), "pred_q90": (0, 10)}
    for col, (lo, hi) in (clip_map or {}).items():
        if col in df:
            try:
                df[col] = pd.to_numeric(df[col], errors="coerce").clip(lower=lo, upper=hi)
            except Exception:
                pass
    return df
 
def add_elfies_number(
    df: pd.DataFrame,
    *,
    pred_col: str = "lambda_or_mu",
    q10_col: str = "q10",
    q90_col: str = "q90",
    out_col: str = "elfies_number",
) -> pd.DataFrame:
    """
    Compute elfies_number = prediction / (1 + (q90 - q10)).
    Fully robust against NaN, Inf, division-by-zero, and single-row inputs.
    """
    import numpy as np
    import pandas as pd

    df2 = df.copy()

    # --- Coerce to numeric and always wrap as Series ---
    p = pd.Series(pd.to_numeric(df2.get(pred_col, np.nan), errors="coerce"), index=df2.index)
    q10 = pd.Series(pd.to_numeric(df2.get(q10_col, np.nan), errors="coerce"), index=df2.index)
    q90 = pd.Series(pd.to_numeric(df2.get(q90_col, np.nan), errors="coerce"), index=df2.index)

    # --- Compute denominator safely ---
    denom = 1.0 + (q90 - q10)
    denom = pd.Series(denom, index=df2.index)
    denom = denom.where(np.isfinite(denom), np.nan)

    # --- Boolean mask: safe even on single row ---
    valid = (p.notna()) & (denom.notna()) & (denom > 0)

    # --- Compute elfies_number ---
    elfies = pd.Series(np.nan, index=df2.index, dtype="float64")
    if valid.any():
        elfies.loc[valid] = (p[valid] / denom[valid]).astype("float64")

    # --- Clean up infinities ---
    elfies = elfies.where(np.isfinite(elfies), np.nan)
    df2[out_col] = elfies

    return df2





def top_k_per_team_by_score(
    df: pd.DataFrame,
    *,
    team_col: str = "team",
    score_col: str = "elfies_number",
    top_k: int = 4,
    # If True, include all rows tied at the boundary (may exceed top_k)
    keep_ties: bool = False,
) -> pd.DataFrame:
    """
    Sort by score desc and keep top_k rows per team. Returns a new DataFrame
    sorted by (team, score desc, then original order as tiebreaker).
    """
    if team_col not in df.columns or score_col not in df.columns:
        # Nothing to do if columns are missing
        return df.copy()

    work = df.copy()
    # Make sure score is numeric
    work[score_col] = pd.to_numeric(work[score_col], errors="coerce")
    # Stable sort: highest score first; keep original index for tie-breaks
    work["_orig_idx"] = np.arange(len(work))
    work = work.sort_values([team_col, score_col, "_orig_idx"],
                            ascending=[True, False, True],
                            kind="mergesort")

    if not keep_ties:
        out = (
            work.groupby(team_col, group_keys=False)
                .head(max(int(top_k), 0))
                .drop(columns=["_orig_idx"])
        )
        return out

    # keep_ties=True → keep everyone whose score equals the kth score
    def _take_with_ties(g: pd.DataFrame) -> pd.DataFrame:
        if top_k <= 0 or g.empty:
            return g.iloc[0:0]
        # g is already sorted desc by score
        boundary = g.iloc[min(len(g), top_k) - 1][score_col]
        mask = g[score_col] >= boundary
        return g.loc[mask]

    out = (
        work.groupby(team_col, group_keys=False)
            .apply(_take_with_ties)
            .drop(columns=["_orig_idx"])
            .reset_index(drop=True)
    )
    return out


def apply_elfies_topk_pipeline(
    df: pd.DataFrame,
    *,
    pred_col: str = "pred_mean",
    q10_col: str = "q10",
    q90_col: str = "q90",
    team_col: str = "team",
    out_col: str = "elfies_number",
    top_k: int = 4,
    keep_ties: bool = False,
) -> pd.DataFrame:
    """
    Convenience wrapper that:
      1) computes elfies_number
      2) sorts by it (desc within team) and takes top_k per team
    Intended to be called AFTER clip_columns().
    """
    df2 = add_elfies_number(
        df,
        pred_col=pred_col,
        q10_col=q10_col,
        q90_col=q90_col,
        out_col=out_col,
    )
    df3 = top_k_per_team_by_score(
        df2,
        team_col=team_col,
        score_col=out_col,
        top_k=top_k,
        keep_ties=keep_ties,
    )
    return df3

def default_pipeline(df: pd.DataFrame, cfg) -> list[dict]:
    df2 = df.copy()
    df2 = normalize_columns(df2, cfg.rename_map)
    df2 = coerce_types(df2)
    df2 = normalize_dates(df2)
    df2 = filter_columns_by_range(df2, {"lambda_or_mu": (0.5, None), "q10": (0.01, None), "q90": (0.5, None)})
    df2 = apply_elfies_topk_pipeline(
        df2,
        pred_col=getattr(cfg, "pred_col", "lambda_or_mu"),
        q10_col=getattr(cfg, "q10_col", "q10"),
        q90_col=getattr(cfg, "q90_col", "q90"),
        team_col=getattr(cfg, "team_col", "team"),
        out_col=getattr(cfg, "elfies_out_col", "elfies_number"),
        top_k=getattr(cfg, "elfies_top_k", 4),
        keep_ties=getattr(cfg, "elfies_keep_ties", False),
    )
    df2 = filter_columns_by_range(df2, {"elfies_number": (0.05, None), "q10": (0.05, None), "q90": (0.5, None)})
    df2 = nullify_non_finite(df2)     # <- critical for JSON
    df2 = drop_missing_required(df2, cfg.required_cols)
    
    for fn in cfg.processors:
        df2 = fn(df2)
    return df2.to_dict(orient="records")
