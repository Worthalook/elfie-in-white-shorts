# processors.py
import re
import math
import os,glob
import numpy as np
import pandas as pd
from typing import Dict, List

def _snake(s: str) -> str:
    return re.sub(r'[^0-9a-zA-Z]+', '_', s).strip('_').lower()

def normalize_columns(df: pd.DataFrame, rename_map: Dict[str,str]) -> pd.DataFrame:
    df = df.rename(columns=rename_map or {})
    df.columns = [_snake(c) for c in df.columns]
    return df

def find_latest_csv(pattern: str):
    files = sorted(glob.glob(pattern), key=os.path.getmtime, reverse=True)
    return files[0] if files else None

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
    
def default_pipeline(df: pd.DataFrame, cfg) -> list[dict]:
    df2 = df.copy()
    df2 = normalize_columns(df2, cfg.rename_map)
    df2 = coerce_types(df2)
    df2 = normalize_dates(df2)
    df2 = clip_columns(df2, {"pred_mean": (0.5, 10), "pred_q10": (0, 10), "pred_q90": (0.5, 10)})
    df2 = nullify_non_finite(df2)     # <- critical for JSON
    df2 = drop_missing_required(df2, cfg.required_cols)
    for fn in cfg.processors:
        df2 = fn(df2)
    return df2.to_dict(orient="records")
