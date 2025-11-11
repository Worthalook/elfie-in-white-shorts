# processors.py
import numpy as np
import pandas as pd
import re
from typing import Dict, List

def _snake(s: str) -> str:
    return re.sub(r'[^0-9a-zA-Z]+', '_', s).strip('_').lower()

def normalize_columns(df: pd.DataFrame, rename_map: Dict[str,str]) -> pd.DataFrame:
    df = df.rename(columns=rename_map or {})
    df.columns = [_snake(c) for c in df.columns]
    return df

def coerce_types(df: pd.DataFrame) -> pd.DataFrame:
    for c in df.columns:
        try:
            converted = pd.to_numeric(df[c])
            # keep only if we didn't nuke most of the column
            if converted.notna().mean() >= 0.8:
                df[c] = converted
        except Exception:
            pass
    return df

def _nullify_non_finite(df: pd.DataFrame) -> pd.DataFrame:
    # Make a copy to avoid chained assignment surprises
    df = df.copy()
    for c in df.columns:
        # Replace pandas NA with None
        df[c] = df[c].where(pd.notna(df[c]), None)
        # For numeric columns, also replace inf/-inf with None
        if pd.api.types.is_numeric_dtype(df[c]):
            df.loc[np.isinf(df[c].astype(float)), c] = None
    return df

def drop_missing_required(df: pd.DataFrame, required_cols: List[str]) -> pd.DataFrame:
    return df.dropna(subset=required_cols) if required_cols else df

def default_pipeline(df: pd.DataFrame, cfg) -> list[dict]:
    df2 = df.copy()
    df2 = normalize_columns(df2, cfg.rename_map)
    df2 = coerce_types(df2)
    df2 = _nullify_non_finite(df2)     # <-- ADD THIS LINE
    df2 = drop_missing_required(df2, cfg.required_cols)
    for fn in cfg.processors:
        df2 = fn(df2)
    return df2.to_dict(orient="records")
