from __future__ import annotations
import pandas as pd
from pathlib import Path
from ..utils.validation import REQUIRED_YTD_COLUMNS, ensure_columns

def load_ytd(csv_path: str | Path) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    ensure_columns(df, REQUIRED_YTD_COLUMNS)

    # Normalise basic types
    df["date"] = pd.to_datetime(df["date"], dayfirst=True, errors="coerce").dt.date
    # home_or_away to {0,1}
    df["home_or_away"] = (
        pd.to_numeric(df["home_or_away"], errors="coerce")
        .fillna(0)
        .clip(0, 1)
        .astype(int)
    )
    return df
