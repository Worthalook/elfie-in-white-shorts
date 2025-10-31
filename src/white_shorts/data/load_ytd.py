from __future__ import annotations
import pandas as pd
from pathlib import Path
from ..utils.validation import REQUIRED_YTD_COLUMNS, ensure_columns

def load_ytd(csv_path: str | Path) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    ensure_columns(df, REQUIRED_YTD_COLUMNS)

    
    # was: df["date"] = pd.to_datetime(df["date"], dayfirst=True, errors="coerce")
    df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d", errors="coerce")

    
    # add these casts
    for c in ["game_id", "player_id", "team", "opponent", "name"]:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()
    # optional: clean “123.0” → “123”
    df["game_id"] = df["game_id"].str.replace(r"\.0$", "", regex=True)


    df["home_or_away"] = (
        pd.to_numeric(df["home_or_away"], errors="coerce").fillna(0).clip(0,1).astype(int)
    )
    return df
