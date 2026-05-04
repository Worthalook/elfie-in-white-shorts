from __future__ import annotations
import pandas as pd
from pathlib import Path
from ..utils.validation import REQUIRED_YTD_COLUMNS, ensure_columns, validate_ytd


def load_ytd(csv_path: str | Path, adapter=None) -> pd.DataFrame:
    """Load a YTD CSV and validate it against *adapter* (or NHL defaults).

    When *adapter* is provided its required_columns are enforced and its
    optional_columns are zero-filled if absent.  When omitted the legacy
    REQUIRED_YTD_COLUMNS list is used so existing call-sites keep working.
    """
    df = pd.read_csv(csv_path)

    if adapter is not None:
        validate_ytd(df, adapter)
    else:
        ensure_columns(df, REQUIRED_YTD_COLUMNS)

    df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d", errors="coerce")

    for c in ["game_id", "player_id", "team", "opponent", "name"]:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()
    df["game_id"] = df["game_id"].str.replace(r"\.0$", "", regex=True)

    df["home_or_away"] = (
        pd.to_numeric(df["home_or_away"], errors="coerce").fillna(0).clip(0, 1).astype(int)
    )
    return df
