from __future__ import annotations
import os
import pandas as pd
from .update_history import load_current_season

def build_weighted_training(ytd_csv: str = "data/NHL_YTD.csv",
                            current_parquet: str | None = None,
                            w_last_season: float = 0.5,
                            w_current: float = 1.0) -> tuple[pd.DataFrame, pd.Series]:
    ytd = pd.read_csv(ytd_csv)
    ytd["player_id"] = ytd["player_id"].astype(str)
    ytd["date"] = pd.to_datetime(ytd["date"], dayfirst=True, errors="coerce")

    cur = load_current_season(current_parquet or os.getenv("WS_CURRENT_SEASON_PARQUET", "data/current_season.parquet"))
    if cur is None or cur.empty:
        df = ytd.copy()
        w = pd.Series([w_last_season] * len(df), index=df.index, dtype=float)
        return df, w

    cols = sorted(set(ytd.columns).union(cur.columns))
    for c in cols:
        if c not in ytd.columns: ytd[c] = pd.NA
        if c not in cur.columns: cur[c] = pd.NA
    ytd = ytd[cols]
    cur = cur[cols]

    df = pd.concat([ytd, cur], ignore_index=True)
    w = pd.Series([w_last_season]*len(ytd) + [w_current]*len(cur), index=df.index, dtype=float)
    return df, w
