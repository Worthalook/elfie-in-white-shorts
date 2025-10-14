from __future__ import annotations
import pandas as pd

# TODO: call projections endpoint to get tomorrow's games + expected players
# For scaffold, we accept a DF of recent stats and return unique player rows as projections
def naive_projections_from_recent(recent_df: pd.DataFrame) -> pd.DataFrame:
    if recent_df.empty:
        return pd.DataFrame(columns=["date","game_id","team","opponent","player_id","name"])
    key = ["date","game_id","team","opponent","player_id","name"]
    return recent_df[key].drop_duplicates().reset_index(drop=True)
