from __future__ import annotations
import pandas as pd

REQUIRED_YTD_COLUMNS = [
    "game_id","team","opponent","player_id","name","date","minutes",
    "points","goals","assists","home_or_away","shots_on_goal",
    "power_play_assists","power_play_goals","goal_tending_goals_against",
]

def ensure_columns(df: pd.DataFrame, required: list[str]) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
