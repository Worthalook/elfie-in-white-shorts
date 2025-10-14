from __future__ import annotations
import pandas as pd

# TODO: implement real SportsData fetch
# For now, accept a list of date strings and return empty DF with correct schema
def fetch_recent(dates: list[str]) -> pd.DataFrame:
    cols = [
        "game_id","team","opponent","player_id","name","date","minutes",
        "points","goals","assists","home_or_away","shots_on_goal",
        "power_play_assists","power_play_goals","goal_tending_goals_against",
    ]
    return pd.DataFrame(columns=cols)
