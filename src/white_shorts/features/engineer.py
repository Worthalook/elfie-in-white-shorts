from __future__ import annotations
import pandas as pd

# NOTE: This is intentionally minimal. Add richer logic later.
def add_days_off(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values(["player_id","date"])  # date is datetime.date
    df["prev_date"] = df.groupby("player_id")["date"].shift(1)
    df["days_off"] = (pd.to_datetime(df["date"]) - pd.to_datetime(df["prev_date"]).fillna(pd.NaT)).dt.days.fillna(7)
    df.drop(columns=["prev_date"], inplace=True)
    return df

def add_rolling(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values(["player_id","date"])  # ensure order
    for col in ["points","goals","assists","shots_on_goal"]:
        df[f"rolling_{col}_5"] = (
            df.groupby("player_id")[col].rolling(window=5, min_periods=1).mean().reset_index(level=0, drop=True)
        )
    # simple team GF/GA: use points as proxy for GF (TODO: replace with team goals once available)
    df["team_gf_5"] = df.groupby(["team","date"])['points'].transform('sum').rolling(5, min_periods=1).mean()
    df["team_ga_5"] = df.groupby(["opponent","date"])['points'].transform('sum').rolling(5, min_periods=1).mean()
    # team-level days off (placeholder)
    df["days_off_team"] = df.groupby("team")["date"].diff().dt.days.fillna(7)
    # opponent team rolling (placeholder reuse)
    df["opp_team_gf_5"] = df.groupby(["opponent","date"])['points'].transform('sum').rolling(5, min_periods=1).mean()
    df["opp_team_ga_5"] = df.groupby(["team","date"])['points'].transform('sum').rolling(5, min_periods=1).mean()
    return df

def add_goalie_signal(df: pd.DataFrame) -> pd.DataFrame:
    # TODO: real probable-goalie mapping; for now smooth opponent goalie GA by team
    ga = df.groupby(["opponent","date"])['goal_tending_goals_against'].mean().rolling(5, min_periods=1).mean()
    df["opp_goalie_ga_smooth"] = ga.reset_index(level=["opponent","date"], drop=True)
    return df

def engineer_minimal(df: pd.DataFrame) -> pd.DataFrame:
    # Chained minimal features for scaffold
    out = df.copy()
    out = add_days_off(out)
    out = add_rolling(out)
    out = add_goalie_signal(out)
    return out
