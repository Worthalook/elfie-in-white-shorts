from __future__ import annotations
import pandas as pd

TEAM_FEATURES = [
  "home_or_away","days_off_team","team_gf_5","team_ga_5",
  "opp_team_gf_5","opp_team_ga_5","opp_goalie_ga_smooth"
]


def _ensure_datetime(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce", dayfirst=True)
    return out

def add_days_off(df: pd.DataFrame) -> pd.DataFrame:
    out = _ensure_datetime(df).sort_values(["player_id","date"])
    out["days_off"] = out.groupby("player_id")["date"].diff().dt.days
    out["days_off"] = out["days_off"].fillna(7).clip(lower=0)
    return out

def add_rolling(df: pd.DataFrame) -> pd.DataFrame:
    out = _ensure_datetime(df)

    # --- player-level rolling ---
    out = out.sort_values(["player_id","date"])
    for col in ["points","goals","assists","shots_on_goal"]:
        out[f"rolling_{col}_5"] = (
            out.groupby("player_id")[col]
               .apply(lambda s: s.rolling(5, min_periods=1).mean())
               .reset_index(level=0, drop=True)
        )
    # optional alias if any old code still expects this
    if "rolling_shots_on_goal_5" in out.columns and "rolling_sog_5" not in out.columns:
        out["rolling_sog_5"] = out["rolling_shots_on_goal_5"]

    # --- team-level features ---
    out = out.sort_values(["team","date"])
    out["days_off_team"] = out.groupby("team")["date"].diff().dt.days
    out["days_off_team"] = out["days_off_team"].fillna(7).clip(lower=0)

    team_day_points = out.groupby(["team","date"])["points"].transform("sum")
    opp_day_points  = out.groupby(["opponent","date"])["points"].transform("sum")

    out["team_gf_5"] = (
        out.assign(_gf=team_day_points)
           .sort_values(["team","date"])
           .groupby("team")["_gf"].apply(lambda s: s.rolling(5, min_periods=1).mean())
           .reset_index(level=0, drop=True)
    )
    out["team_ga_5"] = (
        out.assign(_ga=opp_day_points)
           .sort_values(["team","date"])
           .groupby("team")["_ga"].apply(lambda s: s.rolling(5, min_periods=1).mean())
           .reset_index(level=0, drop=True)
    )

    out["opp_team_gf_5"] = out["team_ga_5"]
    out["opp_team_ga_5"] = out["team_gf_5"]

    return out

def add_goalie_signal(df: pd.DataFrame) -> pd.DataFrame:
    out = _ensure_datetime(df).sort_values(["opponent","date"])
    # Ensure source column exists
    if "goal_tending_goals_against" not in out.columns:
        out["goal_tending_goals_against"] = 0.0
    # Smooth *by opponent* over time
    out["opp_goalie_ga_smooth"] = (
        out.groupby("opponent")["goal_tending_goals_against"]
           .apply(lambda s: s.rolling(5, min_periods=1).mean())
           .reset_index(level=0, drop=True)
    )
    # Guarantee presence (fill if any NaNs)
    out["opp_goalie_ga_smooth"] = out["opp_goalie_ga_smooth"].fillna(0.0)
    return out

def engineer_minimal(df: pd.DataFrame) -> pd.DataFrame:
    out = add_days_off(df)
    out = add_rolling(out)
    out = add_goalie_signal(out)
    # ensure all TEAM_FEATURES exist, even if upstream sparse
    for c in TEAM_FEATURES:
        if c not in out.columns:
            out[c] = 0.0
    out[TEAM_FEATURES] = out[TEAM_FEATURES].fillna(0.0)
    return out
