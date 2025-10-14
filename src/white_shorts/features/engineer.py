from __future__ import annotations
import pandas as pd

def add_days_off(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    # ensure datetime (in case upstream changes)
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out = out.sort_values(["player_id", "date"])
    out["prev_date"] = out.groupby("player_id")["date"].shift(1)
    out["days_off"] = (out["date"] - out["prev_date"]).dt.days.fillna(7)
    out = out.drop(columns=["prev_date"])
    return out

def add_rolling(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce")

    # --- player-level rolling ---
    out = out.sort_values(["player_id", "date"])
    for col in ["points", "goals", "assists", "shots_on_goal"]:
        out[f"rolling_{col}_5"] = (
            out.groupby("player_id")[col]
               .apply(lambda s: s.rolling(5, min_periods=1).mean())
               .reset_index(level=0, drop=True)
        )
     # Backward-compat alias: some code expects 'rolling_sog_5'
    if "rolling_shots_on_goal_5" in out.columns:
        out["rolling_sog_5"] = out["rolling_shots_on_goal_5"]
        
    # --- team-level days off (needs datetime for .dt) ---
    out = out.sort_values(["team", "date"])
    out["days_off_team"] = (
        out.groupby("team")["date"].diff().dt.days.fillna(7)
    )

    # --- team-level rolling (GF/GA proxies) ---
    # team_points per (team, date) then rolling by team
    out["team_points"] = out.groupby(["team", "date"])["points"].transform("sum")
    out["team_gf_5"] = (
        out.groupby("team")["team_points"]
           .apply(lambda s: s.rolling(5, min_periods=1).mean())
           .reset_index(level=0, drop=True)
    )

    # opponent-side proxies: use opponent’s points similarly
    out["opp_points"] = out.groupby(["opponent", "date"])["points"].transform("sum")
    out["team_ga_5"] = (
        out.groupby("team")["opp_points"]
           .apply(lambda s: s.rolling(5, min_periods=1).mean())
           .reset_index(level=0, drop=True)
    )

    # for symmetry keep these helper columns if you like (or drop them)
    out["opp_team_gf_5"] = out["team_ga_5"]
    out["opp_team_ga_5"] = out["team_gf_5"]

    return out

def add_goalie_signal(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    # Smooth opponent goalie GA by team/date (very rough stub)
    out = out.sort_values(["opponent", "date"])
    opp_ga = out.groupby(["opponent", "date"])["goal_tending_goals_against"].transform("mean")
    out["opp_goalie_ga_smooth"] = (
        out.groupby("opponent")["goal_tending_goals_against"]
           .apply(lambda s: s.rolling(5, min_periods=1).mean())
           .reset_index(level=0, drop=True)
    )
    return out

def engineer_minimal(df: pd.DataFrame) -> pd.DataFrame:
    out = add_days_off(df)
    out = add_rolling(out)
    out = add_goalie_signal(out)
    return out
