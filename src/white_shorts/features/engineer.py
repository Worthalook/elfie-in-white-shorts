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


def _add_form_features(
    df: pd.DataFrame,
    short_window: int = 5,
    long_window: int = 20,
) -> pd.DataFrame:
    """
    Add "form" / residual-like features so the model can see when a player
    has been recently under- or over-performing relative to their longer-term
    baseline.

    All features are shifted by 1 game so they only use information that would
    have been available BEFORE the current game.

    This version is robust to whatever 'date' dtype you have (string, datetime,
    tz-aware, etc.) and won't blow up on timestamp comparisons.
    """
    required = {"player_id", "date", "points"}
    if not required.issubset(df.columns):
        return df

    df2 = df.copy()

    # --- Robust date normalisation for ordering ---
    # Coerce to datetime (tz-naive), then to int64 "ordinal" for sorting.
    date_norm = pd.to_datetime(df2["date"], errors="coerce")
    # If there are tz-aware datetimes, convert to naive UTC or local;
    # here we just drop tz and treat them uniformly.
    if getattr(date_norm.dt, "tz", None) is not None:
        date_norm = date_norm.dt.tz_convert("UTC").dt.tz_localize(None)

    df2["_date_ord"] = date_norm.view("int64")  # NaT will become the same sentinel, but that's fine

    # Sort by player + date ordinal; this avoids direct timestamp comparisons
    df2 = df2.sort_values(["player_id", "_date_ord"])

    grouped = df2.groupby("player_id", group_keys=False)

    def _per_player(g: pd.DataFrame) -> pd.DataFrame:
        pts = pd.to_numeric(g["points"], errors="coerce").fillna(0.0)

        # Short- and long-term rolling means (shifted to be pre-game)
        roll_short = pts.rolling(short_window, min_periods=1).mean().shift(1)
        roll_long  = pts.rolling(long_window,  min_periods=1).mean().shift(1)

        g["form_points_short_mean"] = roll_short
        g["form_points_long_mean"]  = roll_long

        # Ratio & difference: "how hot/cold is recent form vs baseline?"
        denom = roll_long.replace(0.0, np.nan)
        g["form_points_ratio_short_over_long"] = (
            (roll_short / denom).replace([np.inf, -np.inf], np.nan)
        )
        g["form_points_diff_short_minus_long"] = roll_short - roll_long

        # Games since last point (simple slump length)
        games_since_last: list[int] = []
        since = 0
        for v in pts:
            games_since_last.append(since)
            if v > 0:
                since = 0
            else:
                since += 1
        g["games_since_last_point"] = games_since_last

        return g

    df2 = grouped.apply(_per_player).reset_index(drop=True)

    # Clean up helper column
    df2 = df2.drop(columns=["_date_ord"])

    return df2



def engineer_minimal(df: pd.DataFrame) -> pd.DataFrame:
    """
    Minimal feature set used by the QRF player model.
    Assumes df has columns: date, player_id, team, opponent, points, goals,
    assists, shots_on_goal, etc.
    """
    out = df.copy()
    out = out.sort_values(["player_id", "date"])

    g = out.groupby("player_id", group_keys=False)

    # --- existing rolling/base features (adapt to your real code) ---
    out["roll_points_5"] = (
        g["points"]
        .rolling(5, min_periods=1)
        .mean()
        .reset_index(level=0, drop=True)
        .shift(1)
    )
    out["roll_points_20"] = (
        g["points"]
        .rolling(20, min_periods=1)
        .mean()
        .reset_index(level=0, drop=True)
        .shift(1)
    )
    # ... your other existing features ...

    # --- NEW: form / residual-style features ---
    df_feat = engineer_minimal(df_raw)
    out = _add_form_features(df_feat, short_window=5, long_window=20)

    return out
