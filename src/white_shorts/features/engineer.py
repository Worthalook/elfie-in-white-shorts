from __future__ import annotations
import pandas as pd
import numpy as np

# ---------------------------------------------------------------------------
# These standalone helpers are kept for callers that use them directly.
# engineer_minimal() is the preferred entry-point and is adapter-aware.
# ---------------------------------------------------------------------------

TEAM_FEATURES = [
    "home_or_away", "days_off_team", "team_gf_5", "team_ga_5",
    "opp_team_gf_5", "opp_team_ga_5", "opp_goalie_ga_smooth",
]


def _ensure_datetime(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce", dayfirst=True)
    return out


def add_days_off(df: pd.DataFrame) -> pd.DataFrame:
    out = _ensure_datetime(df).sort_values(["player_id", "date"])
    out["days_off"] = out.groupby("player_id")["date"].diff().dt.days
    out["days_off"] = out["days_off"].fillna(7).clip(lower=0)
    return out


def add_rolling(df: pd.DataFrame) -> pd.DataFrame:
    out = _ensure_datetime(df)

    out = out.sort_values(["player_id", "date"])
    for col in ["points", "goals", "assists", "shots_on_goal"]:
        out[f"rolling_{col}_5"] = (
            out.groupby("player_id")[col]
               .apply(lambda s: s.rolling(5, min_periods=1).mean())
               .reset_index(level=0, drop=True)
        )
    if "rolling_shots_on_goal_5" in out.columns and "rolling_sog_5" not in out.columns:
        out["rolling_sog_5"] = out["rolling_shots_on_goal_5"]

    out = out.sort_values(["team", "date"])
    out["days_off_team"] = out.groupby("team")["date"].diff().dt.days
    out["days_off_team"] = out["days_off_team"].fillna(7).clip(lower=0)

    team_day_points = out.groupby(["team", "date"])["points"].transform("sum")
    opp_day_points = out.groupby(["opponent", "date"])["points"].transform("sum")

    out["team_gf_5"] = (
        out.assign(_gf=team_day_points)
           .sort_values(["team", "date"])
           .groupby("team")["_gf"].apply(lambda s: s.rolling(5, min_periods=1).mean())
           .reset_index(level=0, drop=True)
    )
    out["team_ga_5"] = (
        out.assign(_ga=opp_day_points)
           .sort_values(["team", "date"])
           .groupby("team")["_ga"].apply(lambda s: s.rolling(5, min_periods=1).mean())
           .reset_index(level=0, drop=True)
    )

    out["opp_team_gf_5"] = out["team_ga_5"]
    out["opp_team_ga_5"] = out["team_gf_5"]

    return out


def add_goalie_signal(df: pd.DataFrame, source_col: str = "goal_tending_goals_against") -> pd.DataFrame:
    out = _ensure_datetime(df).sort_values(["opponent", "date"])
    if source_col not in out.columns:
        out["opp_goalie_ga_smooth"] = 0.0
    else:
        out["opp_goalie_ga_smooth"] = (
            out.groupby("opponent")[source_col]
               .apply(lambda s: s.rolling(5, min_periods=1).mean())
               .reset_index(level=0, drop=True)
        )
    out["opp_goalie_ga_smooth"] = out["opp_goalie_ga_smooth"].fillna(0.0)
    return out


def _add_form_features(
    df: pd.DataFrame,
    primary_col: str = "points",
    short_window: int = 5,
    long_window: int = 20,
) -> pd.DataFrame:
    required = {"player_id", "date", primary_col}
    if not required.issubset(df.columns):
        return df

    out = df.copy()

    date_col = pd.to_datetime(out["date"], errors="coerce")
    if date_col.dt.tz is not None:
        date_col = date_col.dt.tz_convert("UTC").dt.tz_localize(None)
    out["date"] = date_col
    out = out.sort_values(["player_id", "date"])

    # Coerce to a clean numeric Series stored in a temp column so
    # transform operates on float64 with no NAType surprises.
    out["_pts"] = pd.to_numeric(out[primary_col], errors="coerce").fillna(0.0)

    # Use transform (Series → Series) instead of apply (DataFrame → DataFrame).
    # This is safe in pandas 2.x and never drops the groupby key column.
    grp = out.groupby("player_id", group_keys=False)["_pts"]

    out["form_points_short_mean"] = grp.transform(
        lambda s: s.rolling(short_window, min_periods=1).mean().shift(1)
    )
    out["form_points_long_mean"] = grp.transform(
        lambda s: s.rolling(long_window, min_periods=1).mean().shift(1)
    )

    denom = out["form_points_long_mean"].replace(0.0, np.nan)
    out["form_points_ratio_short_over_long"] = (
        (out["form_points_short_mean"] / denom).replace([np.inf, -np.inf], np.nan)
    )
    out["form_points_diff_short_minus_long"] = (
        out["form_points_short_mean"] - out["form_points_long_mean"]
    )

    def _since_last(s: pd.Series) -> pd.Series:
        result, since = [], 0
        for v in s:
            result.append(since)
            since = 0 if v > 0 else since + 1
        return pd.Series(result, index=s.index)

    out["games_since_last_point"] = grp.transform(_since_last)

    return out.drop(columns=["_pts"])


# ---------------------------------------------------------------------------
# Primary entry-point
# ---------------------------------------------------------------------------

def engineer_minimal(df: pd.DataFrame, adapter=None) -> pd.DataFrame:
    """Build all model features for *df*, parameterised by *adapter*.

    When *adapter* is None the NHL defaults are used so all existing
    call-sites continue to work without modification.
    """
    from ..sports.nhl import NHL_ADAPTER
    adp = adapter if adapter is not None else NHL_ADAPTER

    out = df.copy()

    # ------------------------------------------------------------------ #
    # 1. Coerce dtypes                                                     #
    # ------------------------------------------------------------------ #
    for col in adp.stat_columns + ["minutes", "days_off"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")

    if "home_or_away" in out.columns:
        s = out["home_or_away"].astype(str).str.lower()
        out["home_or_away"] = (
            s.map({"home": 1, "h": 1, "1": 1, "away": 0, "a": 0, "0": 0})
             .fillna(0)
             .astype("int8")
        )

    if "date" in out.columns:
        out["date"] = pd.to_datetime(out["date"], errors="coerce")

    # ------------------------------------------------------------------ #
    # 2. Sort                                                              #
    # ------------------------------------------------------------------ #
    out = out.sort_values(["player_id", "date"])

    # ------------------------------------------------------------------ #
    # 3. Days off (player and team)                                        #
    # ------------------------------------------------------------------ #
    out["days_off"] = (
        out.groupby("player_id")["date"].diff().dt.days.fillna(7).clip(lower=0)
    )
    out = out.sort_values(["team", "date"])
    out["days_off_team"] = (
        out.groupby("team")["date"].diff().dt.days.fillna(7).clip(lower=0)
    )
    out = out.sort_values(["player_id", "date"])

    # ------------------------------------------------------------------ #
    # 4. Rolling player stats                                              #
    # ------------------------------------------------------------------ #
    g = out.groupby("player_id", group_keys=False)
    for col in adp.stat_columns:
        if col in out.columns:
            feat_name = f"rolling_{col}_{adp.rolling_short}"
            out[feat_name] = (
                g[col]
                .rolling(adp.rolling_short, min_periods=1)
                .mean()
                .reset_index(level=0, drop=True)
                .shift(1)
            )

    # ------------------------------------------------------------------ #
    # 5. Team rolling (goals-for / goals-against)                          #
    # ------------------------------------------------------------------ #
    primary = adp.primary_score_col
    if primary in out.columns:
        team_day = out.groupby(["team", "date"])[primary].transform("sum")
        opp_day = out.groupby(["opponent", "date"])[primary].transform("sum")

        out["team_gf_5"] = (
            out.assign(_gf=team_day)
               .sort_values(["team", "date"])
               .groupby("team")["_gf"]
               .apply(lambda s: s.rolling(adp.rolling_short, min_periods=1).mean())
               .reset_index(level=0, drop=True)
        )
        out["team_ga_5"] = (
            out.assign(_ga=opp_day)
               .sort_values(["team", "date"])
               .groupby("team")["_ga"]
               .apply(lambda s: s.rolling(adp.rolling_short, min_periods=1).mean())
               .reset_index(level=0, drop=True)
        )
        out["opp_team_gf_5"] = out["team_ga_5"]
        out["opp_team_ga_5"] = out["team_gf_5"]

    # ------------------------------------------------------------------ #
    # 6. Goalie / keeper signal                                            #
    # ------------------------------------------------------------------ #
    if adp.goalie_signal_col and adp.goalie_signal_col in out.columns:
        out[adp.goalie_signal_col] = pd.to_numeric(out[adp.goalie_signal_col], errors="coerce").fillna(0.0)
        out = out.sort_values(["opponent", "date"])
        out["opp_goalie_ga_smooth"] = (
            out.groupby("opponent")[adp.goalie_signal_col]
               .apply(lambda s: s.rolling(adp.rolling_short, min_periods=1).mean())
               .reset_index(level=0, drop=True)
               .fillna(0.0)
        )
        out = out.sort_values(["player_id", "date"])
    else:
        out["opp_goalie_ga_smooth"] = 0.0

    # ------------------------------------------------------------------ #
    # 7. Form / hot-cold features                                          #
    # ------------------------------------------------------------------ #
    out = _add_form_features(
        out,
        primary_col=adp.primary_score_col,
        short_window=adp.rolling_short,
        long_window=adp.rolling_long,
    )

    return out
