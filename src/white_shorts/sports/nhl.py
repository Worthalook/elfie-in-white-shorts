from __future__ import annotations
from .adapter import SportAdapter

NHL_ADAPTER = SportAdapter(
    sport="nhl",

    # ------------------------------------------------------------------ #
    # Data schema                                                          #
    # ------------------------------------------------------------------ #
    required_columns=[
        "game_id", "team", "opponent", "player_id", "name",
        "date", "minutes", "points", "goals", "assists",
        "home_or_away", "shots_on_goal",
    ],
    # Present in NHL exports but not structurally required; zero-filled if absent.
    optional_columns=[
        "power_play_assists",
        "power_play_goals",
        "goal_tending_goals_against",
    ],

    # ------------------------------------------------------------------ #
    # Feature engineering                                                  #
    # ------------------------------------------------------------------ #
    stat_columns=["points", "goals", "assists", "shots_on_goal"],
    primary_score_col="points",
    rolling_short=5,
    rolling_long=20,
    goalie_signal_col="goal_tending_goals_against",

    # ------------------------------------------------------------------ #
    # Feature registry                                                     #
    # ------------------------------------------------------------------ #
    player_features=[
        "home_or_away",
        "minutes",
        "days_off",
        "rolling_points_5",
        "rolling_goals_5",
        "rolling_assists_5",
        "rolling_shots_on_goal_5",
        "opp_goalie_ga_smooth",
        "team_gf_5",
        "team_ga_5",
        "form_points_short_mean",
        "form_points_long_mean",
        "form_points_ratio_short_over_long",
        "form_points_diff_short_minus_long",
        "games_since_last_point",
    ],
    team_features=[
        "home_or_away",
        "days_off_team",
        "team_gf_5",
        "team_ga_5",
        "opp_team_gf_5",
        "opp_team_ga_5",
        "opp_goalie_ga_smooth",
    ],

    # ------------------------------------------------------------------ #
    # Prediction / model config                                            #
    # ------------------------------------------------------------------ #
    targets=["total_goals", "points", "goals", "assists", "shots_on_goal"],
    clip_ranges={
        "points":       (0.0, 5.0),
        "goals":        (0.0, 5.0),
        "assists":      (0.0, 5.0),
        "shots_on_goal": (0.0, 15.0),
        "total_goals":  (0.0, 12.0),
    },
    tweedie_variance_power=1.1,
)
