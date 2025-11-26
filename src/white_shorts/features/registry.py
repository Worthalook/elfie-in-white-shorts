from __future__ import annotations

#PLAYER_FEATURES = [
#    "home_or_away","minutes","days_off","rolling_points_5","rolling_goals_5",
#    "rolling_assists_5","rolling_shots_on_goal_5","opp_goalie_ga_smooth","team_gf_5","team_ga_5",
#]

TEAM_FEATURES = [
  "home_or_away","days_off_team","team_gf_5","team_ga_5",
  "opp_team_gf_5","opp_team_ga_5","opp_goalie_ga_smooth"
]

PLAYER_FEATURES = [
    # --- Existing features ---
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

    # --- NEW: form / residual-style features ---
    "form_points_short_mean",
    "form_points_long_mean",
    "form_points_ratio_short_over_long",
    "form_points_diff_short_minus_long",
    "games_since_last_point",
]
