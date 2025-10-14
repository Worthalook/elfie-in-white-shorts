from __future__ import annotations

# Central list of canonical feature names by level
PLAYER_FEATURES = [
    "home_or_away","minutes","days_off","rolling_points_5","rolling_goals_5",
    "rolling_assists_5","rolling_shots_on_goal_5","opp_goalie_ga_smooth","team_gf_5","team_ga_5",
]

TEAM_FEATURES = [
    "home_or_away","days_off_team","team_gf_5","team_ga_5","opp_team_gf_5",
    "opp_team_ga_5","opp_goalie_ga_smooth",
]
