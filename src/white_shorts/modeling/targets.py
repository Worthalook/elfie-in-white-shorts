from __future__ import annotations
from enum import Enum
class Target(str, Enum):
    TOTAL_GOALS = "total_goals"
    POINTS = "points"
    GOALS = "goals"
    ASSISTS = "assists"
    SHOTS = "shots_on_goal"
TRAIN_ORDER = [Target.TOTAL_GOALS, Target.POINTS, Target.GOALS, Target.ASSISTS, Target.SHOTS]
