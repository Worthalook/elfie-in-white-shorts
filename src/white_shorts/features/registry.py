from __future__ import annotations
from ..sports.nhl import NHL_ADAPTER

# These module-level names are kept for backward compatibility.
# The canonical definitions now live in NHL_ADAPTER; do not edit here.
PLAYER_FEATURES: list[str] = NHL_ADAPTER.player_features
TEAM_FEATURES: list[str] = NHL_ADAPTER.team_features
