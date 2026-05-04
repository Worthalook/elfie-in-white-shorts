from __future__ import annotations
import pandas as pd

# Kept for backward compatibility; prefer SportAdapter.required_columns.
REQUIRED_YTD_COLUMNS = [
    "game_id", "team", "opponent", "player_id", "name", "date", "minutes",
    "points", "goals", "assists", "home_or_away", "shots_on_goal",
    "power_play_assists", "power_play_goals", "goal_tending_goals_against",
]


def ensure_columns(df: pd.DataFrame, required: list[str]) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def validate_ytd(df: pd.DataFrame, adapter) -> None:
    """Validate a YTD DataFrame against a SportAdapter.

    Required columns raise; optional columns are zero-filled with a warning
    so callers don't need to pre-populate sport-specific fields.
    """
    ensure_columns(df, adapter.required_columns)

    for col in adapter.optional_columns:
        if col not in df.columns:
            import warnings
            warnings.warn(
                f"Optional column '{col}' not found for sport '{adapter.sport}'; filling with 0.",
                stacklevel=2,
            )
            df[col] = 0
