from __future__ import annotations
from dataclasses import dataclass, field


@dataclass
class SportAdapter:
    """All sport-specific configuration in one place.

    Create one instance per sport and register it with register_adapter().
    Every component that was previously NHL-hardcoded now accepts an optional
    SportAdapter; when None it falls back to the NHL defaults via get_adapter().
    """

    sport: str

    # ------------------------------------------------------------------ #
    # Data schema                                                          #
    # ------------------------------------------------------------------ #
    # Columns that must be present for load_ytd() to succeed.
    required_columns: list[str]
    # Columns that are sport-specific but not universally mandatory.
    # If absent they are silently zero-filled rather than raising.
    optional_columns: list[str] = field(default_factory=list)

    # ------------------------------------------------------------------ #
    # Feature engineering                                                  #
    # ------------------------------------------------------------------ #
    # Stats to build rolling windows from (e.g. goals, assists, …).
    stat_columns: list[str] = field(default_factory=list)
    # Column used for the player form / hot-cold features.
    primary_score_col: str = "points"
    # Short rolling window (used for recent-form features and stat rolling).
    rolling_short: int = 5
    # Long rolling window (used for baseline in form ratio/diff features).
    rolling_long: int = 20
    # Source column for the opponent keeper/goalie signal; None = no signal.
    goalie_signal_col: str | None = None

    # ------------------------------------------------------------------ #
    # Feature registry                                                     #
    # ------------------------------------------------------------------ #
    player_features: list[str] = field(default_factory=list)
    team_features: list[str] = field(default_factory=list)

    # ------------------------------------------------------------------ #
    # Prediction / model config                                            #
    # ------------------------------------------------------------------ #
    # Ordered list of prediction target column names.
    targets: list[str] = field(default_factory=list)
    # Per-target (lo, hi) clip applied after model.predict().
    clip_ranges: dict[str, tuple[float, float]] = field(default_factory=dict)
    # LightGBM Tweedie variance power (1.0 = Poisson, >1 = compound Poisson).
    tweedie_variance_power: float = 1.1
    # Per-target distribution for interval/quantile computation.
    # Keys are target names (e.g. "points", "goals"); values are distribution
    # identifiers understood by modeling.distributions.compute_quantiles().
    # Omitted targets default to "poisson".
    # Supported: "poisson", "normal"
    target_distributions: dict[str, str] = field(default_factory=dict)
