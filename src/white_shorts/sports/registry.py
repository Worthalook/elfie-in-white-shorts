from __future__ import annotations
from .adapter import SportAdapter
from .nhl import NHL_ADAPTER

_REGISTRY: dict[str, SportAdapter] = {
    "nhl": NHL_ADAPTER,
}


def get_adapter(sport: str) -> SportAdapter:
    key = sport.strip().lower()
    if key not in _REGISTRY:
        available = ", ".join(sorted(_REGISTRY))
        raise ValueError(f"Unknown sport '{sport}'. Registered: {available}")
    return _REGISTRY[key]


def register_adapter(adapter: SportAdapter) -> None:
    """Register a new sport adapter at runtime (e.g. in tests or new sport modules)."""
    _REGISTRY[adapter.sport.lower()] = adapter


def available_sports() -> list[str]:
    return sorted(_REGISTRY)
