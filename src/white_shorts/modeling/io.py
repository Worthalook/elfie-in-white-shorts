from __future__ import annotations
import json
import os
import warnings
from pathlib import Path

import joblib
import hashlib

from .trainers import ModelBundle
from .io_meta import write_model_meta

DEFAULT_DIR = Path(os.getenv("WS_MODELS_DIR", "models")).expanduser()


def feature_sig(features: list[str]) -> str:
    s = "|".join(features)
    return hashlib.sha1(s.encode("utf-8")).hexdigest()[:8]


def save_model(
    bundle: ModelBundle,
    name: str | None = None,
    dir: Path | None = None,
) -> str:
    save_dir = Path(dir or DEFAULT_DIR)
    save_dir.mkdir(parents=True, exist_ok=True)

    sig = feature_sig(bundle.features)
    fname = name or f"{bundle.model_name}_{sig}_{bundle.model_version}.joblib"
    path = save_dir / fname

    sport = getattr(bundle, "sport", "nhl")

    joblib.dump(
        {
            "model": bundle.model,
            "features": bundle.features,
            "target": bundle.target,
            "model_name": bundle.model_name,
            "model_version": bundle.model_version,
            "sport": sport,
        },
        path,
    )

    # Build sidecar metadata from what the bundle knows.
    extra = dict(getattr(bundle, "extra_meta", {}))
    extra["sport"] = sport

    write_model_meta(
        str(path),
        model_name=bundle.model_name,
        model_version=bundle.model_version,
        target=bundle.target,
        features=bundle.features,
        extra=extra,
    )

    return str(path)


def load_model(name_or_path: str, dir: Path | None = None) -> dict:
    p = Path(name_or_path)
    if p.exists():
        return joblib.load(p)
    base = Path(dir or DEFAULT_DIR)
    return joblib.load(base / name_or_path)


def latest_model_path(
    prefix: str,
    features: list[str],
    dir: Path | None = None,
    sport: str | None = None,
) -> str | None:
    search_dir = Path(dir or DEFAULT_DIR)
    sig = feature_sig(features)

    # Exact feature-signature match — safe to use directly.
    candidates = sorted(
        search_dir.glob(f"{prefix}_{sig}_*.joblib"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if candidates:
        path = str(candidates[0])
        # Verify sport matches if caller supplied one.
        if sport is not None:
            try:
                saved = joblib.load(path)
                if saved.get("sport", "nhl") != sport:
                    warnings.warn(
                        f"Model at {path} was trained for sport "
                        f"'{saved.get('sport')}' but sport='{sport}' requested. "
                        "Retraining is recommended.",
                        stacklevel=2,
                    )
            except Exception:
                pass
        return path

    # Prefix-only fallback — feature set may not match.
    fallback = sorted(
        search_dir.glob(f"{prefix}_*.joblib"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if fallback:
        warnings.warn(
            f"No model with exact feature signature '{sig}' found for prefix "
            f"'{prefix}'. Falling back to '{fallback[0].name}' — feature "
            "mismatch is possible. Run 'ws train' to rebuild.",
            stacklevel=2,
        )
        return str(fallback[0])

    return None
