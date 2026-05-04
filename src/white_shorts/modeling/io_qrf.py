from __future__ import annotations
import hashlib
import os
import warnings
from pathlib import Path

import joblib

from .io_meta import write_model_meta

DEFAULT_DIR = Path(os.getenv("WS_MODELS_DIR", "models")).expanduser()


def _sig(features: list[str]) -> str:
    s = "|".join(features)
    return hashlib.sha1(s.encode("utf-8")).hexdigest()[:8]


def save_qrf(bundle) -> str:
    DEFAULT_DIR.mkdir(parents=True, exist_ok=True)

    sig = _sig(bundle.features)
    fname = f"{bundle.model_name}_{sig}_{bundle.model_version}.joblib"
    path = DEFAULT_DIR / fname

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


def load_latest(
    prefix: str,
    features: list[str],
    sport: str | None = None,
) -> dict | None:
    DEFAULT_DIR.mkdir(parents=True, exist_ok=True)
    sig = _sig(features)

    cand = sorted(
        DEFAULT_DIR.glob(f"{prefix}_{sig}_*.joblib"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if cand:
        loaded = joblib.load(cand[0])
        if sport is not None and loaded.get("sport", "nhl") != sport:
            warnings.warn(
                f"QRF model '{cand[0].name}' was trained for sport "
                f"'{loaded.get('sport')}' but sport='{sport}' requested.",
                stacklevel=2,
            )
        return loaded

    fallback = sorted(
        DEFAULT_DIR.glob(f"{prefix}_*.joblib"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if fallback:
        warnings.warn(
            f"No QRF model with exact feature signature '{sig}' for prefix "
            f"'{prefix}'. Falling back to '{fallback[0].name}' — feature "
            "mismatch is possible.",
            stacklevel=2,
        )
        return joblib.load(fallback[0])

    return None
