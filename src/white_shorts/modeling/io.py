# src/white_shorts/modeling/io.py

from __future__ import annotations
import os
from pathlib import Path
import joblib
from .trainers import ModelBundle

# Normalize WS_MODELS_DIR (handles backslashes, trailing slashes, etc.)
DEFAULT_DIR = Path(os.getenv("WS_MODELS_DIR", "models")).expanduser()

def save_model(bundle: ModelBundle, name: str | None = None, dir: Path | None = None) -> str:
    dir = Path(dir or DEFAULT_DIR)
    dir.mkdir(parents=True, exist_ok=True)
    fname = name or f"{bundle.model_name}_{bundle.model_version}.joblib"
    path = dir / fname
    joblib.dump({
        "model": bundle.model,
        "features": bundle.features,
        "target": bundle.target,
        "model_name": bundle.model_name,
        "model_version": bundle.model_version,
    }, path)
    return str(path)

def load_model(name_or_path: str, dir: Path | None = None) -> dict:
    """
    Accept either a bare filename (e.g. 'lgbm_poisson_points_0.3.0.joblib')
    or an absolute/relative path (e.g. 'models\\lgbm_poisson_points_0.3.0.joblib').
    """
    p = Path(name_or_path)
    if p.exists():                      # full path given -> use as-is
        return joblib.load(p)
    base = Path(dir or DEFAULT_DIR)     # filename only -> join with models dir
    return joblib.load(base / name_or_path)

def latest_model_path(prefix: str, dir: Path | None = None) -> str | None:
    dir = Path(dir or DEFAULT_DIR)
    candidates = sorted(dir.glob(f"{prefix}*.joblib"), key=lambda p: p.stat().st_mtime, reverse=True)
    return str(candidates[0]) if candidates else None
