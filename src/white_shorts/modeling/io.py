# src/white_shorts/modeling/io.py

from __future__ import annotations
import os
from pathlib import Path
import joblib
from .trainers import ModelBundle
import hashlib
from .io_meta import write_model_meta

# Normalize WS_MODELS_DIR (handles backslashes, trailing slashes, etc.)
DEFAULT_DIR = Path(os.getenv("WS_MODELS_DIR", "models")).expanduser()

def feature_sig(features: list[str]) -> str:
    # order matters: the training order reflects in the model
    s = "|".join(features)
    return hashlib.sha1(s.encode("utf-8")).hexdigest()[:8]


def save_model(bundle: ModelBundle, name: str | None = None, dir: Path | None = None) -> str:
    dir = Path(dir or DEFAULT_DIR)
    dir.mkdir(parents=True, exist_ok=True)
    sig = feature_sig(bundle.features)
    fname = name or f"{bundle.model_name}_{sig}_{bundle.model_version}.joblib"
    path = dir / fname
    joblib.dump({
        "model": bundle.model,
        "features": bundle.features,
        "target": bundle.target,
        "model_name": bundle.model_name,
        "model_version": bundle.model_version,
    }, path)
    try:
        write_model_meta(
            path,
            model_name=bundle.model_name,
            model_version=bundle.model_version,
            target=bundle.target,
            features=bundle.features,
            train_rows_last_season=getattr(bundle, "train_rows_last_season", None),
            train_rows_current=getattr(bundle, "train_rows_current", None),
            train_cutoff_max_date=str(getattr(bundle, "train_cutoff_max_date", "")) or None,
        )
    except Exception as e:
        print(f"[warn] failed to write model meta: {e}")
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

def latest_model_path(prefix: str, features: list[str], dir: Path | None = None) -> str | None:
    dir = Path(dir or DEFAULT_DIR)
    sig = feature_sig(features)
    # Prefer exact sig
    candidates = sorted(dir.glob(f"{prefix}_{sig}_*.joblib"), key=lambda p: p.stat().st_mtime, reverse=True)
    if candidates:
        return str(candidates[0])
    # Fallback to any prefix (kept for dev), but this may be mismatched
    fallback = sorted(dir.glob(f"{prefix}_*.joblib"), key=lambda p: p.stat().st_mtime, reverse=True)
    return str(fallback[0]) if fallback else None