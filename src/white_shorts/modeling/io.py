from __future__ import annotations
from operator import contains
import os
from pathlib import Path
import joblib
from .trainers import ModelBundle

DEFAULT_DIR = Path(os.getenv("WS_MODELS_DIR", "models"))

def save_model(bundle: ModelBundle, name: str | None = None, dir: Path | None = None) -> str:
    """Save a model bundle (LightGBM etc.) to disk."""
    dir = dir or DEFAULT_DIR
    dir.mkdir(parents=True, exist_ok=True)
    fname = name or f"{bundle.model_name}_{bundle.model_version}.joblib"
    path = dir / fname
    joblib.dump(
        {
            "model": bundle.model,
            "features": bundle.features,
            "target": bundle.target,
            "model_name": bundle.model_name,
            "model_version": bundle.model_version,
        },
        path,
    )
    return str(path)

def load_model(name: str, dir: Path | None = None) -> dict:
    """Load a previously saved model bundle dictionary."""
    
    dir = dir or DEFAULT_DIR
    path = dir / name
   
    #if(path.contains('models//models')):
    #   return "HELLO 2x "
    return joblib.load(path)

def latest_model_path(prefix: str, dir: Path | None = None) -> str | None:
    """Find the most recent joblib file whose name starts with prefix."""
    dir = dir or DEFAULT_DIR
    if not dir.exists():
        return None
    matches = sorted(
        [p for p in dir.glob(f"{prefix}*.joblib")],
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    return str(matches[0]) if matches else None
