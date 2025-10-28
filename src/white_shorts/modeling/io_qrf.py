from __future__ import annotations
from pathlib import Path
import os, hashlib, joblib
from .io_meta import write_model_meta
from ..modeling.metadata import infer_train_meta, write_meta

DEFAULT_DIR = Path(os.getenv("WS_MODELS_DIR", "models")).expanduser()

def _sig(features: list[str]) -> str:
    s = "|".join(features)
    return hashlib.sha1(s.encode("utf-8")).hexdigest()[:8]

def save_qrf(bundle) -> str:
    DEFAULT_DIR.mkdir(parents=True, exist_ok=True)
    fname = f"{bundle.model_name}_{_sig(bundle.features)}_{bundle.model_version}.joblib"
    path = DEFAULT_DIR / fname
    joblib.dump({
        "model": bundle.model,
        "features": bundle.features,
        "target": bundle.target,
        "model_name": bundle.model_name,
        "model_version": bundle.model_version,
    }, path)
    try:
        #-----------------------------------------------------------------
        meta = infer_train_meta(
        bundle=bundle,
        train_df=train_df,  # the frame you trained on (or a representative sample)
        extras={"rows_last_season": int(n_last), "rows_current": int(n_cur)},
    )
    
        write_meta(fname.replace(".joblib", ".meta.json"), meta)
        #-----------------------------------------------------------------
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

def load_latest(prefix: str, features: list[str]) -> dict | None:
    DEFAULT_DIR.mkdir(parents=True, exist_ok=True)
    sig = _sig(features)
    cand = sorted(DEFAULT_DIR.glob(f"{prefix}_{sig}_*.joblib"), key=lambda p: p.stat().st_mtime, reverse=True)
    if cand:
        return joblib.load(cand[0])
    cand = sorted(DEFAULT_DIR.glob(f"{prefix}_*.joblib"), key=lambda p: p.stat().st_mtime, reverse=True)
    return joblib.load(cand[0]) if cand else None
