from __future__ import annotations
import os, json, hashlib
import pandas as pd

def _features_hash(features: list[str]) -> str:
    s = ",".join(sorted(features))
    return hashlib.md5(s.encode("utf-8")).hexdigest()[:12]

def write_model_meta(model_path: str, *, model_name: str, model_version: str, target: str,
                     features: list[str], train_rows_last_season: int | None = None,
                     train_rows_current: int | None = None, train_cutoff_max_date: str | None = None,
                     extra: dict | None = None) -> str:
    meta = {
        "model_path": model_path,
        "model_name": model_name,
        "model_version": model_version,
        "target": target,
        "features": features,
        "features_hash": _features_hash(features),
        "train_rows_last_season": int(train_rows_last_season or 0),
        "train_rows_current": int(train_rows_current or 0),
        "train_cutoff_max_date": train_cutoff_max_date,
        "created_ts": pd.Timestamp.utcnow().isoformat(),
    }
    if extra:
        meta.update(extra)
    meta_path = model_path.replace(".joblib", ".meta.json")
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2)
    return meta_path
