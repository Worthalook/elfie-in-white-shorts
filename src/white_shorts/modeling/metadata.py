from __future__ import annotations
import os, json, hashlib
from typing import Any, Iterable
import pandas as pd

def _hash_list(items: Iterable[str]) -> str:
    h = hashlib.sha256()
    for s in sorted([str(x) for x in items]):
        h.update(str(s).encode("utf-8"))
        h.update(b"\x00")
    return h.hexdigest()[:12]

def infer_train_meta(*, bundle: Any, train_df: pd.DataFrame | None, extras: dict | None = None) -> dict:
    """Assemble training metadata for a saved model.
    `bundle` should expose: .model_name, .model_version, .target, .features
    """
    meta: dict = {}
    for k in ("model_name","model_version","target","features"):
        if hasattr(bundle, k):
            meta[k] = getattr(bundle, k)
    feats = list(meta.get("features") or [])
    meta["features"] = feats
    meta["features_hash"] = _hash_list(feats)

    if train_df is not None and len(train_df) > 0:
        meta["train_rows"] = int(len(train_df))
        if "date" in train_df.columns:
            dtv = pd.to_datetime(train_df["date"], errors="coerce")
            if dtv.notna().any():
                meta["train_cutoff_min_date"] = str(dtv.min().date())
                meta["train_cutoff_max_date"] = str(dtv.max().date())
        tgt = meta.get("target")
        if tgt in (train_df.columns if train_df is not None else []):
            meta["train_rows_target_nonnull"] = int(train_df[tgt].notna().sum())
    else:
        meta["train_rows"] = 0

    if extras:
        meta.update(extras)

    meta["created_ts"] = pd.Timestamp.utcnow().isoformat()
    return meta

def write_meta(sidecar_path: str, meta: dict) -> str:
    os.makedirs(os.path.dirname(sidecar_path), exist_ok=True)
    with open(sidecar_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2)
    return sidecar_path
