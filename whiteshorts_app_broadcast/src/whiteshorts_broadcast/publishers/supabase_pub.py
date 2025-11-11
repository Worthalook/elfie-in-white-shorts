# publishers/supabase_pub.py
import requests, json, math
import numpy as np

def _to_json_safe_value(v):
    # Convert numpy scalars to native
    if isinstance(v, (np.floating, np.integer, np.bool_)):
        v = v.item()

    # Non-finite floats → None
    if isinstance(v, float):
        if math.isnan(v) or math.isinf(v):
            return None
        return v

    # Lists / dicts: sanitize recursively
    if isinstance(v, list):
        return [_to_json_safe_value(x) for x in v]
    if isinstance(v, dict):
        return {k: _to_json_safe_value(x) for k, x in v.items()}

    # pandas Timestamp / datetime: ISO string
    try:
        import pandas as pd
        if isinstance(v, (pd.Timestamp,)):
            return v.isoformat()
    except Exception:
        pass

    # datetime from stdlib
    try:
        from datetime import datetime, date
        if isinstance(v, datetime):
            return v.isoformat()
        if isinstance(v, date):
            return v.isoformat()
    except Exception:
        pass

    return v

def _sanitize_rows(rows):
    clean = []
    changed = []
    for i, r in enumerate(rows):
        cr = {}
        ch = {}
        for k, v in r.items():
            safe = _to_json_safe_value(v)
            # Also clip absurd magnitudes (optional, adjust threshold)
            if isinstance(safe, (int, float)) and isinstance(v, (int, float)):
                if abs(safe) > 1e12:  # defensive cap
                    ch[k] = {"old": v, "new": float(np.sign(safe) * 1e12)}
                    safe = float(np.sign(safe) * 1e12)
            # Track changes (including NaN/Inf → None)
            if (safe is None and v is not None) or (safe != v):
                ch[k] = {"old": v, "new": safe}
            cr[k] = safe
        clean.append(cr)
        if ch:
            changed.append({"row_index": i, "changed": ch})
    return clean, changed

class SupabasePublisher:
    def __init__(self, cfg):
        self.cfg = cfg

    def publish(self, rows):
        if not rows:
            return

        # ---- sanitize & log diffs
        rows, diffs = _sanitize_rows(rows)
        if diffs:
            # print only first few diffs; expand as needed
            print(f"[broadcast] sanitized {len(diffs)} rows (showing up to 3): {diffs[:3]}")

        url = f"{self.cfg.supabase_url}/rest/v1/{self.cfg.supabase_table}"
        headers = {
            "apikey": self.cfg.supabase_anon_key,
            "Authorization": f"Bearer {self.cfg.supabase_anon_key}",
            "Content-Type": "application/json",
            "Prefer": "resolution=merge-duplicates,return=representation"
        }
        keys = ",".join(self.cfg.upsert_on) if self.cfg.upsert_on else ""
        q = f"?on_conflict={keys}" if keys else ""

        payload = json.dumps(rows, allow_nan=False)  # will fail if anything non-finite remains

        resp = requests.post(url + q, headers=headers, data=payload)
        try:
            resp.raise_for_status()
        except requests.HTTPError as e:
            raise RuntimeError(f"Supabase error {resp.status_code}: {resp.text}") from e
