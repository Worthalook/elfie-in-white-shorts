# publishers/supabase_pub.py
import requests, json
from typing import List, Dict

class SupabasePublisher:
    def __init__(self, cfg):
        self.cfg = cfg

    def publish(self, rows: List[Dict]):
        if not rows:
            return
        url = f"{self.cfg.supabase_url}/rest/v1/{self.cfg.supabase_table}"
        headers = {
            "apikey": self.cfg.supabase_anon_key,
            "Authorization": f"Bearer {self.cfg.supabase_anon_key}",
            "Content-Type": "application/json",
            "Prefer": "resolution=merge-duplicates,return=representation"
        }
        keys = ",".join(self.cfg.upsert_on) if self.cfg.upsert_on else ""
        q = f"?on_conflict={keys}" if keys else ""

        # 🔒 Fail fast if NaN/Inf present
        payload = json.dumps(rows, allow_nan=False)

        resp = requests.post(url + q, headers=headers, data=payload)
        # Optional: print server error details for debugging
        try:
            resp.raise_for_status()
        except requests.HTTPError as e:
            raise RuntimeError(f"Supabase error {resp.status_code}: {resp.text}") from e
