import requests
import json
from typing import List, Dict

class WebhookPublisher:
    def __init__(self, cfg):
        self.cfg = cfg

    def publish(self, rows: List[Dict]):
        if not rows:
            return
        headers = {"Content-Type": "application/json"}
        headers.update(self.cfg.headers or {})
        r = requests.post(self.cfg.webhook_url, headers=headers, data=json.dumps(rows))
        r.raise_for_status()
