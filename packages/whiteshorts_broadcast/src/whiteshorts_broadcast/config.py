from dataclasses import dataclass, field
from typing import Callable, List, Dict, Any

@dataclass
class BroadcastConfig:
    backend: str = "supabase"  # supabase | webhook | file
    # supabase
    supabase_url: str = "https://gbxxrfrmzgltdyfunwaa.supabase.co"
    supabase_anon_key: str = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImdieHhyZnJtemdsdGR5ZnVud2FhIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjI4NjAxOTgsImV4cCI6MjA3ODQzNjE5OH0.X_CnVXWArB8tPO1Cq2I18dpeZ7de4dRZliIKVzmGFok"
    supabase_table: str = "predictions_for_broadcast"
    upsert_on: List[str] = field(default_factory=lambda: ["date","player_id","target"])
    # webhook
    webhook_url: str = ""
    headers: Dict[str, str] = field(default_factory=dict)
    # file
    out_json_path: str = "predictions_for_broadcast_payload.json"
    # pipeline
    processors: List[Callable[[Any], Any]] = field(default_factory=list)
    rename_map: Dict[str,str] = field(default_factory=dict)
    required_cols: List[str] = field(default_factory=list)
    sportsdata_api_key: str = "3cafb838ee6d41d7afc0ec33174884a2"
