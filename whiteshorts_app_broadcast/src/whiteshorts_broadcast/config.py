from dataclasses import dataclass, field
from typing import Callable, List, Dict, Any

@dataclass
class BroadcastConfig:
    backend: str = "supabase"  # supabase | webhook | file
    # supabase
    supabase_url: str = "https://gbxxrfrmzgltdyfunwaa.supabase.co"
    supabase_anon_key: str = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImdieHhyZnJtemdsdGR5ZnVud2FhIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2Mjg2MDE5OCwiZXhwIjoyMDc4NDM2MTk4fQ.08GTX8aw9nqNK4Am2YDeAca3Et3zumE0tKHP_uv9Yo0"
    supabase_table: str = "predictions"
    upsert_on: List[str] = field(default_factory=lambda: ["game_date","player_id","target"])
    # webhook
    webhook_url: str = ""
    headers: Dict[str, str] = field(default_factory=dict)
    # file
    out_json_path: str = "predictions_payload.json"
    # pipeline
    processors: List[Callable[[Any], Any]] = field(default_factory=list)
    rename_map: Dict[str,str] = field(default_factory=dict)
    required_cols: List[str] = field(default_factory=list)
