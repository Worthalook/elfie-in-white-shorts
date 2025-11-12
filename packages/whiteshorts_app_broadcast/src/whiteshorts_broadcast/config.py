from dataclasses import dataclass, field
from typing import Callable, List, Dict, Any

@dataclass
class BroadcastConfig:
    backend: str = "supabase"  # supabase | webhook | file
    # supabase
    supabase_url: str = "https://gbxxrfrmzgltdyfunwaa.supabase.co"
    supabase_anon_key: str = ""
    supabase_table: str = "predictions"
    upsert_on: List[str] = field(default_factory=lambda: ["date","player_id","target"])
    # webhook
    webhook_url: str = ""
    headers: Dict[str, str] = field(default_factory=dict)
    # file
    out_json_path: str = "predictions_payload.json"
    # pipeline
    processors: List[Callable[[Any], Any]] = field(default_factory=list)
    rename_map: Dict[str,str] = field(default_factory=dict)
    required_cols: List[str] = field(default_factory=list)
