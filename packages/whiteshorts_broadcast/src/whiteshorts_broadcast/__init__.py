from .config import BroadcastConfig
from .processors import default_pipeline
from .publishers.supabase_pub import SupabasePublisher
from .publishers.webhook_pub import WebhookPublisher
from .publishers.file_pub import FilePublisher
import os,glob

def publish_results(df, config: 'BroadcastConfig'):
    processed = default_pipeline(df, config)
    if config.backend == "supabase":
        SupabasePublisher(config).publish(processed)
    elif config.backend == "webhook":
        WebhookPublisher(config).publish(processed)
    elif config.backend == "file":
        FilePublisher(config).publish(processed)
    else:
        raise ValueError(f"Unknown backend: {config.backend}")
    return processed

def find_latest_csv(pattern: str):
    files = sorted(glob.glob(pattern), key=os.path.getmtime, reverse=True)
    return files[0] if files else None
