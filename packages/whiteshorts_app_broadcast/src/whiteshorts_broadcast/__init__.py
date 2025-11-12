from .config import BroadcastConfig
from .processors import default_pipeline
from .publishers.supabase_pub import SupabasePublisher
from .publishers.webhook_pub import WebhookPublisher
from .publishers.file_pub import FilePublisher

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
