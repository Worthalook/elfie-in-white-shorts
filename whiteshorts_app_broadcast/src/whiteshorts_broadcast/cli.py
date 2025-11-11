import json, sys, pandas as pd
from . import publish_results
from .config import BroadcastConfig

def main():
    if len(sys.argv) < 2:
        print("Usage: whiteshorts-broadcast <csv_path> [backend]")
        sys.exit(2)
    df_path = sys.argv[1]
    backend = sys.argv[2] if len(sys.argv) > 2 else "supabase"
    cfg = BroadcastConfig(backend=backend)
    df = pd.read_csv(df_path)
    out = publish_results(df, cfg)
    print(json.dumps(out[:3], indent=2))

if __name__ == "__main__":
    main()
