import os, glob, pandas as pd
from whiteshorts_broadcast import publish_results, BroadcastConfig

def find_latest_csv(pattern: str):
    files = sorted(glob.glob(pattern), key=os.path.getmtime, reverse=True)
    return files[0] if files else None

def run():
    csv_path = find_latest_csv("predictions_*.csv") or "predictions.csv"
    df = pd.read_csv(csv_path)
    cfg = BroadcastConfig(
        backend=os.environ.get("WS_BACKEND","supabase"),
        supabase_url=os.environ.get("SUPABASE_URL",""),
        supabase_anon_key=os.environ.get("SUPABASE_SERVICE_KEY",""),
        supabase_table=os.environ.get("SUPABASE_TABLE","predictions")
    )
    publish_results(df, cfg)

if __name__ == "__main__":
    run()
