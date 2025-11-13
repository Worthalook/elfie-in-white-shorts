# whiteshorts-broadcast

A tiny, configurable package to:

1) accept a `pandas.DataFrame` of predictions  
2) run a configurable pipeline of processors (format/clean/enrich)  
3) publish to a backend (Supabase, Webhook, or File)

## Install (editable)

```bash
pip install -e .
```

## Usage (Python)

```python
import pandas as pd
from whiteshorts_broadcast import publish_results, BroadcastConfig

df = pd.read_csv("predictions.csv")

cfg = BroadcastConfig(
    backend="supabase",
    supabase_url="https://YOUR-PROJECT.supabase.co",
    supabase_anon_key="YOUR_SERVICE_KEY",
    supabase_table="predictions",
    upsert_on=["game_date","player_id","target"],
    rename_map={"Player Name":"name"},
    required_cols=["game_date","player_id","target"]
)

publish_results(df, cfg)
```

## CLI

```bash
whiteshorts-broadcast predictions.csv supabase
```

## Supabase SQL bootstrap

```sql
create table if not exists public.predictions (
  id bigserial primary key,
  game_date date not null,
  player_id text not null,
  player_name text,
  team text,
  opponent text,
  target text,
  pred_mean numeric,
  pred_q10 numeric,
  pred_q90 numeric,
  model_version text,
  created_at timestamptz default now()
);

create unique index if not exists predictions_uniq on public.predictions (date, player_id, target);
alter table public.predictions enable row level security;
create policy "public read" on public.predictions for select to anon using (true);
```

## GitHub Actions

Add secrets `SUPABASE_URL` and `SUPABASE_SERVICE_KEY`. See `.github/workflows/publish_predictions.yml`.
