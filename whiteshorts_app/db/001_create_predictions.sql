-- 2025-11-10T14:37:30.031513Z
-- Migration: create predictions table + indexes + RLS + realtime publication

create table if not exists public.predictions (
  id bigserial primary key,
  game_date date not null,
  player_id text not null,
  player_name text,
  team text,
  opponent text,
  target text,              -- e.g., points/goals/assists
  pred_mean numeric,
  pred_q10 numeric,
  pred_q90 numeric,
  model_version text,
  created_at timestamptz default now()
);

create unique index if not exists predictions_uniq on public.predictions (game_date, player_id, target);
create index if not exists predictions_date_idx on public.predictions (game_date);
create index if not exists predictions_player_idx on public.predictions (player_id);
create index if not exists predictions_target_idx on public.predictions (target);

-- Realtime
alter publication supabase_realtime add table public.predictions;

-- RLS
alter table public.predictions enable row level security;

-- Public read (you can tighten this later)
drop policy if exists "public read" on public.predictions;
create policy "public read"
on public.predictions
for select
to anon
using (true);

-- Service role write (CI or server key)
drop policy if exists "service insert" on public.predictions;
create policy "service insert"
on public.predictions
for insert
to service_role
with check (true);

drop policy if exists "service update" on public.predictions;
create policy "service update"
on public.predictions
for update
to service_role
using (true)
with check (true);
