DROP TABLE IF EXISTS predictions_for_broadcast;

CREATE TABLE predictions_for_broadcast (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    date date NOT NULL,
    game_id text,
    team text,
    opponent text,
    player_id text,
    name text,
    target text,
    model_name text,
    model_version text,
    distribution text,
    lambda_or_mu numeric,
    q10 numeric,
    q90 numeric,
    p_ge_k_json jsonb,
    run_id text,
    created_ts timestamp with time zone,
    elfies_number numeric,

    -- App-only fields
    actual_points numeric,
    crowd_score integer NOT NULL DEFAULT 0,
    crowd_flag_game_total boolean NOT NULL DEFAULT false,
    crowd_flag_injury boolean NOT NULL DEFAULT false
);

CREATE INDEX idx_pfb_date ON predictions_for_broadcast (date);
CREATE INDEX idx_pfb_player ON predictions_for_broadcast (player_id);
CREATE INDEX idx_pfb_team_date ON predictions_for_broadcast (team, date);
