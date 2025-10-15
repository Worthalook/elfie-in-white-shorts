from __future__ import annotations
import duckdb
from pathlib import Path
import pandas as pd
from ..config import settings

def _connect() -> duckdb.DuckDBPyConnection:
    Path(settings.DATA_DIR).mkdir(parents=True, exist_ok=True)
    Path(settings.PARQUET_DIR).mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(settings.DUCKDB_PATH)
    return con

def init_db() -> None:
    con = _connect()
    con.execute("""
    CREATE TABLE IF NOT EXISTS fact_predictions AS SELECT * FROM (
        SELECT ''::VARCHAR as target, DATE '1970-01-01' as date, ''::VARCHAR as game_id,
               ''::VARCHAR as team, ''::VARCHAR as opponent, ''::VARCHAR as player_id,
               ''::VARCHAR as name, ''::VARCHAR as model_name, ''::VARCHAR as model_version,
               ''::VARCHAR as distribution, 0.0::DOUBLE as lambda_or_mu,
               0.0::DOUBLE as q10, 0.0::DOUBLE as q90, ''::VARCHAR as p_ge_k_json,
               ''::VARCHAR as run_id, NOW() as created_ts
    ) WHERE 1=0;
    """)
    con.execute("""
    CREATE TABLE IF NOT EXISTS fact_actuals AS SELECT * FROM (
        SELECT ''::VARCHAR as target, DATE '1970-01-01' as date, ''::VARCHAR as game_id,
               ''::VARCHAR as team, ''::VARCHAR as opponent, ''::VARCHAR as player_id,
               ''::VARCHAR as name, 0.0::DOUBLE as actual, NOW() as created_ts
    ) WHERE 1=0;
    """)
    con.execute("""
    CREATE TABLE IF NOT EXISTS fact_eval AS SELECT * FROM (
        SELECT ''::VARCHAR as target, ''::VARCHAR as window, ''::VARCHAR as metric,
               0.0::DOUBLE as value, DATE '1970-01-01' as as_of_date, ''::VARCHAR as run_id,
               NOW() as created_ts
    ) WHERE 1=0;
    """)
    con.close()

def append(table: str, df: pd.DataFrame) -> None:
    if df.empty:
        return
    con = _connect()
    con.register("_df", df)
    con.execute(f"INSERT INTO {table} SELECT * FROM _df")
    con.unregister("_df")
    con.close()
