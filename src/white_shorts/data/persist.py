from __future__ import annotations
import duckdb
from pathlib import Path
import pandas as pd
from ..config import settings

PRED_COLS = [
    "target", "date", "game_id", "team", "opponent",
    "player_id", "name", "model_name", "model_version",
    "distribution", "lambda_or_mu", "q10", "q90",
    "p_ge_k_json", "created_ts", "run_id",
]

ACTUAL_COLS = [
    "target", "date", "game_id", "team", "opponent",
    "player_id", "name", "actual", "created_ts",
]

def _connect() -> duckdb.DuckDBPyConnection:
    Path(settings.DATA_DIR).mkdir(parents=True, exist_ok=True)
    Path(settings.PARQUET_DIR).mkdir(parents=True, exist_ok=True)
    return duckdb.connect(settings.DUCKDB_PATH)

def _sanitize_for_duckdb(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    # date as DATE
    if "date" in out.columns:
        out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.date
    # created_ts as tz-naive TIMESTAMP
    if "created_ts" in out.columns:
        out["created_ts"] = pd.to_datetime(out["created_ts"], errors="coerce")
        if pd.api.types.is_datetime64tz_dtype(out["created_ts"]):
            out["created_ts"] = out["created_ts"].dt.tz_convert("UTC").dt.tz_localize(None)
    return out

def init_db() -> None:
    con = _connect()
    con.execute("""
        CREATE TABLE IF NOT EXISTS fact_predictions AS SELECT * FROM (
            SELECT
                ''::VARCHAR  AS target,
                DATE '1970-01-01' AS date,
                ''::VARCHAR  AS game_id,
                ''::VARCHAR  AS team,
                ''::VARCHAR  AS opponent,
                ''::VARCHAR  AS player_id,
                ''::VARCHAR  AS name,
                ''::VARCHAR  AS model_name,
                ''::VARCHAR  AS model_version,
                ''::VARCHAR  AS distribution,
                0.0::DOUBLE  AS lambda_or_mu,
                0.0::DOUBLE  AS q10,
                0.0::DOUBLE  AS q90,
                ''::VARCHAR  AS p_ge_k_json,
                TIMESTAMP '1970-01-01 00:00:00' AS created_ts,
                ''::VARCHAR  AS run_id
        ) WHERE 1=0;
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS fact_actuals AS SELECT * FROM (
            SELECT
                ''::VARCHAR  AS target,
                DATE '1970-01-01' AS date,
                ''::VARCHAR  AS game_id,
                ''::VARCHAR  AS team,
                ''::VARCHAR  AS opponent,
                ''::VARCHAR  AS player_id,
                ''::VARCHAR  AS name,
                0.0::DOUBLE  AS actual,
                TIMESTAMP '1970-01-01 00:00:00' AS created_ts
        ) WHERE 1=0;
    """)
    con.close()

def _ordered_cols_for_table(con: duckdb.DuckDBPyConnection, table: str) -> list[str]:
    # Read schema in defined order; PRAGMA table_info returns rows with 'cid'
    info = con.execute(f"PRAGMA table_info('{table}')").fetchdf()
    return info.sort_values("cid")["name"].tolist()

def _align_df_to_table(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    out = df.copy()
    # ensure all expected cols exist (fill with NA/empty)
    for c in cols:
        if c not in out.columns:
            out[c] = pd.NA
    # drop any extras not in table
    out = out[cols]
    return out

def append(table: str, df: pd.DataFrame) -> None:
    if df.empty:
        return
    con = _connect()
    try:
        clean = _sanitize_for_duckdb(df)
        # Determine expected columns from schema
        table_cols = _ordered_cols_for_table(con, table)
        # Align DataFrame to table schema order
        aligned = _align_df_to_table(clean, table_cols)

        con.register("_df", aligned)
        # Insert BY NAME (no SELECT *): this avoids positional mismatches
        col_list = ", ".join([f'"{c}"' for c in table_cols])
        con.execute(f"INSERT INTO {table} ({col_list}) SELECT {col_list} FROM _df")
    finally:
        try: con.unregister("_df")
        except: pass
        con.close()
