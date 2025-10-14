from __future__ import annotations
import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()

@dataclass
class Settings:
    # Files & storage
    DATA_DIR: str = os.getenv("WS_DATA_DIR", "data")
    DUCKDB_PATH: str = os.getenv("WS_DUCKDB_PATH", "data/white_shorts.duckdb")
    PARQUET_DIR: str = os.getenv("WS_PARQUET_DIR", "data/parquet")

    # API
    SPORTSDATA_API_KEY: str | None = os.getenv("SPORTSDATA_API_KEY")

    # Training
    LAST_SEASON_SAMPLE_WEIGHT: float = float(os.getenv("WS_LAST_SEASON_W", 0.5))
    CURRENT_SEASON_SAMPLE_WEIGHT: float = float(os.getenv("WS_CURR_SEASON_W", 1.0))

    # Random seeds
    SEED: int = int(os.getenv("WS_SEED", 42))

    # Model versions
    MODEL_VERSION_TAG: str = os.getenv("WS_MODEL_VERSION", "0.3.0")

settings = Settings()
