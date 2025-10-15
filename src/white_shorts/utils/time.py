from __future__ import annotations
from datetime import datetime, timedelta, timezone

AUS_TZ = timezone(timedelta(hours=11))

def today_str() -> str:
    return datetime.now(AUS_TZ).strftime("%Y-%m-%d")
