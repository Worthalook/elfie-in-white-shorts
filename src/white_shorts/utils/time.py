from __future__ import annotations
from datetime import datetime, timedelta, timezone

AUS_TZ = timezone(timedelta(hours=11))  # simple AEDT/AEST placeholder; TODO: use zoneinfo

def today_str() -> str:
    return datetime.now(AUS_TZ).strftime("%Y-%m-%d")

def ymd(date: str | None = None) -> datetime:
    return datetime.strptime(date or today_str(), "%Y-%m-%d")

def days_between(prev: str, curr: str) -> int:
    return (ymd(curr) - ymd(prev)).days
