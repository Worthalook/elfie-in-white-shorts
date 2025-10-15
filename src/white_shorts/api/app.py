from __future__ import annotations
from fastapi import FastAPI
import duckdb
from ..config import settings

app = FastAPI(title="WhiteShorts Broadcast API", version="0.3.0")

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/predictions")
def predictions(date: str, target: str | None = None, team: str | None = None, player_id: str | None = None, limit: int = 200):
    con = duckdb.connect(settings.DUCKDB_PATH)
    q = "SELECT * FROM fact_predictions WHERE date = ?"
    params = [date]
    if target:
        q += " AND target = ?"; params.append(target)
    if team:
        q += " AND team = ?"; params.append(team)
    if player_id:
        q += " AND player_id = ?"; params.append(player_id)
    q += " ORDER BY created_ts DESC LIMIT ?"; params.append(limit)
    rows = con.execute(q, params).fetchdf().to_dict(orient="records")
    con.close()
    return {"count": len(rows), "items": rows}

@app.get("/slate")
def slate(date: str):
    con = duckdb.connect(settings.DUCKDB_PATH)
    q = "SELECT DISTINCT date, game_id, team, opponent FROM fact_predictions WHERE date = ? ORDER BY game_id, team"
    rows = con.execute(q, [date]).fetchdf().to_dict(orient="records")
    con.close()
    return {"count": len(rows), "items": rows}
