# WhiteShorts 3.0 (Core Scaffold)

Minimal end-to-end scaffold focusing on the core loop: **train → predict → persist → rolling metrics**.

## Quickstart
```bash
python -m venv .venv && source .venv/bin/activate
pip install -e .
pip install -r requirements.txt

# Place your last-season CSV at data/NHL_YTD.csv
ws train all
ws predict tomorrow --date 2025-10-15
ws log-actuals from-csv path/to/actuals.csv
ws dashboards rolling-metrics --days 14
```

## API (FastAPI)
Run locally:
```bash
uvicorn white_shorts.api.app:app --reload --port 8080
# GET http://localhost:8080/health
# GET http://localhost:8080/predictions?date=2025-10-15&target=points
# GET http://localhost:8080/slate?date=2025-10-15
```

## Model persistence
- `ws train all` saves models into `models/` (joblib files)
- `ws predict tomorrow` will **load latest** saved models by prefix if present, otherwise it quickly trains inline.
