# WhiteShorts 3.0 (Core Scaffold)

Minimal end-to-end scaffold focusing on the core loop: **train → predict → persist → rolling metrics**.

## Quickstart
```bash
python -m venv .venv && source .venv/bin/activate
pip install -e .
pip install -r requirements.txt

# Place your last-season CSV at data/NHL_2023_24.csv
# Then:
ws train all
ws predict tomorrow --date 2025-10-15
ws log-actuals from-csv path/to/actuals.csv
ws dashboards rolling-metrics --days 14
```

## Notes
- Replace `data/fetch_recent.py` and `data/fetch_projections.py` with real SportsData calls.
- Replace team goals proxy with real team GF/GA when available.
- Persist trained models with joblib later and load in predict instead of retraining.
