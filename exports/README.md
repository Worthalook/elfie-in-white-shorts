# Exports

This folder is for user-facing export files, primarily the **daily predictions CSV**.

## Files
- `predictions.csv` – a single, human-friendly CSV produced by CI for easy emailing.
  Columns include:
  - `target, date, game_id, team, opponent, player_id, name, model_name, model_version, distribution, lambda_or_mu, q10, q90, p_ge_k_json, run_id, created_ts`

The CI workflow will:
1. Run `ws predict tomorrow`.
2. Find the most recent `data/parquet/predictions_*.csv`.
3. Copy it to `artifacts/predictions.csv` (attached to the success email).
