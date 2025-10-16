from __future__ import annotations
import os
import typer
import pandas as pd
import numpy as np
from datetime import datetime

from ..data.load_ytd import load_ytd
from ..features.engineer import engineer_minimal
from ..features.registry import PLAYER_FEATURES, TEAM_FEATURES
from ..data.persist import init_db, append, PRED_COLS
from ..modeling.trainers_qrf import train_player_qrf, qrf_predict_with_quantiles
from ..modeling.io_qrf import save_qrf, load_latest
from ..modeling.ets_totals import fit_team_ets, forecast_next
from ..data.projections import fetch_projections_by_date

app = typer.Typer(help="QRF players + ETS totals predictions (additive CLI)")

def _bundle_from_loaded(d):
    class B: ...
    b = B()
    b.model = d["model"]; b.features = d["features"]; b.target = d["target"]
    b.model_name = d["model_name"]; b.model_version = d["model_version"]
    return b

def _load_or_train(prefix: str, df_feat: pd.DataFrame, features: list[str], target: str):
    d = load_latest(prefix, features)
    if d and d.get("features") == features:
        return _bundle_from_loaded(d)
    b = train_player_qrf(df_feat, features, target=target)
    save_qrf(b)
    return b

@app.command()
def tomorrow(date: str = typer.Option(None, help="YYYY-MM-DD (or 15/10/2025) slate date")):
    init_db()

    if not date:
        raise typer.BadParameter("Please pass --date to use the projections-scoped slate")

    # 1) Fetch authoritative slate (players + games) for the date
    from ..data.projections import fetch_projections_by_date
    proj = fetch_projections_by_date(date)  # requires SPORTS_DATA_API_KEY

    if proj.empty:
        typer.echo("No projections returned; nothing to predict.")
        return

    # Canonicalize identifiers from the slate (we will keep these in outputs)
    proj["player_id"] = proj["player_id"].astype(str)
    proj["team"]      = proj["team"].astype(str)
    proj["opponent"]  = proj["opponent"].astype(str)
    if "game_id" in proj.columns:
        proj["game_id"] = pd.to_numeric(proj["game_id"], errors="coerce").astype("Int64")

    # Force slate date on all rows
    slate_date = pd.to_datetime(date, dayfirst=True, errors="coerce").normalize()
    proj["date"] = slate_date

    # 2) Build features from YTD (training history), then LEFT-JOIN onto the slate
    ytd_csv = os.getenv("WS_YTD_CSV", "data/NHL_2023_24.csv")
    df_ytd = load_ytd(ytd_csv)
    df_feat_all = engineer_minimal(df_ytd)


    # Use the most recent engineered row per player_id/team as the feature snapshot
    snap_cols = ["player_id","team"] + [c for c in df_feat_all.columns if c in PLAYER_FEATURES]
    df_feat_all["player_id"] = df_feat_all["player_id"].astype(str)
    df_feat_all["date"] = pd.to_datetime(df_feat_all["date"], errors="coerce")
    last_feat = (df_feat_all
                 .sort_values("date")
                 .groupby(["player_id","team"], as_index=False)
                 .tail(1)[snap_cols])

    # Merge features onto the slate; identifiers (date/game_id/team/opponent) stay from proj
    df_feat = proj.merge(last_feat, on=["player_id","team"], how="left")
    


    # 3) If some players lack features, fill with zeros (stub-friendly)
    for f in PLAYER_FEATURES:
        if f not in df_feat.columns:
            df_feat[f] = 0.0
    df_feat[PLAYER_FEATURES] = df_feat[PLAYER_FEATURES].fillna(0.0)

    ####################################################################
    # Team totals ETS baseline using the slate games
    keys = ["date","game_id","team","opponent"]
    # Build a unique games list for both sides:
    games_home = proj[["date","game_id","team","opponent"]].dropna().drop_duplicates()
    games_away = proj.rename(columns={"team":"opponent","opponent":"team"})[["date","game_id","team","opponent"]]
    games = pd.concat([games_home, games_away], ignore_index=True).drop_duplicates()

    # Team history to fit ETS
    hist_keys = ["date","game_id","team","opponent","home_or_away"]
    team_hist = (df_feat_all.groupby(hist_keys, as_index=False)["points"]
                 .sum().rename(columns={"points":"team_goals"}))
    team_hist = team_hist.sort_values("date")

    ets_models = {}
    for team, grp in team_hist.groupby("team"):
        series = grp[["date","team_goals"]].dropna().sort_values("date")
        if len(series) >= 5:
            ets_models[team] = fit_team_ets(series, team)
    run_id = os.getenv("WS_RUN_ID", str(abs(hash(datetime.utcnow().isoformat()))))
    rows = []
    for _, r in games.iterrows():
        lam = np.nan
        if r["team"] in ets_models:
            lam = forecast_next(ets_models[r["team"]])
        rows.append({
            "date": r["date"], "game_id": r["game_id"], "team": r["team"], "opponent": r["opponent"],
            "player_id": None, "name": None,
            "target": "total_goals",
            "model_name": "ets_sum_team_goals",
            "model_version": "0.3.0",
            "distribution": "ets_sum",
            "lambda_or_mu": float(lam) if lam==lam else 0.0,
            "q10": 0.0, "q90": 0.0, "p_ge_k_json": "",
            "run_id": run_id, "created_ts": datetime.utcnow()
        })
    preds_totals = pd.DataFrame(rows)

    #####################################################################
    


    def _player_block(target: str):
        prefix = f"rf_qrf_{target}"
        bundle = _load_or_train(prefix, df_feat, PLAYER_FEATURES, target=target)
        X = df_feat[bundle.features].fillna(0)
        mu, q10, q90 = qrf_predict_with_quantiles(bundle, X, 0.10, 0.90)
        out = df_feat[["date","game_id","team","opponent","player_id","name"]].copy()
        out["target"] = target
        out["model_name"] = bundle.model_name
        out["model_version"] = bundle.model_version
        out["distribution"] = "empirical_qrf"
        out["lambda_or_mu"] = mu
        out["q10"] = q10
        out["q90"] = q90
        out["p_ge_k_json"] = ""
        out["run_id"] = run_id
        out["created_ts"] = datetime.utcnow()
        return out

    preds_points  = _player_block("points")
    preds_goals   = _player_block("goals")
    preds_assists = _player_block("assists")
    preds_shots   = _player_block("shots_on_goal")

    keys = ["date","game_id","team","opponent","home_or_away"]
    team_target = (df_feat.groupby(keys, as_index=False)["points"]
                   .sum().rename(columns={"points":"team_goals"}))
    team_series = team_target.sort_values("date")

    ets_models = {}
    for team, grp in team_series.groupby("team"):
        s = grp[["date","team_goals"]].dropna().sort_values("date")
        if len(s) >= 5:
            ets_models[team] = fit_team_ets(s, team)

    rows = []
    for _, r in team_series.drop_duplicates(subset=["date","game_id","team","opponent"]).iterrows():
        lam_h = forecast_next(ets_models.get(r["team"])) if r["team"] in ets_models else np.nan
        lam_a = forecast_next(ets_models.get(r["opponent"])) if r["opponent"] in ets_models else np.nan
        lam_total = np.nansum([lam_h, lam_a])
        rows.append({
            "date": r["date"], "game_id": r["game_id"], "team": r["team"], "opponent": r["opponent"],
            "player_id": None, "name": None,
            "target": "total_goals",
            "model_name": "ets_sum_team_goals",
            "model_version": "0.3.0",
            "distribution": "ets_sum",
            "lambda_or_mu": float(lam_total) if lam_total==lam_total else 0.0,
            "q10": 0.0, "q90": 0.0, "p_ge_k_json": "",
            "run_id": run_id, "created_ts": datetime.utcnow()
        })
    preds_totals = pd.DataFrame(rows)

    all_preds = pd.concat([preds_points, preds_goals, preds_assists, preds_shots, preds_totals], ignore_index=True)

    for c in PRED_COLS:
        if c not in all_preds.columns:
            all_preds[c] = pd.NA

    out_dir = os.getenv("WS_PARQUET_DIR", "data/parquet")
    os.makedirs(out_dir, exist_ok=True)
    out_csv = os.path.join(out_dir, f"predictions_{run_id}.csv")
    all_preds.to_csv(out_csv, index=False)

    append("fact_predictions", all_preds)
    typer.echo(f"Saved predictions CSV → {out_csv}")

if __name__ == "__main__":
    app()
