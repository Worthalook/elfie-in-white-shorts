from __future__ import annotations
import os
import typer
import pandas as pd
import numpy as np
from datetime import datetime

from ..data.load_ytd import load_ytd
from ..data.update_history import load_current_season
from ..features.engineer import engineer_minimal
from ..features.registry import PLAYER_FEATURES
from ..data.persist import init_db, append, PRED_COLS

from ..modeling.trainers_qrf import train_player_qrf, qrf_predict_with_quantiles
from ..modeling.io_qrf import save_qrf, load_latest
from ..modeling.ets_totals import fit_team_ets, forecast_next

app = typer.Typer(help="Predict using a saved slate parquet (players+games) as the authoritative driver.")

def _bundle_from_loaded(d):
    class B: ...
    b = B()
    b.model = d["model"]; b.features = d["features"]; b.target = d["target"]
    b.model_name = d["model_name"]; b.model_version = d["model_version"]
    return b

from pathlib import Path
import os, pandas as pd

def _resolve_slate_path(s: str, base="data/slates") -> Path | None:
    # If a direct .parquet path was passed, honor it
    p = Path(s)
    if p.suffix == ".parquet" and p.exists():
        return p

    d = pd.to_datetime(s, dayfirst=True, errors="coerce")
    if pd.isna(d):
        return None

    iso = d.date().isoformat()                 # YYYY-MM-DD
    dmy = d.strftime("%Y-%d-%m")               # legacy pattern (buggy past runs)

    basep = Path(os.getenv("WS_SLATES_DIR", base))
    for name in (f"slate_{iso}.parquet", f"slate_{dmy}.parquet"):
        cand = basep / name
        if cand.exists():
            return cand
    return None





def _load_or_train(prefix: str, df_feat: pd.DataFrame, features: list[str], target: str):
    d = load_latest(prefix, features)
    if d and d.get("features") == features:
        return _bundle_from_loaded(d)
    b = train_player_qrf(df_feat, features, target=target)
    save_qrf(b)
    return b

@app.command()
def slate(slate_parquet: str = typer.Argument(..., help="Path to data/slates/slate_<date>.parquet"),
          ytd_csv: str = typer.Option("data/NHL_YTD.csv", help="Last season CSV"),
          current_season_parquet: str = typer.Option(None, help="Current-season parquet (defaults to WS_CURRENT_SEASON_PARQUET)"),
          version: str = typer.Option("0.3.0", help="Model version tag"),
          out_dir: str = typer.Option("data/parquet", help="Output directory for CSV artifact")):
    """Run player QRF predictions and team totals ETS using a saved slate as the driver."""
    init_db()

    # 0) Load slate (authoritative identifiers: date, game_id, team, opponent, player_id, name)
    
    # then use it:
    path = _resolve_slate_path(slate_parquet)
    if path is None or not path.exists():
        print(f"[predict_from_slate] Slate not found for {slate_parquet}")
        raise SystemExit(0)

    df = pd.read_parquet(path)
    if df.empty:
        print(f"[predict_from_slate] Empty slate → {path}. Nothing to predict.")
        raise SystemExit(0)


    proj = pd.read_parquet(slate_parquet)
    if proj.empty:
        typer.echo("Slate is empty; nothing to predict.")
        return

    # Coerce types
    proj["player_id"] = proj["player_id"].astype(str)
    if "game_id" in proj.columns:
        proj["game_id"] = pd.to_numeric(proj["game_id"], errors="coerce")
    for c in ["team","opponent"]:
        if c in proj.columns:
            proj[c] = proj[c].astype(str)
    # Ensure date is normalized
    proj["date"] = pd.to_datetime(proj["date"], errors="coerce").dt.normalize()

    # 1) Build feature history from YTD (+ current season if available)
    df_ytd = load_ytd(ytd_csv)
    cur_path = current_season_parquet or os.getenv("WS_CURRENT_SEASON_PARQUET", "data/current_season.parquet")
    df_cur = load_current_season(cur_path)
    if df_cur is not None and not df_cur.empty:
        # union columns
        cols = sorted(set(df_ytd.columns).union(df_cur.columns))
        for c in cols:
            if c not in df_ytd.columns: df_ytd[c] = pd.NA
            if c not in df_cur.columns: df_cur[c] = pd.NA
        hist = pd.concat([df_ytd[cols], df_cur[cols]], ignore_index=True)
    else:
        hist = df_ytd

    # Engineer features on history
    df_feat_all = engineer_minimal(hist)
    df_feat_all["player_id"] = df_feat_all["player_id"].astype(str)
    df_feat_all["date"] = pd.to_datetime(df_feat_all["date"], errors="coerce")

    # Take most recent snapshot per (player_id, team) for features
    snap_cols = ["player_id","team"] + [f for f in PLAYER_FEATURES]
    for c in ["team"]:
        if c not in df_feat_all.columns: df_feat_all[c] = pd.NA
    df_feat_all = df_feat_all.sort_values("date")
    last_feat = (df_feat_all
                 .groupby(["player_id","team"], as_index=False)
                 .tail(1)[snap_cols])

    # Merge features onto slate; keep slate identifiers
    df_feat = proj.merge(last_feat, on=["player_id","team"], how="left")

    # Fill any missing feature columns with zeros (stub-friendly)
    for f in PLAYER_FEATURES:
        if f not in df_feat.columns:
            df_feat[f] = 0.0
    df_feat[PLAYER_FEATURES] = df_feat[PLAYER_FEATURES].fillna(0.0)

    run_id = os.getenv("WS_RUN_ID", str(abs(hash(datetime.utcnow().isoformat()))))

    # 2) Player predictions via QRF
    def _player_block(target: str):
        prefix = f"rf_qrf_{target}"
        # Optionally, we could train with sample_weight using training_merge, but keep simple here.
        bundle = _load_or_train(prefix, df_feat_all, PLAYER_FEATURES, target=target)
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

    # 3) Team totals via ETS, but driven by slate games (one total per game)
    # Fit ETS per team from historical team_goals
    hist_keys = ["date","game_id","team","opponent","home_or_away"]
    team_hist = (df_feat_all.groupby(hist_keys, as_index=False)["points"]
                 .sum().rename(columns={"points":"team_goals"}))
    team_hist = team_hist.sort_values("date")

    ets_models = {}
    for team, grp in team_hist.groupby("team"):
        s = grp[["date","team_goals"]].dropna().sort_values("date")
        if len(s) >= 5:
            ets_models[team] = fit_team_ets(s, team)

    # Derive unique games from the slate
    games = proj[["date","game_id","team","opponent"]].dropna().drop_duplicates()
    # Create a per-game total by summing team λs
    total_rows = []

    for gid, ggrp in games.groupby("game_id"):
        # pick a canonical row (first) for identifiers
        r0 = ggrp.iloc[0]
        pred_date = r0["date"].strftime("%Y-%m-%d")
        team_a = str(r0["team"]); team_b = str(r0["opponent"])
        lam_a = forecast_next(ets_models.get(team_a)) if team_a in ets_models else np.nan
        lam_b = forecast_next(ets_models.get(team_b)) if team_b in ets_models else np.nan
        lam_total = np.nansum([lam_a, lam_b])
        total_rows.append({
            "date": r0["date"],
            "game_id": gid,
            "team": team_a,
            "opponent": team_b,
            "player_id": None,
            "name": None,
            "target": "total_goals",
            "model_name": "ets_sum_team_goals",
            "model_version": version,
            "distribution": "ets_sum",
            "lambda_or_mu": float(lam_total) if lam_total==lam_total else 0.0,
            "q10": 0.0,
            "q90": 0.0,
            "p_ge_k_json": "",
            "run_id": run_id,
            "created_ts": datetime.utcnow(),
        })
    preds_totals = pd.DataFrame(total_rows)

    all_preds = pd.concat([preds_points, preds_goals, preds_assists, preds_shots, preds_totals], ignore_index=True)

    # Align to expected columns for persistence
    for c in PRED_COLS:
        if c not in all_preds.columns:
            all_preds[c] = pd.NA

    os.makedirs(out_dir, exist_ok=True)
    out_csv = os.path.join(out_dir, f"predictions_{pred_date}_{run_id}.csv")
    all_preds.to_csv(out_csv, index=False)

    append("fact_predictions", all_preds)
    typer.echo(f"Saved predictions CSV → {out_csv}")

if __name__ == "__main__":
    app()
