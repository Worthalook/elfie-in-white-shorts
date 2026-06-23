"""Backtesting CLI.

Commands
--------
ws backtest fetch-season    One-off: fetch full season stats from API → CSV
ws backtest reset-db        Backup + reinitialise DuckDB (clean slate)
ws backtest run             Single-date prediction + evaluation
ws backtest sweep           Full-season loop (one retrain per date)
ws backtest score           Fast re-score of stored predictions with new formula
"""
from __future__ import annotations
import os
import shutil
import uuid
from datetime import timedelta
from pathlib import Path

import duckdb
import pandas as pd
import typer

from ..config import settings
from ..data.persist import _connect, _sanitize_for_duckdb, init_db, BACKTEST_COLS
from ..data.update_history import fetch_player_stats_by_date
from ..backtest.metrics import add_all_elfies_variants, compute_day_metrics
from ..backtest.runner import run_single_date

app = typer.Typer(help="Backtesting commands")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_csv(path: str, label: str, optional: bool = False) -> pd.DataFrame:
    p = Path(path)
    if not p.exists():
        if optional:
            typer.echo(f"[backtest] {label} not found ({path}) — skipping")
            return pd.DataFrame()
        typer.echo(f"[backtest] {label} not found: {path}", err=True)
        raise typer.Exit(1)
    df = pd.read_csv(path)
    if len(df) < 2:
        if optional:
            typer.echo(f"[backtest] {label} is a stub ({len(df)} rows) — skipping")
            return pd.DataFrame()
        typer.echo(f"[backtest] {label} appears to be a stub file: {path}", err=True)
        raise typer.Exit(1)
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.normalize()
    df["player_id"] = df["player_id"].astype(str)
    return df


def _store_results(results: pd.DataFrame) -> None:
    """Append results to fact_backtest_results in DuckDB."""
    if results.empty:
        return
    con = _connect()
    try:
        out = _sanitize_for_duckdb(results.copy())
        # Replace bt_date with DATE type
        if "bt_date" in out.columns:
            out["bt_date"] = pd.to_datetime(out["bt_date"], errors="coerce").dt.date
        # Align to table schema
        con.execute("SELECT * FROM fact_backtest_results LIMIT 0")  # verify table exists
        col_info = con.execute("PRAGMA table_info('fact_backtest_results')").fetchdf()
        table_cols = col_info.sort_values("cid")["name"].tolist()
        for c in table_cols:
            if c not in out.columns:
                out[c] = None
        out = out[table_cols]
        con.register("_bt", out)
        col_list = ", ".join([f'"{c}"' for c in table_cols])
        con.execute(f"INSERT INTO fact_backtest_results ({col_list}) SELECT {col_list} FROM _bt")
        con.execute("CHECKPOINT")
    finally:
        try:
            con.unregister("_bt")
        except Exception:
            pass
        con.close()


_DEFAULT_START = "2025-10-08"
_DEFAULT_END   = "2026-04-17"

def _date_range(start: str, end: str) -> list[pd.Timestamp]:
    start = (start or "").strip() or _DEFAULT_START
    end   = (end   or "").strip() or _DEFAULT_END
    s = pd.Timestamp(start).normalize()
    e = pd.Timestamp(end).normalize()
    if pd.isna(s) or pd.isna(e):
        raise ValueError(f"Unparseable date range: {start!r} → {end!r}")
    dates = []
    cur = s
    while cur <= e:
        dates.append(cur)
        cur += timedelta(days=1)
    return dates


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

@app.command()
def fetch_season(
    start: str = typer.Option("2025-10-08", help="Season start (YYYY-MM-DD)"),
    end: str   = typer.Option("2026-04-17", help="Season end (YYYY-MM-DD)"),
    out: str   = typer.Option("data/NHL_2526.csv", help="Output CSV path"),
):
    """Fetch full 25-26 season player stats from API → CSV.

    Run once before the backtest sweep. Skips dates with no games.
    Requires SPORTS_DATA_API_KEY in environment.
    """
    dates = _date_range(start, end)
    typer.echo(f"Fetching {len(dates)} dates ({start} → {end}) …")

    frames: list[pd.DataFrame] = []
    for dt in dates:
        date_str = dt.strftime("%Y-%m-%d")
        try:
            df = fetch_player_stats_by_date(date_str)
        except Exception as exc:
            typer.echo(f"  {date_str}: error — {exc}")
            continue

        if df.empty:
            typer.echo(f"  {date_str}: no games")
            continue

        df["date"] = dt
        frames.append(df)
        typer.echo(f"  {date_str}: {len(df)} player rows")

    if not frames:
        typer.echo("No data fetched.", err=True)
        raise typer.Exit(1)

    full = pd.concat(frames, ignore_index=True)
    # Compute hockey points = goals + assists (if not already correct)
    if "goals" in full.columns and "assists" in full.columns:
        full["points"] = (
            pd.to_numeric(full["goals"], errors="coerce").fillna(0)
            + pd.to_numeric(full["assists"], errors="coerce").fillna(0)
        )

    Path(out).parent.mkdir(parents=True, exist_ok=True)
    full.to_csv(out, index=False)
    typer.echo(f"\nSaved {len(full)} rows → {out}")


@app.command()
def reset_db(
    backup: bool = typer.Option(True, help="Backup existing DuckDB before resetting"),
    confirm: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt"),
):
    """Backup and reinitialise the DuckDB (clean slate for backtest sweep)."""
    db_path = Path(settings.DUCKDB_PATH)

    if not confirm:
        typer.confirm(
            f"This will WIPE {db_path}. A backup will be made if --backup. Continue?",
            abort=True,
        )

    if db_path.exists() and backup:
        bak = db_path.with_suffix(".duckdb.bak")
        shutil.copy2(db_path, bak)
        typer.echo(f"Backed up → {bak}")

    if db_path.exists():
        db_path.unlink()
        typer.echo(f"Removed {db_path}")

    init_db()
    typer.echo(f"Initialised fresh DuckDB at {db_path}")


@app.command()
def run(
    date: str   = typer.Argument(..., help="Backtest date (YYYY-MM-DD)"),
    last_csv: str  = typer.Option("data/NHL_2023_24.csv", help="Last-season CSV (optional — skipped if stub/missing)"),
    curr_csv: str  = typer.Option("data/NHL_2526.csv", help="Current-season CSV (full)"),
    bt_id: str  = typer.Option("", help="BT run ID (auto-generated if blank)"),
    alpha: float = typer.Option(1.0, help="Elfies v4 alpha exponent"),
    beta: float  = typer.Option(1.0, help="Elfies v4 beta exponent"),
    version: str = typer.Option("0.3.0"),
    no_store: bool = typer.Option(False, "--no-store", help="Skip DuckDB write (dry-run)"),
):
    """Run backtest for a single date and store results."""
    init_db()
    run_id = bt_id or str(uuid.uuid4())

    df_last = _load_csv(last_csv, "last-season CSV", optional=True)
    df_curr = _load_csv(curr_csv, "current-season CSV")

    typer.echo(f"[backtest] date={date}  bt_id={run_id[:8]}…")
    typer.echo(f"  Training slice: {len(df_last)} last-season + "
               f"{(df_curr['date'] < pd.Timestamp(date)).sum()} current-season rows")

    results, summary = run_single_date(
        bt_date=date,
        df_last_season=df_last,
        df_current_season=df_curr,
        bt_id=run_id,
        alpha=alpha,
        beta=beta,
        version=version,
    )

    if "error" in summary:
        typer.echo(f"  SKIPPED: {summary['error']}", err=True)
        raise typer.Exit(0)

    typer.echo(f"  Players predicted: {summary.get('n_players', 0)}")
    for k, v in summary.items():
        if k.startswith("spearman_elfies"):
            typer.echo(f"  {k}: {v}")
    for k, v in summary.items():
        if k.startswith("hit"):
            typer.echo(f"  {k}: {v}")

    if not no_store:
        _store_results(results)
        typer.echo(f"  Stored {len(results)} rows → fact_backtest_results")

    # Also write a per-date CSV artifact
    out_dir = Path(settings.PARQUET_DIR) / "backtest"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_csv = out_dir / f"bt_{date}_{run_id[:8]}.csv"
    results.to_csv(out_csv, index=False)
    typer.echo(f"  CSV → {out_csv}")


@app.command()
def sweep(
    start: str  = typer.Argument(..., help="Sweep start date (YYYY-MM-DD)"),
    end: str    = typer.Argument(..., help="Sweep end date (YYYY-MM-DD)"),
    last_csv: str  = typer.Option("data/NHL_2023_24.csv", help="Last-season CSV (optional — skipped if stub/missing)"),
    curr_csv: str  = typer.Option("data/NHL_2526.csv", help="Current-season CSV"),
    bt_id: str  = typer.Option("", help="BT sweep ID (auto-generated if blank)"),
    alpha: float = typer.Option(1.0),
    beta: float  = typer.Option(1.0),
    version: str = typer.Option("0.3.0"),
    min_players: int = typer.Option(5, help="Skip dates with fewer players than this"),
):
    """Walk-forward backtest over a date range (retrains model each day).

    Load last-season and current-season data once; iterate over each date
    in [start, end], filtering training data to before each date.

    Typical runtime: ~2–4 min/date × N dates in GitHub Actions.
    """
    init_db()
    run_id = bt_id or str(uuid.uuid4())
    typer.echo(f"[backtest sweep] {start} → {end}  bt_id={run_id}")
    typer.echo(f"  alpha={alpha}  beta={beta}")

    df_last = _load_csv(last_csv, "last-season CSV", optional=True)
    df_curr = _load_csv(curr_csv, "current-season CSV")

    dates = _date_range(start, end)
    # Only consider dates that have current-season data
    game_dates = set(df_curr["date"].dt.normalize().unique())
    dates = [d for d in dates if d in game_dates]
    typer.echo(f"  Dates with game data: {len(dates)}")

    summaries: list[dict] = []

    for i, dt in enumerate(dates, 1):
        date_str = dt.strftime("%Y-%m-%d")
        typer.echo(f"\n[{i}/{len(dates)}] {date_str}")

        results, summary = run_single_date(
            bt_date=date_str,
            df_last_season=df_last,
            df_current_season=df_curr,
            bt_id=run_id,
            alpha=alpha,
            beta=beta,
            version=version,
        )

        if "error" in summary:
            typer.echo(f"  SKIPPED: {summary['error']}")
            continue

        if summary.get("n_players", 0) < min_players:
            typer.echo(f"  SKIPPED: only {summary['n_players']} players")
            continue

        _store_results(results)
        summaries.append(summary)

        # Progress log — selection quality is the primary signal
        n    = summary.get("n_players", 0)
        l5   = summary.get("top_5_lift_hit1",  "n/a")
        l10  = summary.get("top_10_lift_hit1", "n/a")
        l20  = summary.get("top_20_lift_hit1", "n/a")
        avg5 = summary.get("top_5_avg_actual", "n/a")
        cal  = summary.get("calibration_80",   "n/a")
        typer.echo(f"  n={n}  lift_hit1 top5={l5} top10={l10} top20={l20}  avg_actual_top5={avg5}  cal={cal}")

    if not summaries:
        typer.echo("\nNo dates processed.", err=True)
        raise typer.Exit(1)

    # Aggregate summary CSV
    summary_df = pd.DataFrame(summaries)
    out_dir = Path(settings.PARQUET_DIR) / "backtest"
    out_dir.mkdir(parents=True, exist_ok=True)
    summary_csv = out_dir / f"sweep_summary_{run_id[:8]}.csv"
    summary_df.to_csv(summary_csv, index=False)

    # Print aggregate stats — selection quality first
    typer.echo(f"\n=== Sweep complete: {len(summaries)} dates ===")
    typer.echo(f"  BT ID: {run_id}")
    primary_cols = [
        "field_hit1_rate", "field_hit2_rate", "field_avg_actual",
        "top_5_hit1_rate",  "top_5_hit2_rate",  "top_5_avg_actual",  "top_5_lift_hit1",  "top_5_lift_avg",
        "top_10_hit1_rate", "top_10_hit2_rate", "top_10_avg_actual", "top_10_lift_hit1", "top_10_lift_avg",
        "top_20_hit1_rate", "top_20_hit2_rate", "top_20_avg_actual", "top_20_lift_hit1", "top_20_lift_avg",
        "calibration_80", "mae",
        "spearman_elfies_v2", "spearman_elfies_v1",
    ]
    for col in primary_cols:
        if col in summary_df.columns:
            mean_val = pd.to_numeric(summary_df[col], errors="coerce").mean()
            if pd.notna(mean_val):
                typer.echo(f"  mean {col}: {mean_val:.4f}")

    typer.echo(f"\nSummary CSV → {summary_csv}")
    typer.echo(f"Results stored in DuckDB: fact_backtest_results (bt_id={run_id})")


@app.command()
def score(
    bt_id: str   = typer.Argument(..., help="BT sweep ID to re-score"),
    alpha: float = typer.Option(1.0, help="Elfies v4 alpha exponent"),
    beta: float  = typer.Option(1.0, help="Elfies v4 beta exponent"),
    out_csv: str = typer.Option("", help="Output CSV path (optional)"),
):
    """Fast re-score: recompute elfies variants for a stored BT run without retraining.

    Reads raw (lambda_or_mu, q10, q90, p_ge_1) from fact_backtest_results,
    applies new formula parameters, and writes the rescored rows to a CSV for review.
    This is the inner loop for formula tuning — run after a full sweep is stored.
    """
    con = _connect()
    try:
        df = con.execute(
            "SELECT * FROM fact_backtest_results WHERE bt_id = ?", [bt_id]
        ).fetchdf()
    finally:
        con.close()

    if df.empty:
        typer.echo(f"No rows found for bt_id={bt_id}", err=True)
        raise typer.Exit(1)

    typer.echo(f"Loaded {len(df)} rows for bt_id={bt_id[:8]}…")
    typer.echo(f"Re-scoring with alpha={alpha}  beta={beta}")

    df = add_all_elfies_variants(df, alpha=alpha, beta=beta)

    # Recompute hit flags + errors (actuals already stored)
    if "actual" in df.columns:
        df["hit_1pt"]     = df["actual"] >= 1.0
        df["hit_2pt"]     = df["actual"] >= 2.0
        df["in_interval"] = (df["actual"] >= df["q10"]) & (df["actual"] <= df["q90"])
        df["abs_error"]   = (df["actual"] - df["lambda_or_mu"]).abs()

    # Aggregate metrics per date
    summaries = []
    for bt_date, grp in df.groupby("bt_date"):
        s = {"bt_date": bt_date, "alpha": alpha, "beta": beta}
        s.update(compute_day_metrics(grp))
        summaries.append(s)

    summary_df = pd.DataFrame(summaries)

    # Print aggregate — selection quality first
    typer.echo(f"\n=== Re-score summary (α={alpha}, β={beta}) ===")
    primary_cols = [
        "field_hit1_rate", "field_hit2_rate", "field_avg_actual",
        "top_5_hit1_rate",  "top_5_hit2_rate",  "top_5_avg_actual",  "top_5_lift_hit1",  "top_5_lift_avg",
        "top_10_hit1_rate", "top_10_hit2_rate", "top_10_avg_actual", "top_10_lift_hit1", "top_10_lift_avg",
        "top_20_hit1_rate", "top_20_hit2_rate", "top_20_avg_actual", "top_20_lift_hit1", "top_20_lift_avg",
        "calibration_80", "mae",
        "spearman_elfies_v2",
    ]
    for col in primary_cols:
        if col in summary_df.columns:
            mean_val = pd.to_numeric(summary_df[col], errors="coerce").mean()
            if pd.notna(mean_val):
                typer.echo(f"  mean {col}: {mean_val:.4f}")

    # Write output
    if not out_csv:
        out_dir = Path(settings.PARQUET_DIR) / "backtest"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_csv = str(out_dir / f"rescore_{bt_id[:8]}_a{alpha}_b{beta}.csv")

    summary_df.to_csv(out_csv, index=False)
    typer.echo(f"\nSummary CSV → {out_csv}")

    # Also write full per-player rescored output
    detail_csv = out_csv.replace(".csv", "_detail.csv")
    df.to_csv(detail_csv, index=False)
    typer.echo(f"Detail CSV  → {detail_csv}")


if __name__ == "__main__":
    app()
