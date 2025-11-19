#!/usr/bin/env python
"""
WhiteShorts pattern miner for predictions_for_broadcast

- Loads data from Supabase or from CSV
- Applies filters:
    crowd_flag_game_total != true
    flag_not_playing != false   (i.e. ignore records where flag_not_playing is false)
- Uses columns:
    q10, q90, lambda_or_mu, target, elfies_number (if present)
    actual_points (outcome)
- Computes:
    spread = q90 - q10
    is_1_plus = actual_points >= 1
    is_2_plus = actual_points >= 2
- Produces:
    CSV: pattern tables for 1+ and 2+ points
    HTML: Plotly charts (heatmaps + coeffs)
    Markdown: summary of strongest patterns
    
    
    ***********
    **Run from SupaBAse
      python ws_pattern_miner.py \
        --table predictions_for_broadcast \
        --output-dir ws_pattern_output \
        --min-n 30        # require at least 30 rows per pattern

    
    ***********
    **Run from csv
    
      python ws_pattern_miner.py \
        --input-csv predictions_for_broadcast.csv \
        --output-dir ws_pattern_output \
        --min-n 30
    ***********
Requires:
    pip install supabase-py pandas numpy scikit-learn plotly
"""

import argparse
import os
from pathlib import Path
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.tree import DecisionTreeClassifier, export_text
import plotly.express as px
import plotly.graph_objects as go

try:
    from supabase import create_client, Client  # type: ignore
    HAS_SUPABASE = True
except ImportError:
    HAS_SUPABASE = False


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_from_supabase(
    url: str,
    key: str,
    table: str = "predictions_for_broadcast",
    limit: Optional[int] = None,
) -> pd.DataFrame:
    if not HAS_SUPABASE:
        raise RuntimeError(
            "supabase-py is not installed. Install with: pip install supabase-py"
        )

    supabase: Client = create_client(url, key)
    query = supabase.table(table).select(
        "q10,q90,target,lambda_or_mu,crowd_flag_game_total,flag_not_playing,"
        "actual_points,elfies_number"
    )
    if limit:
        query = query.limit(limit)
    resp = query.execute()
    data = resp.data
    if not data:
        raise RuntimeError("No data returned from Supabase.")
    return pd.DataFrame(data)


def load_data(
    input_csv: Optional[str],
    supabase_url: Optional[str],
    supabase_key: Optional[str],
    table: str,
    limit: Optional[int],
) -> pd.DataFrame:
    if input_csv:
        df = pd.read_csv(input_csv)
    else:
        if not supabase_url or not supabase_key:
            raise ValueError(
                "Either --input-csv must be provided OR SUPABASE_URL & SUPABASE_KEY / args must be set."
            )
        df = load_from_supabase(supabase_url, supabase_key, table=table, limit=limit)

    return df


# ---------------------------------------------------------------------------
# Preprocessing & feature engineering
# ---------------------------------------------------------------------------

def preprocess(df: pd.DataFrame) -> pd.DataFrame:
    # enforce required columns presence
    required = ["q10", "q90", "lambda_or_mu", "target", "actual_points",
                "crowd_flag_game_total", "flag_not_playing"]
    for col in required:
        if col not in df.columns:
            raise ValueError(f"Missing required column '{col}' in dataframe.")

    # filters
    # ignore ALL records where crowd_flag_game_total = true
    df = df.loc[~df["crowd_flag_game_total"].astype("boolean").fillna(False)]

    # ignore ALL records where flag_not_playing = false
    # (i.e. keep only flag_not_playing == True or NULL)
    df = df.loc[~(df["flag_not_playing"].astype("boolean") == False)]

    # remove rows with missing actual_points
    df = df[df["actual_points"].notna()].copy()

    # ensure numeric types
    num_cols = ["q10", "q90", "lambda_or_mu", "actual_points"]
    if "elfies_number" in df.columns:
        num_cols.append("elfies_number")
    for c in num_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.dropna(subset=["q10", "q90", "lambda_or_mu"])

    # spread
    df["spread"] = df["q90"] - df["q10"]

    # outcomes
    df["is_1_plus"] = (df["actual_points"] >= 1).astype(int)
    df["is_2_plus"] = (df["actual_points"] >= 2).astype(int)

    # drop any rows that fail these transforms
    df = df.dropna(subset=["spread"])

    # standardise target as string
    df["target"] = df["target"].astype(str)

    return df


# ---------------------------------------------------------------------------
# Binning & cross-tab patterns
# ---------------------------------------------------------------------------

def bin_feature(
    s: pd.Series,
    bins: Optional[List[float]] = None,
    labels: Optional[List[str]] = None,
    q: Optional[int] = None,
    name: str = "",
) -> pd.Series:
    """
    Helper: either fixed bins or quantile bins.
    """
    if q is not None:
        # quantile binning
        try:
            return pd.qcut(s, q=q, duplicates="drop")
        except ValueError:
            # not enough unique values
            return pd.cut(s, bins=min(q, s.nunique()), include_lowest=True)
    else:
        return pd.cut(s, bins=bins, labels=labels, include_lowest=True)


def add_binned_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # spread: fixed bins are usually intuitive
    spread_bins = [0, 0.5, 1.0, 1.5, 2.5, np.inf]
    df["spread_bin"] = bin_feature(df["spread"], bins=spread_bins)

    # lambda_or_mu: quantiles
    df["lambda_bin"] = bin_feature(df["lambda_or_mu"], q=4, name="lambda")

    # q10 / q90: coarse bins
    q10_bins = [-np.inf, 0.5, 1.0, 2.0, np.inf]
    q90_bins = [-np.inf, 1.0, 2.0, 3.0, np.inf]
    df["q10_bin"] = bin_feature(df["q10"], bins=q10_bins)
    df["q90_bin"] = bin_feature(df["q90"], bins=q90_bins)

    # elfies_number: if present, quantiles
    if "elfies_number" in df.columns:
        df["elfies_bin"] = bin_feature(df["elfies_number"], q=4, name="elfies")
    else:
        df["elfies_bin"] = pd.NA

    return df


def compute_pattern_table(
    df: pd.DataFrame,
    outcome: str,
    group_cols: List[str],
    min_n: int = 20,
) -> pd.DataFrame:
    """
    Aggregates probabilities for is_1_plus or is_2_plus across grouped bins.
    """
    grouped = (
        df.groupby(group_cols)
          .agg(
              n=("actual_points", "size"),
              mean_actual=("actual_points", "mean"),
              p_1_plus=("is_1_plus", "mean"),
              p_2_plus=("is_2_plus", "mean"),
          )
          .reset_index()
    )
    grouped = grouped[grouped["n"] >= min_n].copy()

    # sort by chosen outcome probability
    grouped = grouped.sort_values(
        "p_2_plus" if outcome == "is_2_plus" else "p_1_plus",
        ascending=False,
    )

    return grouped


# ---------------------------------------------------------------------------
# Logistic regression & decision tree (pattern explanation)
# ---------------------------------------------------------------------------

def prepare_ml_matrix(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Returns X (features) and y dataframe with is_1_plus, is_2_plus.
    Uses:
        lambda_or_mu, spread, q10, q90, elfies_number, target (one-hot)
    """
    used_cols = ["lambda_or_mu", "spread", "q10", "q90"]
    if "elfies_number" in df.columns:
        used_cols.append("elfies_number")

    num_df = df[used_cols].copy()
    # one-hot encode target
    target_dummies = pd.get_dummies(df["target"], prefix="target")
    X = pd.concat([num_df, target_dummies], axis=1)

    y = df[["is_1_plus", "is_2_plus"]].copy()
    return X, y


def fit_logistic_models(X: pd.DataFrame, y: pd.DataFrame):
    models = {}
    coef_tables = {}

    for col in ["is_1_plus", "is_2_plus"]:
        lr = LogisticRegression(
            penalty="l2",
            C=1.0,
            max_iter=500,
            solver="lbfgs",
        )
        lr.fit(X, y[col])
        models[col] = lr
        coef = pd.DataFrame(
            {
                "feature": X.columns,
                "coef": lr.coef_[0],
                "odds_ratio": np.exp(lr.coef_[0]),
            }
        ).sort_values("coef", key=lambda s: s.abs(), ascending=False)
        coef_tables[col] = coef

    return models, coef_tables


def fit_decision_tree(
    X: pd.DataFrame,
    y_binary: pd.Series,
    max_depth: int = 3,
    min_samples_leaf: int = 50,
):
    tree = DecisionTreeClassifier(
        max_depth=max_depth,
        min_samples_leaf=min_samples_leaf,
        random_state=42,
    )
    tree.fit(X, y_binary)
    return tree


# ---------------------------------------------------------------------------
# Outputs: CSV, HTML, Markdown
# ---------------------------------------------------------------------------

def ensure_dir(path: Path):
    path.mkdir(parents=True, exist_ok=True)


def save_csv(df: pd.DataFrame, path: Path):
    df.to_csv(path, index=False)
    print(f"[write] {path}")





def create_html_report(
    out_dir: Path,
    patterns_1: pd.DataFrame,
    patterns_2: pd.DataFrame,
    coef_1: pd.DataFrame,
    coef_2: pd.DataFrame,
):
    figs = []

    # Heatmap: elfies vs spread bins for 2+ points (if elfies present)
    if "elfies_bin" in patterns_2.columns and patterns_2["elfies_bin"].notna().any():
        heat_2 = (
            patterns_2.groupby(["elfies_bin", "spread_bin"])
            .agg(p_2_plus=("p_2_plus", "mean"), n=("n", "sum"))
            .reset_index()
        )
        fig_heat = px.imshow(
            heat_2.pivot(index="elfies_bin", columns="spread_bin", values="p_2_plus"),
            aspect="auto",
            color_continuous_scale="Viridis",
        )
        fig_heat.update_layout(
            title="P(actual_points ≥ 2) by elfies_bin vs spread_bin",
        )
        figs.append(fig_heat)

    # Bar plots of logistic coefficients (top 15 by abs(coef))
    for coef, name in [(coef_1, "is_1_plus"), (coef_2, "is_2_plus")]:
        top = coef.head(15).copy()
        fig_coef = go.Figure(
            data=go.Bar(
                x=top["feature"],
                y=top["coef"],
            )
        )
        fig_coef.update_layout(
            title=f"Logistic coefficients (top 15) for {name}",
            xaxis_title="Feature",
            yaxis_title="Coefficient",
        )
        figs.append(fig_coef)

    # Combine into a single HTML file
    html_path = out_dir / "pattern_report.html"
    with open(html_path, "w", encoding="utf-8") as f:
        f.write("<html><head><title>WhiteShorts Pattern Report</title></head><body>\n")
        f.write("<h1>WhiteShorts Pattern Report</h1>\n")
        f.write("<p>Interactive Plotly charts below.</p>\n")
        for i, fig in enumerate(figs):
            f.write(f"<h2>Figure {i+1}</h2>\n")
            f.write(fig.to_html(full_html=False, include_plotlyjs="cdn"))
            f.write("<hr/>\n")
        f.write("</body></html>")
    print(f"[write] {html_path}")


def create_markdown_summary(
    out_dir: Path,
    patterns_1: pd.DataFrame,
    patterns_2: pd.DataFrame,
    coef_1: pd.DataFrame,
    coef_2: pd.DataFrame,
):
    md_path = out_dir / "pattern_summary.md"

    def top_rows_md(df: pd.DataFrame, k: int = 10) -> str:
        return df.head(k).to_markdown(index=False)

    with open(md_path, "w", encoding="utf-8") as f:
        f.write("# WhiteShorts Pattern Summary\n\n")
        f.write("## Top patterns for actual_points ≥ 1\n\n")
        f.write(top_rows_md(patterns_1))
        f.write("\n\n---\n\n")
        f.write("## Top patterns for actual_points ≥ 2\n\n")
        f.write(top_rows_md(patterns_2))
        f.write("\n\n---\n\n")
        f.write("## Top logistic features for is_1_plus\n\n")
        f.write(coef_1.head(15).to_markdown(index=False))
        f.write("\n\n---\n\n")
        f.write("## Top logistic features for is_2_plus\n\n")
        f.write(coef_2.head(15).to_markdown(index=False))
        f.write("\n")

    print(f"[write] {md_path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(description="WhiteShorts pattern miner")
    p.add_argument("--input-csv", help="Optional local CSV instead of Supabase.")
    p.add_argument("--supabase-url", help="Supabase URL (or use SUPABASE_URL env)")
    p.add_argument("--supabase-key", help="Supabase anon/service key (or SUPABASE_KEY env)")
    p.add_argument("--table", default="predictions_for_broadcast")
    p.add_argument("--limit", type=int, help="Optional row limit from Supabase.")
    p.add_argument("--min-n", type=int, default=20, help="Min rows per pattern group.")
    p.add_argument("--output-dir", default="ws_pattern_output")
    return p.parse_args()


def main():
    args = parse_args()
    out_dir = Path(args.output_dir)
    ensure_dir(out_dir)

    supabase_url = args.supabase_url or os.getenv("SUPABASE_URL")
    supabase_key = args.supabase_key or os.getenv("SUPABASE_KEY")

    df_raw = load_data(
        input_csv=args.input_csv,
        supabase_url=supabase_url,
        supabase_key=supabase_key,
        table=args.table,
        limit=args.limit,
    )

    print(f"[info] Loaded {len(df_raw)} rows")
    df = preprocess(df_raw)
    print(f"[info] After filters: {len(df)} rows")

    df = add_binned_columns(df)

    # pattern tables
    group_cols = ["target", "elfies_bin", "spread_bin", "lambda_bin"]
    # guard for missing elfies_bin (if column not present)
    if "elfies_bin" not in df.columns:
        group_cols = ["target", "spread_bin", "lambda_bin"]

    patterns_1 = compute_pattern_table(df, "is_1_plus", group_cols, min_n=args.min_n)
    patterns_2 = compute_pattern_table(df, "is_2_plus", group_cols, min_n=args.min_n)

    save_csv(patterns_1, out_dir / "patterns_1_plus.csv")
    save_csv(patterns_2, out_dir / "patterns_2_plus.csv")

    # ML models
    X, y = prepare_ml_matrix(df)
    models, coef_tables = fit_logistic_models(X, y)
    coef_1 = coef_tables["is_1_plus"]
    coef_2 = coef_tables["is_2_plus"]

    save_csv(coef_1, out_dir / "logistic_features_1_plus.csv")
    save_csv(coef_2, out_dir / "logistic_features_2_plus.csv")

    # tree for 2+ points rules (optional but handy)
    tree = fit_decision_tree(X, y["is_2_plus"])
    rules_txt = export_text(tree, feature_names=list(X.columns))
    rules_path = out_dir / "tree_rules_2_plus.txt"
    with open(rules_path, "w", encoding="utf-8") as f:
        f.write(rules_txt)
    print(f"[write] {rules_path}")

    # HTML Plotly report + Markdown summary
    create_html_report(out_dir, patterns_1, patterns_2, coef_1, coef_2)
    create_markdown_summary(out_dir, patterns_1, patterns_2, coef_1, coef_2)

    print("[done] Pattern mining complete.")


if __name__ == "__main__":
    main()
