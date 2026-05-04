from __future__ import annotations
import os
from pathlib import Path
from typing import Optional

import typer

app = typer.Typer(help="Analysis commands")


@app.command()
def patterns(
    input_csv: Optional[str] = typer.Option(None, "--input-csv", help="Local CSV of predictions + actuals."),
    supabase_url: Optional[str] = typer.Option(None, "--supabase-url", help="Supabase project URL."),
    supabase_key: Optional[str] = typer.Option(None, "--supabase-key", help="Supabase anon/service key."),
    table: str = typer.Option("predictions_for_broadcast", "--table", help="Supabase table name."),
    limit: Optional[int] = typer.Option(None, "--limit", help="Max rows to fetch from Supabase."),
    min_n: int = typer.Option(20, "--min-n", help="Min rows required per pattern group."),
    n_bins: int = typer.Option(4, "--n-bins", help="Quantile bins for continuous features."),
    output_dir: str = typer.Option("data/dashboards/patterns", "--output-dir", help="Directory for output files."),
):
    """Mine peaks & troughs patterns from prediction history.

    Loads predictions + actuals, bins features using quantile-based cuts
    (self-calibrating across sports), fits logistic regression and a decision
    tree, then writes CSV pattern tables, an HTML report, and a Markdown
    summary to OUTPUT_DIR.
    """
    from ..analysis.ws_pattern_miner import (
        load_data,
        preprocess,
        add_binned_columns,
        compute_pattern_table,
        prepare_ml_matrix,
        fit_logistic_models,
        fit_decision_tree,
        create_html_report,
        create_markdown_summary,
        save_csv,
        ensure_dir,
    )
    from sklearn.tree import export_text

    out_dir = Path(output_dir)
    ensure_dir(out_dir)

    url = supabase_url or os.getenv("SUPABASE_URL")
    key = supabase_key or os.getenv("SUPABASE_KEY")

    df_raw = load_data(
        input_csv=input_csv,
        supabase_url=url,
        supabase_key=key,
        table=table,
        limit=limit,
    )
    typer.echo(f"Loaded {len(df_raw):,} rows")

    df = preprocess(df_raw)
    typer.echo(f"After filters: {len(df):,} rows")

    df = add_binned_columns(df, n_bins=n_bins)

    group_cols = ["target", "elfies_bin", "spread_bin", "lambda_bin"]
    if "elfies_bin" not in df.columns or df["elfies_bin"].isna().all():
        group_cols = ["target", "spread_bin", "lambda_bin"]

    patterns_1 = compute_pattern_table(df, "is_1_plus", group_cols, min_n=min_n)
    patterns_2 = compute_pattern_table(df, "is_2_plus", group_cols, min_n=min_n)

    save_csv(patterns_1, out_dir / "patterns_1_plus.csv")
    save_csv(patterns_2, out_dir / "patterns_2_plus.csv")

    X, y = prepare_ml_matrix(df)
    _, coef_tables = fit_logistic_models(X, y)
    coef_1 = coef_tables["is_1_plus"]
    coef_2 = coef_tables["is_2_plus"]

    save_csv(coef_1, out_dir / "logistic_features_1_plus.csv")
    save_csv(coef_2, out_dir / "logistic_features_2_plus.csv")

    if y["is_2_plus"].nunique() < 2:
        typer.echo("[warn] is_2_plus has only one class; skipping decision tree.")
    else:
        tree = fit_decision_tree(X, y["is_2_plus"])
        rules = export_text(tree, feature_names=list(X.columns))
        rules_path = out_dir / "tree_rules_2_plus.txt"
        rules_path.write_text(rules, encoding="utf-8")
        typer.echo(f"Wrote {rules_path}")

    create_html_report(out_dir, patterns_1, patterns_2, coef_1, coef_2)
    create_markdown_summary(out_dir, patterns_1, patterns_2, coef_1, coef_2)

    typer.echo("Pattern mining complete.")
