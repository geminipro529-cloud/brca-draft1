#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Merge TWO structured-annotation folders into a single master table.

- Inputs:
    --in-old   : older/unlabeled structured outputs (csv/parquet)
    --in-v2    : new v2 structured outputs (csv/parquet)
- Behavior:
    * Discovers all files in both trees.
    * For the same file stem in both, **prefers v2**.
    * Attempts to find a sample id column (configurable candidates).
    * Aggregates per file (mean for numeric, any for bool, first for strings, union for lists).
    * Prefixes columns with sanitized file stem to avoid collisions.
    * Outer-joins all sources on sample_id → one unified master.
    * Anything without a usable ID is logged to `unlabeled_catalog.csv`.

- Outputs (to --out):
    - master_structured_merged.parquet (+ .csv)
    - master_structured_merged_report.md
    - unlabeled_catalog.csv   (files/rows we couldn’t tie to a sample_id)

Tip: Keep caselist/metadata ingestion as a separate step (build_caselist_index.py),
then merge caselist features into the master with merge_caselist_into_master.py.
"""

from __future__ import annotations
import argparse, os, glob, re
from pathlib import Path
from typing import List, Tuple, Optional

import polars as pl
from rich.console import Console
from rich.progress import Progress, BarColumn, TimeElapsedColumn, TimeRemainingColumn, TextColumn

console = Console()

DEFAULT_ID_CANDIDATES = [
    "sample_id","model_id","tumor_sample_barcode","tsb","patient_id",
    "case_id","depmap_id","sample","model","cell_id","cell_line","specimen_id"
]

def safe_mkdir(p: str) -> None:
    os.makedirs(p, exist_ok=True)

def discover_inputs(in_dir: str) -> Tuple[list[str], list[str]]:
    pq = sorted(set(glob.glob(os.path.join(in_dir, "**", "*.parquet"), recursive=True)))
    csv = sorted(set(glob.glob(os.path.join(in_dir, "**", "*.csv"), recursive=True)))
    return pq, csv

def stem_key(p: str) -> str:
    # normalized stem to dedupe v1/v2 collisions
    return Path(p).with_suffix("").name.lower()

def pick_id_column(df: pl.DataFrame, candidates: List[str]) -> Optional[str]:
    cols = {c.lower(): c for c in df.columns}
    for k in candidates:
        if k.lower() in cols:
            return cols[k.lower()]
    return None

def sanitize_prefix(path: str) -> str:
    stem = Path(path).stem
    stem = re.sub(r"_annotated$", "", stem)
    return re.sub(r"[^A-Za-z0-9]+", "_", stem).strip("_").lower()

def load_one(path: str) -> pl.DataFrame:
    if path.endswith(".parquet"):
        return pl.read_parquet(path)
    df = pl.read_csv(path, infer_schema_length=10000)
    # our annotators write list-like cols as '|' joined in CSV; convert back if present
    for col in ("mentions","normalized_terms"):
        if col in df.columns and df.schema.get(col) != pl.List(pl.Utf8):
            df = df.with_columns(pl.col(col).cast(pl.Utf8).str.split("|").alias(col))
    return df

def agg_by_id(df: pl.DataFrame, id_col: str) -> pl.DataFrame:
    aggs = []
    for c, dt in zip(df.columns, df.dtypes):
        if c == id_col: 
            continue
        
        # CORRECTED: Properly check for numeric types
        if dt.is_numeric():
            aggs.append(pl.col(c).cast(pl.Float64).mean().alias(c))
        elif dt == pl.Boolean:
            aggs.append(pl.col(c).any().alias(c))
        # CORRECTED: Use modern isinstance check for List dtype
        elif isinstance(dt, pl.List):
            aggs.append(pl.col(c).list.explode().drop_nulls().unique().alias(c))
        else:
            aggs.append(pl.col(c).forward_fill().backward_fill().first().alias(c))
            
    if not aggs:
        return df.unique(subset=[id_col])
        
    g = df.group_by(id_col).agg(aggs)
    return g

def prefix_cols(df: pl.DataFrame, id_col: str, prefix: str) -> pl.DataFrame:
    ren = {c: f"{prefix}__{c}" for c in df.columns if c != id_col}
    return df.rename(ren)

def choose_sources(old_dir: Optional[str], v2_dir: Optional[str]) -> list[tuple[str,str]]:
    """
    Returns list of (path, tag) with tag in {"v2","old"} and v2 wins on stem collisions.
    """
    items: dict[str, tuple[str,str]] = {}
    if v2_dir:
        for p in discover_inputs(v2_dir)[0] + discover_inputs(v2_dir)[1]:
            items[stem_key(p)] = (p, "v2")
    if old_dir:
        for p in discover_inputs(old_dir)[0] + discover_inputs(old_dir)[1]:
            sk = stem_key(p)
            if sk not in items:   # only take if not overridden by v2
                items[sk] = (p, "old")
    # keep deterministic order: v2 first then old
    return sorted(items.values(), key=lambda x: (0 if x[1]=="v2" else 1, x[0]))

def main():
    ap = argparse.ArgumentParser(description="Merge old (unlabeled) and v2 structured outputs into one master.")
    ap.add_argument("--in-old", default="", help="Older/unlabeled structured outputs dir (optional)")
    ap.add_argument("--in-v2",  default="", help="New v2 structured outputs dir (preferred)")
    ap.add_argument("--out",    required=True, help="Output folder (e.g., data/04_model_ready)")
    ap.add_argument("--id-candidates", nargs="+", default=DEFAULT_ID_CANDIDATES,
                    help="Ordered list of possible sample id column names.")
    ap.add_argument("--csv", choices=["flat","none"], default="flat", help="Also write CSV for the master.")
    args = ap.parse_args()

    if not args.in_old and not args.in_v2:
        console.print("[red]Provide at least one input folder.[/red]")
        raise SystemExit(1)

    out_dir = os.path.abspath(args.out); safe_mkdir(out_dir)
    sources = choose_sources(args.in_old or None, args.in_v2 or None)
    if not sources:
        console.print("[red]No inputs discovered.[/red]"); raise SystemExit(1)

    bar = BarColumn(bar_width=None)
    cols = [TextColumn("[progress.description]{task.description}"), bar, TimeElapsedColumn(), TimeRemainingColumn()]
    master: Optional[pl.DataFrame] = None
    unlabeled_rows: list[dict] = []

    with Progress(*cols, console=console) as prog:
        t = prog.add_task("[bold cyan]Merging structured sources (v2 wins)", total=len(sources))
        for path, tag in sources:
            try:
                df = load_one(path)
                if df.is_empty():
                    prog.advance(t); continue
                # normalize column names to lowercase for id detection/prefix safety
                df = df.rename({c: c.lower() for c in df.columns})
                id_col = pick_id_column(df, args.id_candidates)

                if id_col is None:
                    # Unlabeled → log for follow-up, but keep provenance counts
                    unlabeled_rows.append({"file": path, "tag": tag, "rows": df.height})
                    prog.advance(t); continue

                # aggregate within file (if duplicates per id)
                if df.select(pl.col(id_col)).n_unique() < df.height:
                    df = agg_by_id(df, id_col)

                prefix = f"{tag}__{sanitize_prefix(path)}"
                dfp = prefix_cols(df, id_col, prefix)

                # merge into master (outer join)
                if master is None:
                    master = dfp
                else:
                    master = master.join(dfp, on=id_col, how="outer")

            except Exception as e:
                unlabeled_rows.append({"file": path, "tag": tag, "rows": 0, "error": str(e)})
            finally:
                prog.advance(t)

    if master is None:
        console.print("[red]Nothing with a usable sample ID to merge.[/red]")
        if unlabeled_rows:
            pl.DataFrame(unlabeled_rows).write_csv(os.path.join(out_dir, "unlabeled_catalog.csv"))
        raise SystemExit(1)

    # Harmonize id column name to 'sample_id'
    id_col = None
    for c in args.id_candidates:
        if c in master.columns:
            id_col = c; break
    if id_col is None:
        id_col = master.columns[0]
    if id_col != "sample_id":
        master = master.rename({id_col: "sample_id"})

    # Write outputs
    out_parq = os.path.join(out_dir, "master_structured_merged.parquet")
    master.write_parquet(out_parq)
    if args.csv != "none":
        flat = master
        for c, dt in zip(flat.columns, flat.dtypes):
            if isinstance(dt, pl.List):
                flat = flat.with_columns(pl.col(c).cast(pl.List(pl.Utf8)).list.join("|").alias(c))
        flat.write_csv(os.path.join(out_dir, "master_structured_merged.csv"))

    # Logs & report
    if unlabeled_rows:
        pl.DataFrame(unlabeled_rows).write_csv(os.path.join(out_dir, "unlabeled_catalog.csv"))

    with open(os.path.join(out_dir, "master_structured_merged_report.md"), "w", encoding="utf-8") as f:
        f.write("# Master Structured (merged) Report\n\n")
        f.write(f"- Rows (unique samples): **{master.height}**\n")
        f.write(f"- Columns: **{len(master.columns)}**\n")
        if unlabeled_rows:
            f.write(f"- Files without usable IDs logged in `unlabeled_catalog.csv` (count: {len(unlabeled_rows)})\n")

    console.rule("[bold green]master_structured_merged built (v2 precedence)")

if __name__ == "__main__":
    main()
