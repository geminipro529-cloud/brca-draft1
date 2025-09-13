#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Build a canonical sample_id crosswalk from structured folders.
- Detect ID-like columns across CSV/TSV/Parquet
- Normalize TCGA barcodes (patient-level=first 12, sample-level=first 16)
- Keep model/depmap as-is when no mapping exists (still useful)
- Emit: crosswalk.parquet/csv with columns:
    source_file, source_col, source_value, sample_id, level, strategy, confidence
"""

from __future__ import annotations
import argparse, os, glob, re
from pathlib import Path
from typing import List, Tuple

import polars as pl
from rich.console import Console
from rich.progress import Progress, BarColumn, TimeElapsedColumn, TimeRemainingColumn, TextColumn

console = Console()

ID_COL_HINTS = [
    "sample_id","tumor_sample_barcode","tsb","patient_id","case_id",
    "model_id","depmap_id","cell_id","cell_line","specimen_id","barcode"
]
TCGA_RE = re.compile(r"^TCGA-[A-Z0-9]{2}-[A-Z0-9]{4}(-[A-Z0-9]{2}[A-Z0-9]?(-[A-Z0-9]{2}[A-Z0-9]?)?)?", re.I)

def discover(root: str) -> List[str]:
    pats = ("**/*.parquet","**/*.csv","**/*.tsv")
    out = []
    for p in pats:
        out += glob.glob(os.path.join(root, p), recursive=True)
    return sorted(set(out))

def relpath(p: str, root: str) -> str:
    try: return os.path.relpath(p, root).replace("\\","/")
    except Exception: return p.replace("\\","/")

def first_nonempty_texts(s: pl.Series, k=5) -> List[str]:
    return [str(x) for x in s.cast(pl.Utf8).drop_nulls().unique().head(k).to_list()]

def load_head(path: str, n: int = 20000) -> pl.DataFrame:
    try:
        if path.endswith(".parquet"):
            return pl.scan_parquet(path).limit(n).collect(streaming=True)
        sep = "\t" if path.lower().endswith(".tsv") else None
        lf = pl.scan_csv(path, separator=sep, infer_schema_length=5000).limit(n)
        return lf.collect(streaming=True)
    except Exception:
        try:
            return pl.read_csv(path, separator=("\t" if path.lower().endswith(".tsv") else ","), infer_schema_length=5000, n_rows=n)
        except Exception:
            return pl.DataFrame()

def tcga_levels(s: str) -> Tuple[str,str]:
    """
    Return (patient_level, sample_level) if looks like TCGA; else ("","")
    patient_level: first 12 chars; sample_level: first 16 chars
    """
    if not s: return "",""
    m = TCGA_RE.match(s)
    if not m: return "",""
    s = s.upper()
    pat = s[:12]
    samp = s[:16] if len(s) >= 16 else ""
    return pat, samp

def main():
    ap = argparse.ArgumentParser(description="Build canonical sample_id crosswalk from structured data.")
    ap.add_argument("--in-old", default="", help="data/02_interim/structured_annotated (optional)")
    ap.add_argument("--in-v2",  default="", help="data/02_interim/structured_annotated_v2 (optional)")
    ap.add_argument("--out",    required=True, help="output dir for crosswalk")
    ap.add_argument("--prefer", choices=["sample","patient"], default="sample",
                    help="When TCGA both levels present, prefer which as canonical sample_id")
    ap.add_argument("--row-sample", type=int, default=50000)
    args = ap.parse_args()

    os.makedirs(args.out, exist_ok=True)
    roots = [p for p in (args.in_v2, args.in_old) if p]
    files = []
    for r in roots: files += discover(r)
    files = sorted(set(files))
    if not files:
        console.print("[red]No inputs found.[/red]"); return

    rows = []
    bar = BarColumn(bar_width=None)
    with Progress(TextColumn("[progress.description]{task.description}"), bar,
                  TimeElapsedColumn(), TimeRemainingColumn(), console=console) as prog:
        task = prog.add_task("[bold cyan]Scanning for IDs", total=len(files))
        for p in files:
            try:
                root = args.in_v2 if p.startswith(os.path.abspath(args.in_v2) if args.in_v2 else "") else (args.in_old or "")
                rel = relpath(p, root or os.path.dirname(p))
                df = load_head(p, args.row_sample)
                if df.is_empty():
                    prog.advance(task); continue
                cols = {c.lower(): c for c in df.columns}
                cand = [cols[k] for k in cols if any(h in k for h in ID_COL_HINTS)]
                if not cand:
                    prog.advance(task); continue
                for c in cand:
                    s = df[c].cast(pl.Utf8).drop_nulls()
                    # sample a reasonable set of values
                    for val in s.unique().head(50000).to_list():
                        if not val: continue
                        valu = str(val).strip()
                        pat, samp = tcga_levels(valu)
                        strategy = ""
                        sample_id = ""
                        level = ""
                        conf = 0.6
                        if pat or samp:
                            if args.prefer == "sample" and samp:
                                sample_id, level, conf, strategy = samp, "tcga_sample", 0.95, "tcga_16"
                            elif pat:
                                sample_id, level, conf, strategy = pat, "tcga_patient", 0.9, "tcga_12"
                            else:
                                sample_id, level, conf, strategy = valu.upper(), "tcga_raw", 0.7, "tcga_raw"
                        else:
                            # accept other IDs as-is (can be mapped later)
                            sample_id, level, conf, strategy = valu, c.lower(), 0.7, "verbatim"
                        rows.append({
                            "source_file": rel, "source_col": c, "source_value": valu,
                            "sample_id": sample_id, "level": level, "strategy": strategy, "confidence": conf
                        })
            except Exception:
                pass
            finally:
                prog.advance(task)

    cw = pl.DataFrame(rows) if rows else pl.DataFrame({"sample_id":[]})
    if cw.is_empty():
        console.print("[yellow]No ID candidates found.[/yellow]"); return

    # prefer highest confidence per (source_file, source_col, source_value)
    cw = (cw.sort("confidence", descending=True)
            .unique(subset=["source_file","source_col","source_value"], keep="first"))

    # also provide a collapsed view: source_value -> canonical sample_id (highest frequency/score wins)
    best = (cw.group_by(["source_col","source_value","sample_id","level"])
              .agg(pl.col("confidence").mean().alias("mean_conf"), pl.len().alias("n"))
              .sort(["n","mean_conf"], descending=True)
              .unique(subset=["source_col","source_value"], keep="first"))

    out_pq = os.path.join(args.out, "crosswalk.parquet")
    out_csv = os.path.join(args.out, "crosswalk.csv")
    cw.write_parquet(out_pq); cw.write_csv(out_csv)
    best.write_csv(os.path.join(args.out, "crosswalk_best.csv"))
    console.rule("[bold green]Crosswalk built")
    console.print(f"[green]Rows:[/green] {cw.height}  → {out_csv}")

if __name__ == "__main__":
    main()
