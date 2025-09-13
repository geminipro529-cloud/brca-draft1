
# -*- coding: utf-8 -*-
"""
Extract vocabulary candidates from structured data folders (CSV/TSV/Parquet + case_lists .txt)
and align them to an existing vocabulary.

Sources mined
  • Column names (descriptors/features)
  • Text/categorical values (UTF-8 columns)
  • 'normalized_terms' list columns (from your annotators)
  • Case list labels/IDs in cBioPortal-style text files (case_list_ids: ...)
\
Outputs
  out_dir/
    structured_vocab_raw.parquet      (one row per candidate occurrence)
    structured_vocab_enriched.parquet (deduped, with counts and alignment)
    structured_vocab_enriched.csv
    structured_vocab_report.md

Optional:
  --merge-to <path>   Write a merged vocabulary (existing + new) alongside existing file.

Notes
  • Parquet is source of truth; CSV is for eyeballing (no nested types).
  • Heuristics avoid ID columns; you can override with flags.
"""

from __future__ import annotations
import argparse, os, glob, re, json
from pathlib import Path
from typing import List, Dict, Optional, Tuple

import polars as pl
from rich.console import Console
from rich.progress import Progress, BarColumn, TimeElapsedColumn, TimeRemainingColumn, TextColumn

console = Console()

ID_HINTS = (
    "sample_id","model_id","tumor_sample_barcode","patient_id","case_id",
    "depmap_id","cell_id","cell_line","specimen_id","tsb","barcode"
)

CASELIST_HINTS = ("case_lists", "cases_", "caselist")
CBIO_CASELINE = re.compile(r"(?i)^case_list_ids:\s*(.*)$")
SPLIT_IDS = re.compile(r"[,\s;]+")

def safe_mkdir(p: str) -> None:
    os.makedirs(p, exist_ok=True)

def discover_files(root: str) -> Tuple[list[str], list[str], list[str]]:
    pq = sorted(set(glob.glob(os.path.join(root, "**", "*.parquet"), recursive=True)))
    csv = sorted(set(glob.glob(os.path.join(root, "**", "*.csv"), recursive=True) +
                     glob.glob(os.path.join(root, "**", "*.tsv"), recursive=True)))
    txt = sorted(set(glob.glob(os.path.join(root, "**", "*.txt"), recursive=True)))
    return pq, csv, txt

def relpath(p: str, root: str) -> str:
    try:
        return os.path.relpath(p, root).replace("\\", "/")
    except ValueError:
        return p.replace("\\", "/")

def is_id_column(col: str) -> bool:
    c = col.lower()
    return any(h in c for h in ID_HINTS)

def load_small_parquet(path: str, n_rows: int) -> pl.DataFrame:
    try:
        lf = pl.scan_parquet(path)
        if n_rows > 0:
            lf = lf.limit(n_rows)
        return lf.collect(streaming=True)
    except Exception:
        return pl.DataFrame()

def load_small_csv(path: str, n_rows: int) -> pl.DataFrame:
    sep = "\t" if path.lower().endswith(".tsv") else None
    try:
        if sep:
            lf = pl.scan_csv(path, separator="\t", infer_schema_length=5000)
        else:
            lf = pl.scan_csv(path, infer_schema_length=5000)
        if n_rows > 0:
            lf = lf.limit(n_rows)
        return lf.collect(streaming=True)
    except Exception:
        try:
            # fallback eager read (some quirky files)
            df = pl.read_csv(path, separator=("\t" if sep else ","), infer_schema_length=5000, n_rows=(n_rows or None))
            return df
        except Exception:
            return pl.DataFrame()

def parse_caselist_txt(path: str) -> Tuple[str, List[str]]:
    """Return (label, sample_ids) if found."""
    try:
        text = Path(path).read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return "", []
    label = Path(path).stem
    ids: List[str] = []
    for ln in text.splitlines():
        m = CBIO_CASELINE.match(ln.strip())
        if m:
            ids = [t for t in SPLIT_IDS.split(m.group(1).strip()) if t]
            break
    if not ids:
        # one-id-per-line fallback (ignore coloned lines)
        ids = [ln.strip() for ln in text.splitlines() if ln.strip() and ":" not in ln]
    ids = [i for i in ids if len(i) >= 3]
    return label, ids

def clean_term(s: str) -> str:
    s = str(s).strip()
    s = re.sub(r"\s+", " ", s)
    return s

def surface_ok(s: str, min_len: int) -> bool:
    if not s or len(s) < min_len: return False
    # drop obvious junk tokens
    if s.lower() in ("na","n/a","null","none","unknown"): return False
    return True

def load_existing_vocab(vpath: str) -> Tuple[pl.DataFrame, Dict[str, Tuple[str,str]]]:
    if not vpath or not os.path.exists(vpath):
        return pl.DataFrame({"term":[],"canonical":[],"term_type":[]}), {}
    v = pl.read_parquet(vpath) if vpath.lower().endswith(".parquet") else pl.read_csv(vpath)
    # normalize columns
    if "term" not in v.columns and "canonical" in v.columns:
        v = v.rename({"canonical":"term"})
    for need in ("term","canonical","term_type"):
        if need not in v.columns:
            if need == "canonical":
                v = v.with_columns(pl.col("term").alias("canonical"))
            elif need == "term_type":
                v = v.with_columns(pl.lit("unknown").alias("term_type"))
            else:
                v = v.with_columns(pl.lit(None).alias("term"))
    v = v.select([
        pl.col("term").cast(pl.Utf8).str.strip().alias("term"),
        pl.col("canonical").cast(pl.Utf8).str.strip().alias("canonical"),
        pl.col("term_type").cast(pl.Utf8).str.strip().fill_null("unknown").alias("term_type")
    ]).drop_nulls(subset=["term"])
    # fast lookup by lower(term)
    lut: Dict[str, Tuple[str,str]] = {}
    for t, c, tt in zip(v["term"].to_list(), v["canonical"].to_list(), v["term_type"].to_list()):
        if isinstance(t, str):
            lut[t.lower()] = (c or t, tt or "unknown")
    # also allow canonical as key
    for c, tt in zip(v["canonical"].to_list(), v["term_type"].to_list()):
        if isinstance(c, str):
            lut.setdefault(c.lower(), (c, tt or "unknown"))
    return v, lut

def main():
    ap = argparse.ArgumentParser(description="Extract vocabulary from structured data (headers, values, normalized_terms, caselists).")
    ap.add_argument("--in",  dest="in_dir", required=True, help="structured folder (e.g., data/02_interim/structured_annotated_v2)")
    ap.add_argument("--out", dest="out_dir", required=True, help="output folder (e.g., data/02_interim/VOCAB_FROM_STRUCTURED)")
    ap.add_argument("--existing-vocab", default="", help="existing vocab (vocabulary_enriched.parquet/csv) to align types/canonical")
    ap.add_argument("--row-sample", type=int, default=100000, help="max rows per file to sample (0=all)")
    ap.add_argument("--min-len", type=int, default=3, help="min term length")
    ap.add_argument("--include-headers", action="store_true", help="include column names as terms")
    ap.add_argument("--include-values", action="store_true", help="include text/categorical values")
    ap.add_argument("--include-normalized", action="store_true", help="include normalized_terms list column")
    ap.add_argument("--merge-to", default="", help="optional path to existing vocab to write a merged copy next to it")
    ap.add_argument("--csv", choices=["flat","none"], default="flat", help="also write CSV for enriched output")
    args = ap.parse_args()

    safe_mkdir(args.out_dir)
    V_exist, canon_lut = load_existing_vocab(args.existing_vocab)

    pq, csvs, txts = discover_files(args.in_dir)
    files = [(p, "parquet") for p in pq] + [(p, "csv") for p in csvs] + [(p, "txt") for p in txts]
    if not files:
        console.print(f"[red]No inputs found under {args.in_dir}[/red]")
        raise SystemExit(1)

    # storage rows
    RAW_ROWS: List[dict] = []
    bar = BarColumn(bar_width=None)
    cols = [TextColumn("[progress.description]{task.description}"), bar, TimeElapsedColumn(), TimeRemainingColumn()]

    with Progress(*cols, console=console) as prog:
        t = prog.add_task("[bold cyan]Scanning structured for vocab", total=len(files))
        for path, kind in files:
            rel = relpath(path, args.in_dir)
            stem = Path(path).stem
            try:
                if kind == "txt":
                    # case lists (labels/IDs are "terms" of type caselist)
                    label, ids = parse_caselist_txt(path)
                    if ids:
                        RAW_ROWS.append({
                            "term": clean_term(label), "origin": "caselist_label",
                            "term_type": "caselist", "canonical": clean_term(label),
                            "source_file": rel, "context": "case_list_label", "freq": 1
                        })
                    # Optionally include the literal ID tokens as terms? Typically no – they are sample IDs.
                    # We skip IDs; they are not vocabulary concepts.
                    prog.advance(t); continue

                # load a small frame
                df = load_small_parquet(path, args.row_sample) if kind == "parquet" else load_small_csv(path, args.row_sample)
                if df.is_empty():
                    prog.advance(t); continue

                # 1) column headers
                if args.include_headers:
                    for col in df.columns:
                        if is_id_column(col):  # skip obvious IDs
                            continue
                        term = clean_term(col)
                        if surface_ok(term, args.min_len):
                            RAW_ROWS.append({
                                "term": term, "origin": "colname",
                                "term_type": "", "canonical": "", "source_file": rel,
                                "context": f"header:{col}", "freq": 1
                            })

                # 2) normalized_terms column (list or pipe-joined)
                if args.include_normalized and "normalized_terms" in df.columns:
                    s = df["normalized_terms"]
                    if s.dtype == pl.List(pl.Utf8):
                        vals = (df.select(pl.col("normalized_terms").explode().drop_nulls())
                                  .to_series().to_list())
                    else:
                        # pipe-joined fallback
                        vals = (df.select(pl.col("normalized_terms").cast(pl.Utf8))
                                  .drop_nulls()
                                  .to_series().str.split("|").explode().drop_nulls().to_list())
                    for v in vals:
                        v2 = clean_term(v)
                        if surface_ok(v2, args.min_len):
                            RAW_ROWS.append({
                                "term": v2, "origin": "normalized_terms",
                                "term_type": "", "canonical": "", "source_file": rel,
                                "context": "normalized_terms", "freq": 1
                            })

                # 3) values from UTF8 columns (categorical/text)
                if args.include_values:
                    for col, dt in zip(df.columns, df.dtypes):
                        if is_id_column(col):          # skip IDs
                            continue
                        if dt != pl.Utf8:              # only textual columns
                            continue
                        # unique values (sampled)
                        vals = (df.select(pl.col(col).cast(pl.Utf8))
                                  .drop_nulls()
                                  .unique()
                                  .head(20000)  # cap per column to keep reasonable
                                  [col].to_list())
                        for v in vals:
                            term = clean_term(v)
                            if surface_ok(term, args.min_len):
                                RAW_ROWS.append({
                                    "term": term, "origin": "value",
                                    "term_type": "", "canonical": "", "source_file": rel,
                                    "context": f"value_col:{col}", "freq": 1
                                })

            except Exception as e:
                console.log(f"[yellow]Skip {rel}[/yellow]: {e}")
            finally:
                prog.advance(t)

    if not RAW_ROWS:
        console.print("[yellow]No vocabulary candidates found.[/yellow]")
        return

    raw_df = pl.DataFrame(RAW_ROWS)

    # aggregate counts by (term, origin)
    agg = (raw_df
           .group_by(["term","origin"])
           .agg([
               pl.len().alias("n_occurrences"),
               pl.col("source_file").n_unique().alias("n_files"),
               pl.col("context").n_unique().alias("n_contexts"),
               pl.col("source_file").unique().alias("example_files")
           ])
           .sort("n_occurrences", descending=True))

    # align to existing vocab (canonical + term_type)
    def align(term: str) -> Tuple[str, str]:
        hit = canon_lut.get(term.lower())
        if hit:
            return hit[0], hit[1]
        # light heuristic for term_type if unseen
        low = term.lower()
        if any(x in low for x in (" pathway"," signaling","signalling")): return term, "pathway"
        if any(x in low for x in (" carcinoma"," cancer"," tumor"," tumour"," neoplasm")): return term, "disease"
        if low.isupper() and 2 <= len(low) <= 7: return term, "gene"
        if any(x in low for x in (" inhibitor"," antibody"," drug"," therapy")): return term, "drug"
        return term, ""

    aligned = agg.with_columns([
        pl.col("term").map_elements(lambda s: align(s)[0], return_dtype=pl.Utf8).alias("canonical"),
        pl.col("term").map_elements(lambda s: align(s)[1], return_dtype=pl.Utf8).alias("term_type")
    ])

    # flatten example files for CSV
    enriched = (aligned
                .with_columns(pl.col("example_files").cast(pl.List(pl.Utf8)).list.slice(0,3).alias("example_files"))
                .with_columns(pl.col("example_files").cast(pl.List(pl.Utf8)).list.join("|").alias("example_files")))

    # write outputs
    raw_out = os.path.join(args.out_dir, "structured_vocab_raw.parquet")
    raw_df.write_parquet(raw_out)

    enr_pq = os.path.join(args.out_dir, "structured_vocab_enriched.parquet")
    enriched.write_parquet(enr_pq)
    if args.csv != "none":
        enriched.write_csv(os.path.join(args.out_dir, "structured_vocab_enriched.csv"))

    # small report
    by_origin = (enriched.group_by("origin").agg(pl.len().alias("n")).sort("n", descending=True))
    by_type = (enriched.group_by("term_type").agg(pl.len().alias("n")).sort("n", descending=True))
    with open(os.path.join(args.out_dir, "structured_vocab_report.md"), "w", encoding="utf-8") as f:
        f.write("# Structured Vocab Extraction\n\n")
        f.write(f"- Candidates (raw rows): **{len(RAW_ROWS)}**\n")
        f.write(f"- Unique candidates: **{enriched.height}**\n\n")
        f.write("## By origin\n")
        if not by_origin.is_empty(): f.write(by_origin.to_pandas().to_markdown(index=False))
        f.write("\n\n## By term_type (aligned)\n")
        if not by_type.is_empty(): f.write(by_type.to_pandas().to_markdown(index=False))
        f.write("\n")

    console.log(f"[green]Raw:[/green] {raw_out}")
    console.log(f"[green]Enriched:[/green] {enr_pq}")

    # optional merge to existing vocabulary
    if args.merge_to:
        existing_path = args.merge_to
        if not os.path.exists(existing_path):
            console.print(f"[yellow]--merge-to path not found: {existing_path} (skipping merge)[/yellow]")
            return
        V0 = pl.read_parquet(existing_path) if existing_path.lower().endswith(".parquet") else pl.read_csv(existing_path)
        # normalize cols
        cols = set(V0.columns)
        if "term" not in cols and "canonical" in cols:
            V0 = V0.rename({"canonical":"term"})
        if "canonical" not in V0.columns:
            V0 = V0.with_columns(pl.col("term").alias("canonical"))
        if "term_type" not in V0.columns:
            V0 = V0.with_columns(pl.lit("unknown").alias("term_type"))
        V0 = V0.select([pl.col("term").cast(pl.Utf8).str.strip(),
                        pl.col("canonical").cast(pl.Utf8).str.strip(),
                        pl.col("term_type").cast(pl.Utf8).str.strip().fill_null("unknown")])

        Vnew = (enriched
                .select([pl.col("canonical").alias("term"),
                         pl.col("canonical"),
                         pl.col("term_type")])
                .unique(subset=["canonical","term_type"])
                .rename({"canonical":"canonical"}))

        Vmerged = (pl.concat([V0, Vnew], how="diagonal_relaxed")
                     .drop_nulls(subset=["term"])
                     .unique(subset=["canonical","term_type"], keep="first"))

        # write next to existing
        base = Path(existing_path)
        out_parq = base.with_name(base.stem + "_with_structured.parquet")
        out_csv  = base.with_name(base.stem + "_with_structured.csv")
        Vmerged.write_parquet(str(out_parq))
        Vmerged.write_csv(str(out_csv))
        console.log(f"[green]Merged vocab:[/green] {out_parq}")

    console.rule("[bold green]Structured vocabulary extraction complete")

if __name__ == "__main__":
    main()
