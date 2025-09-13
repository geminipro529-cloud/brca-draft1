#!/usr/bin/env python3
# Stage 2: Merge RNA-seq expression with metadata (robust version)

import argparse
from pathlib import Path
import pandas as pd
from rich.console import Console
from rich.table import Table

ALIASES = ["aliquot_id", "sample_id"]

# ----------------- loaders -----------------
def load_metadata(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise SystemExit(f"[ERROR] Metadata not found: {path}")
    df = pd.read_parquet(path).astype(str)
    print(f"[INFO] Loaded metadata: {path} ({len(df)} rows × {len(df.columns)} cols)")
    return df

def pick_expression_file(source: str, prefer_parquet: bool) -> Path:
    rnaseq_dir = Path("data") / "01_raw" / "RNASEQ" / source
    if not rnaseq_dir.exists():
        raise SystemExit(f"[ERROR] RNASEQ folder not found: {rnaseq_dir}")

    # Prefer parquet
    if prefer_parquet:
        cand = list(rnaseq_dir.glob("*.parquet")) + list(rnaseq_dir.glob("*.pq"))
        if cand:
            print(f"[INFO] Using Parquet file: {cand[0]}")
            return cand[0]

    # Fallback: CSV/TSV
    cand = list(rnaseq_dir.glob("*.csv")) + list(rnaseq_dir.glob("*.tsv"))
    if cand:
        print(f"[INFO] Using CSV/TSV file: {cand[0]}")
        return cand[0]

    raise SystemExit(f"[ERROR] No RNA-seq files found in {rnaseq_dir}")

def load_expression(path: Path, preview_rows=50, transpose=False):
    ext = path.suffix.lower()
    size_mb = path.stat().st_size / (1024*1024)
    print(f"[INFO] Expression file size: {size_mb:.1f} MB")

    if ext in (".parquet", ".pq"):
        full = pd.read_parquet(path).astype(str)
    elif ext in (".csv", ".tsv"):
        sep = "\t" if ext == ".tsv" else ","
        full = pd.read_csv(path, sep=sep, dtype=str, low_memory=False)
    else:
        raise SystemExit(f"[ERROR] Unsupported format: {path}")

    if transpose:
        print("[INFO] Transposing expression table (genes as cols → rows)")
        full = full.set_index(full.columns[0]).T.reset_index().rename(columns={"index": "sample_id"})

    preview = full.head(preview_rows)
    print(f"[INFO] Loaded expression: {path} ({len(full)} rows × {len(full.columns)} cols)")
    return full, preview

# ----------------- merging -----------------
def merge_expression(expr: pd.DataFrame, meta: pd.DataFrame, outdir: Path, source: str):
    expr.columns = [c.lower().strip() for c in expr.columns]
    meta.columns = [c.lower().strip() for c in meta.columns]

    print(f"[DEBUG] Metadata cols: {list(meta.columns)[:15]}")
    print(f"[DEBUG] Expression cols: {list(expr.columns)[:15]}")

    key = next((c for c in ALIASES if c in expr.columns and c in meta.columns), None)
    if not key:
        raise SystemExit("[ERROR] No common key (aliquot_id or sample_id) between metadata and expression")

    print(f"[INFO] Merging on key: {key}")
    merged = pd.merge(meta, expr, on=key, how="inner")

    if "case_id" in merged.columns:
        merged["human_id"] = merged["case_id"].fillna(merged[key])
    else:
        merged["human_id"] = merged[key]

    id_cols = [c for c in ("human_id", "case_id", "sample_id", "aliquot_id") if c in merged.columns]
    expr_cols = [c for c in merged.columns if c not in id_cols and c not in ("__sourcefile",)]
    merged = merged[id_cols + expr_cols]

    outdir.mkdir(parents=True, exist_ok=True)
    out_parquet = outdir / f"{source}_expression_merged.parquet"
    out_csv = outdir / f"{source}_expression_merged.csv"
    merged.to_parquet(out_parquet, index=False)
    merged.to_csv(out_csv, index=False)

    print(f"[INFO] Saved merged table → {out_parquet}")
    print(f"[INFO] Saved merged table → {out_csv}")
    print(f"[INFO] Final size: {len(merged)} rows × {len(merged.columns)} cols")
    return merged

# ----------------- preview -----------------
def print_preview(df: pd.DataFrame, preview_expr: pd.DataFrame, n=5):
    console = Console()
    if df.empty:
        console.print("[WARN] No merged rows to preview.", style="yellow")
        return
    table = Table(title="Expression Preview", show_lines=True)
    for col in df.columns[:6] + list(preview_expr.columns[:4]):
        if col not in table.columns:
            table.add_column(col, style="cyan")
    for _, row in df.head(n).iterrows():
        values = []
        for col in df.columns[:6]:
            values.append(str(row.get(col, ""))[:12])
        for col in preview_expr.columns[:4]:
            values.append(str(row.get(col, ""))[:12])
        table.add_row(*values)
    console.print(table)

# ----------------- main -----------------
def main():
    ap = argparse.ArgumentParser(description="Stage 2: Merge expression with metadata")
    ap.add_argument("--source", required=True, help="Source name, e.g. brca")
    ap.add_argument("--metadata", default="data/02_interim", help="Folder with metadata master")
    ap.add_argument("--out", default="data/03_processed", help="Output folder")
    ap.add_argument("--prefer-parquet", action="store_true", help="Prefer .parquet/.pq expression files")
    ap.add_argument("--transpose", action="store_true", help="Transpose expression (if samples are columns)")
    args = ap.parse_args()

    meta_path = Path(args.metadata) / f"{args.source}_metadata_master.parquet"
    expr_path = pick_expression_file(args.source, args.prefer_parquet)
    outdir = Path(args.out)

    print(f"[DEBUG] Metadata path: {meta_path}")
    print(f"[DEBUG] Expression path: {expr_path}")

    meta = load_metadata(meta_path)
    expr, preview_expr = load_expression(expr_path, preview_rows=50, transpose=args.transpose)
    merged = merge_expression(expr, meta, outdir, args.source)
    print_preview(merged, preview_expr)

if __name__ == "__main__":
    main()
