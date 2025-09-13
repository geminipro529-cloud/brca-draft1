#!/usr/bin/env python3
# audit_data.py — multi-threaded audit + linkage + gap report for BRCA / (BRCA_)nanomedicine
# Runs on Windows + Python 3.11. Minimal deps: pandas, rich.
import os, sys, csv, json, argparse, io
from pathlib import Path
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd

try:
    from rich.console import Console
    from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn
    from rich.table import Table
except ImportError:
    print("[ERR] Please install rich: pip install rich", file=sys.stderr); sys.exit(1)

console = Console()

# ---- Heuristics ----
ID_HINTS = ("sample", "sample_id", "case", "case_id", "aliquot", "barcode", "patient", "model_id", "submitter_id")
META_HINTS = ("metadata", "clinical")
EXPR_HINTS = ("rnaseq", "expression", "methyl", "mutation")
NANO_HINTS = ("nano", "nanomed", "nanopart", "exosome", "proteomics")

def human_size(n: int) -> str:
    for unit in ("B","KB","MB","GB","TB"):
        if n < 1024: return f"{n:.1f}{unit}"
        n /= 1024
    return f"{n:.1f}PB"

def safe_stat(p: Path):
    try: s = p.stat(); return s.st_size, int(s.st_mtime)
    except Exception: return 0, 0

def is_table(p: Path) -> bool:
    return p.suffix.lower() in (".csv",".tsv",".parquet")

def id_cols(cols) -> list[str]:
    low = [str(c).lower() for c in cols]
    return [cols[i] for i, c in enumerate(low) if any(h in c for h in ID_HINTS)]

def read_head(p: Path, nrows: int = 5000) -> pd.DataFrame:
    # Read small head to infer IDs without loading entire file (memory safe).
    if p.suffix.lower()==".parquet":
        try: return pd.read_parquet(p, engine="pyarrow").head(nrows)
        except Exception: return pd.read_parquet(p).head(nrows)
    # CSV/TSV with sniff + UTF-8 fallback
    sep = "," if p.suffix.lower()==".csv" else "\t"
    try:
        return pd.read_csv(p, sep=sep, nrows=nrows, dtype=str, low_memory=False, encoding="utf-8")
    except UnicodeDecodeError:
        return pd.read_csv(p, sep=sep, nrows=nrows, dtype=str, low_memory=False, encoding="latin1")

def classify_file(path: Path, root: Path) -> tuple[str, str]:
    # dataset is first component under root; type is meta/expr/other via name hints
    parts = path.relative_to(root).parts
    dataset = parts[0] if parts else "misc"
    name = path.name.lower()
    if any(h in name for h in META_HINTS): return dataset, "meta"
    if any(h in name for h in EXPR_HINTS): return dataset, "expr"
    return dataset, "other"

def process_file(path: Path, root: Path):
    try:
        size, mtime = safe_stat(path)
        dataset, kind = classify_file(path, root)
        return {"dataset":dataset, "kind":kind, "size":size, "mtime":mtime,
                "ext":path.suffix.lower(), "path":str(path)}
    except Exception:
        return None

def scan_root(root: Path, workers: int):
    files = [p for p in root.rglob("*") if p.is_file()]
    stats = defaultdict(lambda: {"count":0, "size":0, "exts":defaultdict(int), "paths_meta":[], "paths_expr":[], "paths_other":[]})
    with Progress(TextColumn("[progress.description]{task.description}"),
                  BarColumn(), TextColumn("{task.completed}/{task.total}"),
                  TimeRemainingColumn(), console=console) as progress:
        task = progress.add_task(f"[cyan]Scanning {root}...", total=len(files))
        with ThreadPoolExecutor(max_workers=workers) as ex:
            for fut in as_completed({ex.submit(process_file, f, root): f for f in files}):
                progress.update(task, advance=1)
                rec = fut.result()
                if not rec: continue
                ds = rec["dataset"]; s = stats[ds]
                s["count"] += 1; s["size"] += rec["size"]; s["exts"][rec["ext"]] += 1
                tgt = "paths_meta" if rec["kind"]=="meta" else ("paths_expr" if rec["kind"]=="expr" else "paths_other")
                s[tgt].append(rec["path"])
    return stats

def best_overlap(meta_df: pd.DataFrame, expr_df: pd.DataFrame) -> tuple[str,int,int,int]:
    mc = id_cols(meta_df.columns); ec = id_cols(expr_df.columns)
    if not mc or not ec: return ("", 0, len(meta_df), len(expr_df))
    best = ("", 0, len(meta_df), len(expr_df))
    for m in mc:
        a = set(meta_df[m].dropna().astype(str))
        for e in ec:
            b = set(expr_df[e].dropna().astype(str))
            ov = len(a & b)
            if ov > best[1]: best = (f"{m}~{e}", ov, len(a), len(b))
    return best

def deep_linkage_for_dataset(ds_stats: dict, read_errors: list) -> list[dict]:
    results = []
    metas = [Path(p) for p in ds_stats["paths_meta"] if is_table(Path(p))]
    exprs = [Path(p) for p in ds_stats["paths_expr"] if is_table(Path(p))]
    if metas and not exprs: results.append({"status":"warn","msg":"Metadata without expression"})
    if exprs and not metas: results.append({"status":"fail","msg":"Expression without metadata"})
    for m in metas:
        for e in exprs:
            try:
                mdf = read_head(m); edf = read_head(e)
                colpair, ov, ma, eb = best_overlap(mdf, edf)
                pct = (ov / max(1, min(ma, eb))) * 100
                level = "ok" if pct >= 50 else ("warn" if ov>0 else "fail")
                results.append({"status":level,"meta":str(m),"expr":str(e),
                                "ids":colpair,"overlap":ov,"meta_n":ma,"expr_n":eb,"pct":round(pct,1)})
            except Exception as ex:
                read_errors.append(f"{m.name} ↔ {e.name}: {ex}")
                results.append({"status":"fail","meta":str(m),"expr":str(e),
                                "ids":"","overlap":0,"meta_n":0,"expr_n":0,"pct":0.0,"error":str(ex)})
    return results

def suggest_additions(all_roots_stats: dict) -> list[str]:
    present_names = " ".join(" ".join(map(str, s.keys())) for s in all_roots_stats.values()).lower()
    hints = []
    if "rnaseq" in present_names and "methyl" not in present_names: hints.append("Add GDC methylation to pair with RNA-seq.")
    if "rnaseq" in present_names and "mutation" not in present_names: hints.append("Add GDC mutation calls (MAF/VCF) for integrative analyses.")
    if any(h in present_names for h in NANO_HINTS) and "proteomics" not in present_names: hints.append("Add proteomics/exosome datasets in nanomedicine cohorts.")
    return hints

def main():
    ap = argparse.ArgumentParser(description="Audit raw/interim/validation data with linkage + gap report.")
    ap.add_argument("--roots", nargs="+", default=["data/01_raw","data/02_interim","data/_validation"])
    ap.add_argument("--out-csv", default="data/_validation/audit_summary.csv")
    ap.add_argument("--out-gap", default="data/_validation/gap_report.json")
    ap.add_argument("--out-link", default="data/_validation/linkage_report.json")
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--deep", action="store_true", help="Check ALL meta↔expr pairs per dataset (slower).")
    args = ap.parse_args()

    Path(args.out_csv).parent.mkdir(parents=True, exist_ok=True)

    all_stats, rows, linkage_map, read_errors = {}, [], {}, []
    # Scan
    for r in args.roots:
        root = Path(r)
        if not root.exists():
            console.print(f"[yellow][WARN][/yellow] Missing: {r}"); continue
        stats = scan_root(root, args.workers)
        all_stats[r] = stats
        for ds, s in stats.items():
            exts = ", ".join(f"{k}:{v}" for k,v in sorted(s["exts"].items()))
            rows.append([r, ds, s["count"], s["size"], exts])

    # Linkage (deep optional)
    for r, stats in all_stats.items():
        for ds, s in stats.items():
            ds_key = f"{r}:{ds}"
            if args.deep:
                linkage_map[ds_key] = deep_linkage_for_dataset(s, read_errors)
            else:
                # shallow: only first meta/expr pair if present
                metas = [Path(p) for p in s["paths_meta"] if is_table(Path(p))]
                exprs = [Path(p) for p in s["paths_expr"] if is_table(Path(p))]
                if metas and exprs:
                    try:
                        mdf, edf = read_head(metas[0]), read_head(exprs[0])
                        colpair, ov, ma, eb = best_overlap(mdf, edf)
                        pct = (ov / max(1, min(ma, eb))) * 100
                        level = "ok" if pct >= 50 else ("warn" if ov>0 else "fail")
                        linkage_map[ds_key] = [{"status":level,"meta":str(metas[0]),"expr":str(exprs[0]),
                                                "ids":colpair,"overlap":ov,"meta_n":ma,"expr_n":eb,"pct":round(pct,1)}]
                    except Exception as ex:
                        read_errors.append(f"{metas[0].name} ↔ {exprs[0].name}: {ex}")
                        linkage_map[ds_key] = [{"status":"fail","meta":str(metas[0]),"expr":str(exprs[0]),
                                                "ids":"","overlap":0,"meta_n":0,"expr_n":0,"pct":0.0,"error":str(ex)}]
                elif metas and not exprs:
                    linkage_map[ds_key] = [{"status":"warn","msg":"Metadata without expression"}]
                elif exprs and not metas:
                    linkage_map[ds_key] = [{"status":"fail","msg":"Expression without metadata"}]

    # Console table
    table = Table(title="Dataset Audit Summary")
    table.add_column("Root"); table.add_column("Dataset")
    table.add_column("Files", justify="right"); table.add_column("Size", justify="right")
    table.add_column("Extensions")
    for r, ds, cnt, size, exts in rows:
        table.add_row(r, ds, str(cnt), human_size(size), exts)
    console.print(table)

    # Linkage summary
    if linkage_map:
        console.print("\n[bold underline]Linkage Checks[/bold underline]")
        for k, items in linkage_map.items():
            for it in items:
                status = it.get("status","")
                msg = it.get("msg", f"{Path(it.get('meta','?')).name} ↔ {Path(it.get('expr','?')).name} [{it.get('ids','')}] overlap={it.get('overlap',0)} ({it.get('pct',0)}%)")
                color = "green" if status=="ok" else ("yellow" if status=="warn" else "red")
                console.print(f"[{color}]{status.upper()}[/] {k}: {msg}")

    # Suggestions
    hints = suggest_additions(all_stats)
    if hints:
        console.print("\n[bold underline]Suggestions[/bold underline]")
        for h in hints: console.print(f"- {h}")

    # Writes
    with open(args.out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f); w.writerow(["root","dataset","file_count","total_bytes","extensions"]); w.writerows(rows)
    with open(args.out_link, "w", encoding="utf-8") as f:
        json.dump(linkage_map, f, ensure_ascii=False, indent=2)
    with open(args.out_gap, "w", encoding="utf-8") as f:
        # Gap report is a minimal, actionable view
        gaps = []
        for k, items in linkage_map.items():
            for it in items:
                if it.get("status") in ("warn","fail"):
                    gaps.append({"dataset":k, **{kk:it.get(kk) for kk in ("msg","meta","expr","ids","pct")}})
        json.dump({"gaps":gaps, "suggestions":hints, "read_errors":read_errors}, f, ensure_ascii=False, indent=2)
    console.print(f"[green]Saved → {args.out_csv} ; {args.out_link} ; {args.out_gap}[/green]")

if __name__ == "__main__":
    main()
