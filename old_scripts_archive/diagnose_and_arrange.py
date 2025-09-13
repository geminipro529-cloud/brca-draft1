#!/usr/bin/env python3
# Diagnose source folders and arrange files into categories

import os, sys, shutil, argparse
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
from rich.progress import Progress, BarColumn, TimeRemainingColumn, TextColumn, TaskProgressColumn

# ---------- classify file kind ----------
def classify(path: Path) -> str:
    low = path.name.lower()
    if low.endswith((".csv", ".tsv", ".parquet", ".pq", ".xlsx", ".xls")):
        return "structured"
    if low.endswith((".txt", ".log", ".md", ".json", ".jsonl", ".ndjson", ".xml")):
        return "unstructured"
    if "rnaseq" in low or low.endswith((".fastq", ".fastq.gz", ".fq", ".fq.gz")):
        return "rnaseq"
    return "other"

# ---------- arrange ----------
def arrange(source: str, files: list[Path], base: Path, dry: bool, workers: int):
    # output folders
    out_map = {
        "structured": base / "structured",
        "unstructured": base / "unstructured",
        "rnaseq": base / "rnaseq",
        "other": base / "other",
    }
    for p in out_map.values():
        p.mkdir(parents=True, exist_ok=True)

    counts = defaultdict(int)

    with Progress(
        TextColumn("[cyan]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        TimeRemainingColumn(),
        transient=True,
    ) as progress:
        task = progress.add_task("Copying files", total=len(files))

        def _copy(f: Path):
            kind = classify(f)
            dest = out_map[kind] / f.name
            if not dry:
                try:
                    shutil.copy2(f, dest)
                except Exception as e:
                    return kind, f, str(e)
            return kind, f, None

        results = []
        with ThreadPoolExecutor(max_workers=max(1, workers)) as ex:
            futs = {ex.submit(_copy, f): f for f in files}
            for fut in as_completed(futs):
                kind, f, err = fut.result()
                if err:
                    print(f"[ERROR] {f} → {kind}: {err}", file=sys.stderr)
                else:
                    counts[kind] += 1
                progress.update(task, advance=1)

    return out_map, counts

# ---------- main ----------
def main():
    parser = argparse.ArgumentParser(description="Diagnose and arrange source files")
    parser.add_argument("--source", required=True, help="Source name, e.g. brca")
    parser.add_argument("--dry", action="store_true", help="Dry run (no copy)")
    parser.add_argument("--workers", type=int, default=4, help="Number of worker threads")
    args = parser.parse_args()

    src_dir = Path("data") / "00_sources" / args.source
    if not src_dir.exists():
        print(f"[ERROR] source not found: {src_dir}", file=sys.stderr)
        sys.exit(1)

    # collect all files
    files = [p for p in src_dir.rglob("*") if p.is_file()]
    print(f"[INFO] Found {len(files)} files in {src_dir}")

    # simple stats
    by_kind = defaultdict(list)
    for f in files:
        by_kind[classify(f)].append(f)
    for k in ("structured","unstructured","other","rnaseq"):
        total = sum(p.stat().st_size for p in by_kind[k]) / (1024*1024)
        print(f"  {k:12} {len(by_kind[k]):5d} files, {total:8.1f} MB")

    # output base always data\01_arranged\<source>
    base = Path("data") / "01_arranged" / args.source
    base.mkdir(parents=True, exist_ok=True)

    out_map, counts = arrange(args.source, files, base, args.dry, args.workers)

    # summary
    print("\n[SUMMARY]")
    for kind, folder in out_map.items():
        print(f"  {kind:12} {counts.get(kind,0):5d} files -> {folder}")

if __name__ == "__main__":
    main()
