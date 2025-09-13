#!/usr/bin/env python3
"""
Build Structured Union — stand-alone script

Collects CSV/TSV/TXT/Parquet files into one Parquet dataset, fast & robust:
 - Parallel file loading (ProcessPoolExecutor) with --workers
 - Chunked CSV reading (pandas C-engine first; Python fallback)
 - Per-chunk Parquet shard writing in workers (no giant pickled DataFrames)
 - Final single-file Parquet via PyArrow (if available) or keep sharded dataset
 - Windows-safe renames (retries) to avoid [WinError 5]
 - Windows long-path support (\\?\)
 - Ignore junk sidecars (index/hits/report) and user excludes
 - Mini report CSV (file, rows, status, engine, seconds)
"""

import os, sys, time, shutil, argparse, fnmatch, uuid, json
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm
import pandas as pd

# ---------- optional pyarrow ----------
try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    HAVE_PA = True
except Exception:
    HAVE_PA = False

# ---------- windows long path ----------
def win_safe(path: str) -> str:
    p = Path(path).resolve()
    return str(p) if os.name != "nt" else ("\\\\?\\" + str(p))

# ---------- safe rename with retries ----------
def safe_rename(src, dst, retries=5, delay=2):
    for i in range(retries):
        try:
            Path(dst).parent.mkdir(parents=True, exist_ok=True)
            os.replace(src, dst)
            return
        except PermissionError as e:
            if i < retries - 1:
                print(f"⚠️  Rename failed ({e}), retrying in {delay}s... [{i+1}/{retries}]")
                time.sleep(delay)
            else:
                raise
        except Exception:
            if i < retries - 1:
                time.sleep(delay)
            else:
                shutil.move(src, dst)
                return

# ---------- forgiving CSV reader ----------
def read_csv_forgiving(path, sep=None, chunksize=None):
    ext = Path(path).suffix.lower()
    if sep is None:
        sep = "\t" if ext in {".tsv", ".txt"} else ","
    # Try fast C engine first
    try:
        return pd.read_csv(win_safe(path), sep=sep, dtype=str, low_memory=False,
                           engine="c", on_bad_lines="skip",
                           encoding="utf-8", errors="replace",
                           chunksize=chunksize)
    except Exception:
        # Fallback: Python engine (no low_memory)
        return pd.read_csv(win_safe(path), sep=sep, dtype=str,
                           engine="python", on_bad_lines="skip",
                           encoding="utf-8", errors="replace",
                           chunksize=chunksize)

# ---------- filters ----------
DEFAULT_IGNORE_PATTERNS = ["index", "hits", "report"]

def should_ignore_file(fname: str, extra_excludes):
    low = fname.lower()
    if any(p in low for p in DEFAULT_IGNORE_PATTERNS):
        return True
    for pat in extra_excludes or []:
        if fnmatch.fnmatch(fname, pat) or pat.lower() in low:
            return True
    return False

# ---------- worker: process ONE file, write shards, return metadata ----------
def worker_process_file(fp, parts_dir, chunksize=200_000, exclude_cols=None):
    """
    Reads a file and writes one or more parquet shards into parts_dir.
    Returns a dict with: file, rows, status, engine, seconds, shards
    """
    t0 = time.time()
    meta = {
        "file": fp,
        "rows": 0,
        "status": "ok",
        "engine": "",
        "seconds": 0.0,
        "shards": []
    }
    try:
        # Parquet direct
        if fp.lower().endswith((".parquet", ".pq")):
            try:
                df = pd.read_parquet(win_safe(fp))
                if exclude_cols:
                    df = df.drop(columns=[c for c in exclude_cols if c in df.columns], errors="ignore")
                df["__source"] = fp
                if len(df) == 0:
                    meta["status"] = "ok-empty"
                # write single shard
                shard = Path(parts_dir) / f"part-{uuid.uuid4().hex}.parquet"
                df.to_parquet(win_safe(str(shard)), index=False)
                meta["rows"] = len(df)
                meta["engine"] = "parquet"
                meta["shards"].append(str(shard))
            except Exception as e:
                meta["status"] = f"skip: {e}"

        # CSV/TSV/TXT chunked
        else:
            used_engine = "c"
            wrote_any = False
            # first attempt: C engine via read_csv_forgiving
            try:
                iterator = read_csv_forgiving(fp, chunksize=chunksize)
                for chunk in iterator:
                    if exclude_cols:
                        chunk = chunk.drop(columns=[c for c in exclude_cols if c in chunk.columns], errors="ignore")
                    chunk["__source"] = fp
                    if len(chunk) == 0:
                        continue
                    shard = Path(parts_dir) / f"part-{uuid.uuid4().hex}.parquet"
                    chunk.to_parquet(win_safe(str(shard)), index=False)
                    meta["shards"].append(str(shard))
                    meta["rows"] += len(chunk)
                    wrote_any = True
                meta["engine"] = used_engine if wrote_any else "c-empty"
            except Exception:
                # second attempt: force Python engine
                try:
                    used_engine = "python"
                    iterator = pd.read_csv(win_safe(fp), dtype=str,
                                           engine="python", on_bad_lines="skip",
                                           encoding="utf-8", errors="replace",
                                           chunksize=chunksize)
                    for chunk in iterator:
                        if exclude_cols:
                            chunk = chunk.drop(columns=[c for c in exclude_cols if c in chunk.columns], errors="ignore")
                        chunk["__source"] = fp
                        if len(chunk) == 0:
                            continue
                        shard = Path(parts_dir) / f"part-{uuid.uuid4().hex}.parquet"
                        chunk.to_parquet(win_safe(str(shard)), index=False)
                        meta["shards"].append(str(shard))
                        meta["rows"] += len(chunk)
                        wrote_any = True
                    meta["engine"] = used_engine if wrote_any else "python-empty"
                except Exception as e2:
                    meta["status"] = f"skip: {e2}"

    except Exception as e:
        meta["status"] = f"skip: {e}"

    meta["seconds"] = round(time.time() - t0, 3)
    if meta["rows"] == 0 and meta["status"].startswith("ok"):
        meta["status"] = "ok-empty"
    return meta

# ---------- assemble final single-file parquet from shards ----------
def combine_shards_to_single(shards, out_file, compression="snappy"):
    if not HAVE_PA:
        return False, "pyarrow not available"
    # Infer schema from first shard
    first = shards[0]
    table0 = pq.read_table(win_safe(first))
    writer = pq.ParquetWriter(win_safe(out_file), table0.schema, compression=compression)
    try:
        for sh in shards:
            tbl = pq.read_table(win_safe(sh))
            writer.write_table(tbl)
    finally:
        writer.close()
    return True, "ok"

# ---------- write manifest ----------
def write_manifest(parts_dir, out_manifest, total_rows, report_rows):
    tmp = str(out_manifest) + ".tmp"
    data = {
        "created": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "parts_dir": str(parts_dir),
        "total_rows": int(total_rows),
        "files": report_rows
    }
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False, indent=2)
    safe_rename(tmp, out_manifest)

# ---------- main ----------
def main():
    ap = argparse.ArgumentParser(description="Build structured union (robust, parallel, Windows-safe)")
    ap.add_argument("--in-dirs", nargs="+", required=True, help="Input directories to scan")
    ap.add_argument("--out", default="data/02_interim/structured_union/union.parquet",
                    help="Final Parquet file (single file if pyarrow available; otherwise shards only)")
    ap.add_argument("--chunksize", type=int, default=200_000, help="CSV read chunk size")
    ap.add_argument("--workers", type=int, default=3, help="Parallel workers")
    ap.add_argument("--exclude-glob", nargs="*", default=[], help="Extra patterns to exclude (e.g. *.index.csv *PIPELINE_OUT*)")
    ap.add_argument("--exclude-cols", nargs="*", default=[], help="Column names to drop from all inputs")
    ap.add_argument("--keep-shards", action="store_true", help="Keep shards directory even after single-file build")
    args = ap.parse_args()

    out_parq = Path(args.out)
    parts_dir = out_parq.parent / "union_parts"
    parts_dir.mkdir(parents=True, exist_ok=True)
    manifest = out_parq.parent / "union_index.json"

    # Discover files
    candidates = []
    for in_dir in args.in_dirs:
        for root, dirs, files in os.walk(in_dir):
            # optionally skip common output dirs to avoid loops
            skip_dirs = {"PIPELINE_OUT", "ANNOTATED", "union_parts", ".git", "__pycache__"}
            dirs[:] = [d for d in dirs if d not in skip_dirs]
            for f in files:
                fl = f.lower()
                if not (fl.endswith(".csv") or fl.endswith(".tsv") or fl.endswith(".txt")
                        or fl.endswith(".parquet") or fl.endswith(".pq")):
                    continue
                if should_ignore_file(f, args.exclude_glob):
                    continue
                fp = os.path.join(root, f)
                candidates.append(fp)

    if not candidates:
        sys.exit("❌ No candidate files found")

    # Process in parallel → shards
    report_rows, all_shards, total_rows = [], [], 0
    with ProcessPoolExecutor(max_workers=args.workers) as ex:
        futs = {ex.submit(worker_process_file, fp, str(parts_dir), args.chunksize, args.exclude_cols): fp
                for fp in candidates}
        for fut in tqdm(as_completed(futs), total=len(futs),
                        desc=f"Union files (parallel, {args.workers} workers)", unit="file"):
            meta = fut.result()
            report_rows.append({
                "file": meta["file"],
                "rows": meta["rows"],
                "status": meta["status"],
                "engine": meta["engine"],
                "seconds": meta["seconds"]
            })
            if meta["rows"] > 0 and meta["status"].startswith(("ok", "python", "fallback")):
                total_rows += meta["rows"]
                all_shards.extend(meta["shards"])

    # Write mini report
    rep_path = out_parq.with_suffix(".report.csv")
    pd.DataFrame(report_rows).to_csv(rep_path, index=False, encoding="utf-8")
    print(f"📑 Report written: {rep_path}")

    # If no data, stop early
    if not all_shards:
        print("⚠️  No shards produced (all empty/failed).")
        # still write manifest
        write_manifest(parts_dir, manifest, 0, report_rows)
        return

    # Build single-file Parquet if we can
    built_single = False
    if HAVE_PA:
        tmp_single = str(out_parq) + ".tmp"
        ok, msg = combine_shards_to_single(all_shards, tmp_single, compression="snappy")
        if ok:
            safe_rename(tmp_single, str(out_parq))
            built_single = True
            print(f"✅ Union (single file): {out_parq}  rows={total_rows:,}")
        else:
            print(f"⚠️  Could not build single Parquet: {msg}. Keeping shards.")

    else:
        print("⚠️  PyArrow not available → keeping shards only (no single file).")
        print("    Install: pip install pyarrow")

    # Write manifest
    write_manifest(parts_dir, manifest, total_rows, report_rows)
    print(f"🗂  Manifest written: {manifest}")

    # Optionally clean shards if we made a single file
    if built_single and not args.keep_shards:
        try:
            shutil.rmtree(parts_dir)
            print(f"🧹 Removed shards dir: {parts_dir}")
        except Exception as e:
            print(f"⚠️  Could not remove shards dir ({e}). You can delete it later.")

if __name__ == "__main__":
    main()
