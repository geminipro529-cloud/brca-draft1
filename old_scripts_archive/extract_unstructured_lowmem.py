#!/usr/bin/env python3
"""
Unstructured extractor (low-mem, Windows-friendly, parallel, resumable, streaming)

- Scans an input folder for .txt/.md/.log
- Streams each file in byte chunks, splits to sentences, matches vocab tokens
- Flushes rows to shard files periodically (Parquet if pyarrow else CSV)
- Heartbeat logs so you see steady progress
- Soft per-file timeout ensures no single file blocks forever
- Manifest enables resume and incremental results
"""

import os, sys, re, json, time, shutil, argparse, uuid, fnmatch
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor
from typing import List, Dict, Tuple, Optional
import pandas as pd
from tqdm import tqdm

# Optional pyarrow for Parquet output
try:
    import pyarrow as pa  # noqa: F401
    import pyarrow.parquet as pq
    HAVE_PA = True
except Exception:
    HAVE_PA = False

# ---------------- Windows helpers ----------------
def win_safe(path: str) -> str:
    p = Path(path).resolve()
    return str(p) if os.name != "nt" else ("\\\\?\\" + str(p))

def safe_rename(src: str, dst: str, retries: int = 5, delay: int = 2):
    for i in range(retries):
        try:
            Path(dst).parent.mkdir(parents=True, exist_ok=True)
            os.replace(src, dst)
            return
        except PermissionError as e:
            if i < retries - 1:
                print(f"⚠️  Rename failed ({e}); retry {i+1}/{retries} in {delay}s")
                time.sleep(delay)
            else:
                raise
        except Exception:
            if i < retries - 1:
                time.sleep(delay)
            else:
                shutil.move(src, dst)
                return

# ---------------- Vocab loading ----------------
def load_vocab(vpath: str,
               term_col: str = "term",
               norm_col: str = "normalized",
               max_terms_per_group: Optional[int] = 400) -> pd.DataFrame:
    ext = Path(vpath).suffix.lower()
    if ext in (".parquet", ".pq"):
        df = pd.read_parquet(win_safe(vpath))
    elif ext in (".csv", ".tsv", ".txt"):
        sep = "\t" if ext in (".tsv", ".txt") else ","
        df = pd.read_csv(win_safe(vpath), sep=sep, dtype=str, encoding="utf-8", errors="replace")
    elif ext == ".json":
        df = pd.read_json(win_safe(vpath))
    else:
        raise ValueError(f"Unsupported vocab extension: {ext}")

    if term_col not in df.columns:
        term_col = df.columns[0]
    if norm_col not in df.columns:
        df[norm_col] = df[term_col].astype(str).str.lower().str.strip()

    df[term_col] = df[term_col].astype(str).str.strip()
    df[norm_col] = df[norm_col].astype(str).str.lower().str.strip()
    df = df.dropna(subset=[term_col])
    df = df[df[term_col] != ""]

    if max_terms_per_group:
        df = (df.sort_values([norm_col, term_col])
                .groupby(norm_col, as_index=False)
                .head(int(max_terms_per_group)))

    return df[[term_col, norm_col]].rename(columns={term_col: "term", norm_col: "normalized"})

def normalize_text(s: str) -> List[str]:
    if not isinstance(s, str):
        return []
    s = s.lower()
    s = re.sub(r"[^a-z0-9_]+", " ", s)
    return [t for t in s.split() if t]

# ---------------- Shard writing ----------------
def write_shard(df: pd.DataFrame, shard_path_base: str) -> str:
    if HAVE_PA:
        path = f"{shard_path_base}.parquet"
        df.to_parquet(win_safe(path), index=False)
        return path
    else:
        path = f"{shard_path_base}.csv"
        df.to_csv(win_safe(path), index=False, encoding="utf-8")
        return path

# ---------------- Worker (streaming with periodic flush) ----------------
def process_one_file(fp: str,
                     in_root: str,
                     shards_dir: str,
                     vocab_terms: set,
                     batch_sents: int,
                     max_chars: int,
                     file_timeout_sec: int,
                     stream_chunk_bytes: int,
                     flush_every_sents: int) -> Dict:
    t0 = time.time()
    meta = {"file": fp, "rows": 0, "status": "ok", "seconds": 0.0, "shards": []}
    rx_sent = re.compile(r"(?<=[.!?])\s+|\n{2,}")
    rel = os.path.relpath(fp, in_root)

    try:
        buf = ""           # carry partial sentence across chunks
        out_rows = []      # current rows buffer to flush as shard
        total_sents = 0    # count sentences produced
        read_total = 0     # bytes read

        with open(win_safe(fp), "rb") as fh:
            while True:
                if max_chars and read_total >= max_chars:
                    break
                to_read = stream_chunk_bytes if not max_chars else min(stream_chunk_bytes, max_chars - read_total)
                chunk = fh.read(to_read)
                if not chunk:
                    break
                read_total += len(chunk)
                buf += chunk.decode("utf-8", errors="replace")

                parts = rx_sent.split(buf)
                if len(parts) > 1:
                    complete = parts[:-1]
                    buf = parts[-1]
                    for s in complete:
                        s = s.strip()
                        if not s:
                            continue
                        toks = set(normalize_text(s))
                        hits = toks & vocab_terms
                        out_rows.append((rel, total_sents, s,
                                         int(len(hits) > 0),
                                         min(len(hits), 999),
                                         ", ".join(list(hits)[:20])))
                        total_sents += 1

                        # periodic flush
                        if len(out_rows) >= flush_every_sents:
                            df = pd.DataFrame(out_rows, columns=["__source","sent_idx","sentence","has_hit","hit_count","hit_terms"])
                            shard_base = str(Path(shards_dir) / f"shard-{uuid.uuid4().hex}")
                            actual = write_shard(df, shard_base)
                            meta["shards"].append(actual)
                            meta["rows"] += len(df)
                            out_rows.clear()

                        if batch_sents and total_sents >= batch_sents:
                            break
                        if (time.time() - t0) > file_timeout_sec:
                            meta["status"] = "timeout-partial"
                            break

                if batch_sents and total_sents >= batch_sents:
                    break
                if (time.time() - t0) > file_timeout_sec:
                    meta["status"] = "timeout-partial"
                    break

        # flush remaining buffered text
        if meta["status"] != "timeout-partial" and buf.strip() and (not batch_sents or total_sents < batch_sents):
            rest = [s.strip() for s in rx_sent.split(buf) if s.strip()]
            for s in rest:
                if batch_sents and total_sents >= batch_sents:
                    break
                toks = set(normalize_text(s))
                hits = toks & vocab_terms
                out_rows.append((rel, total_sents, s,
                                 int(len(hits) > 0),
                                 min(len(hits), 999),
                                 ", ".join(list(hits)[:20])))
                total_sents += 1
                if len(out_rows) >= flush_every_sents:
                    df = pd.DataFrame(out_rows, columns=["__source","sent_idx","sentence","has_hit","hit_count","hit_terms"])
                    shard_base = str(Path(shards_dir) / f"shard-{uuid.uuid4().hex}")
                    actual = write_shard(df, shard_base)
                    meta["shards"].append(actual)
                    meta["rows"] += len(df)
                    out_rows.clear()

        # final flush
        if out_rows:
            df = pd.DataFrame(out_rows, columns=["__source","sent_idx","sentence","has_hit","hit_count","hit_terms"])
            shard_base = str(Path(shards_dir) / f"shard-{uuid.uuid4().hex}")
            actual = write_shard(df, shard_base)
            meta["shards"].append(actual)
            meta["rows"] += len(df)

        if meta["rows"] == 0 and meta["status"] == "ok":
            meta["status"] = "ok-empty"

    except Exception as e:
        meta["status"] = f"skip: {e}"

    meta["seconds"] = round(time.time() - t0, 3)
    return meta

# ---------------- Combine shards (optional) ----------------
def try_build_combined_parquet(shards: List[str], out_file: str) -> Tuple[bool, str]:
    if not HAVE_PA:
        return False, "pyarrow not installed"
    try:
        table0 = pq.read_table(win_safe(shards[0]))
        writer = pq.ParquetWriter(win_safe(out_file), table0.schema, compression="snappy")
        try:
            for sh in shards:
                tbl = pq.read_table(win_safe(sh))
                writer.write_table(tbl)
        finally:
            writer.close()
        return True, "ok"
    except Exception as e:
        return False, f"{e}"

# ---------------- Manifest helpers ----------------
def write_manifest(out_dir: str, name: str, data: Dict):
    tmp = str(Path(out_dir) / (name + ".tmp"))
    final = str(Path(out_dir) / name)
    # ensure JSON-serializable
    data = json.loads(json.dumps(data))
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False, indent=2)
    safe_rename(tmp, final)

def read_manifest(out_dir: str, name: str) -> Dict:
    path = Path(out_dir) / name
    if path.exists():
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    return {"created": None, "processed": {}, "shards": []}

# ---------------- Discovery / filters ----------------
TEXT_EXT = {".txt", ".md", ".log"}

def should_skip(fname: str, extra_excludes: List[str]) -> bool:
    low = fname.lower()
    if any(k in low for k in ["manifest", "report", "index", "shard"]):
        return True
    for pat in extra_excludes or []:
        if fnmatch.fnmatch(fname, pat) or pat.lower() in low:
            return True
    return False

# ---------------- Main ----------------
def main():
    ap = argparse.ArgumentParser(description="Extract unstructured sentences + vocab hits (parallel, resumable, streaming)")
    ap.add_argument("--in", dest="in_dir", required=True, help="Input folder with .txt/.md/.log")
    ap.add_argument("--vocab", required=True, help="Vocabulary file (parquet/csv/tsv/json)")
    ap.add_argument("--out", dest="out_dir", required=True, help="Output folder")
    ap.add_argument("--workers", type=int, default=3, help="Parallel workers (default=3)")
    ap.add_argument("--resume", action="store_true", help="Skip files already in manifest")
    ap.add_argument("--max-chars", type=int, default=800_000, help="Max bytes to read per file")
    ap.add_argument("--batch-sents", type=int, default=300, help="Max sentences to keep per file")
    ap.add_argument("--status-interval-sec", type=int, default=30, help="Heartbeat interval")
    ap.add_argument("--file-timeout-sec", type=int, default=600, help="Soft per-file time limit")
    ap.add_argument("--priority", choices=["none", "small-first", "large-first"], default="small-first",
                    help="File processing order")
    ap.add_argument("--exclude-glob", nargs="*", default=[], help="Extra filename patterns to exclude")
    ap.add_argument("--keep-shards", action="store_true", help="Keep shards after combining")
    ap.add_argument("--stream-chunk-bytes", type=int, default=2_000_000, help="Bytes per read while streaming")
    ap.add_argument("--flush-every-sents", type=int, default=5_000, help="Flush shard every N sentences")
    args = ap.parse_args()

    in_dir = args.in_dir
    out_dir = args.out_dir
    shards_dir = str(Path(out_dir) / "shards")
    Path(shards_dir).mkdir(parents=True, exist_ok=True)

    # Load vocab
    print(f"[INFO] Loading vocab: {Path(args.vocab).resolve()}")
    vdf = load_vocab(args.vocab)
    vocab_terms = set(vdf["normalized"].astype(str).tolist())

    # Discover files
    candidates: List[str] = []
    for root, dirs, files in os.walk(in_dir):
        skip_dirs = {"PIPELINE_OUT", "ANNOTATED", "shards", ".git", "__pycache__"}
        dirs[:] = [d for d in dirs if d not in skip_dirs]
        for f in files:
            if should_skip(f, args.exclude_glob):
                continue
            if Path(f).suffix.lower() in TEXT_EXT:
                candidates.append(os.path.join(root, f))

    if not candidates:
        print("❌ No candidate text files found.")
        sys.exit(1)

    # Priority ordering by size
    if args.priority != "none":
        candidates.sort(key=lambda p: os.path.getsize(p),
                        reverse=(args.priority == "large-first"))

    # Manifest / resume
    manifest_name = "unstructured_manifest.json"
    manifest = read_manifest(out_dir, manifest_name)
    already: Dict[str, Dict] = manifest.get("processed", {})

    todo = [fp for fp in candidates if (not args.resume or os.path.relpath(fp, in_dir) not in already)]
    print(f"[INFO] Files total={len(candidates)}, to_process={len(todo)}, resume={'ON' if args.resume else 'OFF'}")

    # Parallel with heartbeat
    report_rows, all_shards, total_rows = [], [], 0
    with ProcessPoolExecutor(max_workers=args.workers) as ex:
        futs = {
            ex.submit(
                process_one_file, fp, in_dir, shards_dir, vocab_terms,
                args.batch_sents, args.max_chars, args.file_timeout_sec,
                args.stream_chunk_bytes, args.flush_every_sents
            ): fp
            for fp in todo
        }
        pbar = tqdm(total=len(futs), desc=f"Unstructured (workers={args.workers})", unit="file")
        last_hb = time.time()
        while futs:
            done_now = [f for f in list(futs) if f.done()]
            if done_now:
                for fut in done_now:
                    fp = futs.pop(fut)
                    pbar.update(1)
                    try:
                        meta = fut.result()
                    except Exception as e:
                        meta = {"file": fp, "rows": 0, "status": f"skip:{e}", "seconds": 0.0, "shards": []}
                    rel = os.path.relpath(meta["file"], in_dir)
                    report_rows.append({"file": rel, "rows": meta["rows"], "status": meta["status"], "seconds": meta["seconds"]})
                    if meta["rows"] > 0:
                        total_rows += meta["rows"]
                        all_shards.extend(meta["shards"])
                    # update manifest incrementally
                    manifest.setdefault("processed", {})[rel] = meta
                    uniq = list(dict.fromkeys(manifest.get("shards", []) + meta["shards"]))
                    manifest["shards"] = uniq
                    write_manifest(out_dir, manifest_name, manifest)
            else:
                now = time.time()
                if now - last_hb >= args.status_interval_sec:
                    print(f"[HB] done={pbar.n}/{pbar.total}, in_flight={len(futs)}")
                    last_hb = now
                time.sleep(0.25)
        pbar.close()

    # Per-file report
    rep_path = str(Path(out_dir) / "unstructured.report.csv")
    pd.DataFrame(report_rows).to_csv(win_safe(rep_path), index=False, encoding="utf-8")
    print(f"[INFO] Report written: {rep_path}")

    # Combined parquet (optional)
    combined_path = str(Path(out_dir) / "unstructured_extracted.parquet")
    if all_shards and HAVE_PA:
        tmp = combined_path + ".tmp"
        ok, msg = try_build_combined_parquet(all_shards, tmp)
        if ok:
            safe_rename(tmp, combined_path)
            print(f"✅ Combined Parquet: {combined_path}  rows≈{total_rows:,}")
            if not args.keep_shards:
                try:
                    shutil.rmtree(shards_dir)
                    print(f"[CLEAN] Removed shards dir: {shards_dir}")
                except Exception as e:
                    print(f"[WARN] Could not remove shards dir: {e}")
        else:
            print(f"[WARN] Could not build combined Parquet: {msg}")
    else:
        if not HAVE_PA:
            print("[WARN] pyarrow not installed → keeping shards only (no combined parquet).")
        elif not all_shards:
            print("[WARN] No data rows produced → skipping combine.")

    # Final manifest update
    manifest["created"] = manifest.get("created") or time.strftime("%Y-%m-%dT%H:%M:%S")
    manifest["total_rows"] = total_rows
    write_manifest(out_dir, manifest_name, manifest)
    print("[DONE] Unstructured extraction complete.")

if __name__ == "__main__":
    main()
