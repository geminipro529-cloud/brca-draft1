#!/usr/bin/env python3
# scripts/annotate_structured_with_vocab.py
# Brevity + safety: concurrent, resumable, long-path-safe annotations for structured tables.
# Inputs:  --in <dir>  --vocab <parquet|csv>  --out <dir>
# Outputs: <out>/hits_parts/*.hits.csv, <out>/summary.csv, <out>/annotated.parquet (+ .csv)
import os, sys, re, csv, json, argparse, hashlib, traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Tuple
import pandas as pd

CSV_LIKE = (".csv", ".tsv", ".txt")
PARQ     = (".parquet", ".pq")

# ---------------- tiny utils ----------------
def ensure_dir(p: str): 
    if p: os.makedirs(p, exist_ok=True)

def md5_12(s: str) -> str:
    return hashlib.md5(s.encode("utf-8", "ignore")).hexdigest()[:12]

def short_name_for(path: str, suffix: str) -> str:
    # Avoid Windows MAX_PATH by hashing basename only; keep extension/suffix short.
    base = os.path.basename(path)
    return f"{md5_12(base)}{suffix}"

def atomic_write_csv(df: pd.DataFrame, out_path: str, mode: str = "w", header: bool = True):
    # For 'w' writes atomically, for 'a' we best-effort append (atomic append is impractical).
    if mode == "w":
        tmp = out_path + ".tmp"
        df.to_csv(tmp, index=False, encoding="utf-8", errors="replace")
        os.replace(tmp, out_path)
    else:
        with open(out_path, mode, encoding="utf-8", errors="replace", newline="") as f:
            df.to_csv(f, index=False, header=header)

def atomic_write_parquet(df: pd.DataFrame, out_path: str):
    tmp = out_path + ".tmp"
    df.to_parquet(tmp, index=False)
    os.replace(tmp, out_path)

def list_data_files(indir: str) -> List[str]:
    out = []
    for root, _, files in os.walk(indir):
        for fn in files:
            lo = fn.lower()
            if lo.endswith(PARQ) or lo.endswith(CSV_LIKE):
                out.append(os.path.join(root, fn))
    return out

def load_vocab_terms(vocab_path: str) -> List[str]:
    # Load but DO NOT build a giant regex from 400k terms (too slow). We primarily breast-filter fast.
    try:
        if vocab_path.lower().endswith((".parquet",".pq")):
            v = pd.read_parquet(vocab_path)
        else:
            v = pd.read_csv(vocab_path)
        # Use a tiny subset for column-name tagging only (fast), fall back to first column if unknown schema.
        col = "normalized_term" if "normalized_term" in v.columns else v.columns[0]
        terms = v[col].astype(str).str.lower().dropna().unique().tolist()
        return terms[:1000]  # small cap keeps it fast; we still use a robust breast regex for cell content
    except Exception:
        return []

# ---------------- matching logic ----------------
# Fast, pragmatic filter for breast-only rows (robust and cheap)
BREAST_REGEX = re.compile(r"\b(breast|mammary|icd-?10:? ?c50)\b", flags=re.IGNORECASE)

def chunk_iter_csv(path: str, chunksize: int):
    # Robust CSV/TSV reader with fallbacks that won't crash the pipeline
    sep = "\t" if path.lower().endswith((".tsv",".txt")) else ","
    try:
        for chunk in pd.read_csv(path, sep=sep, chunksize=chunksize, low_memory=False, encoding="utf-8", on_bad_lines="skip"):
            yield chunk
    except UnicodeDecodeError:
        for chunk in pd.read_csv(path, sep=sep, chunksize=chunksize, low_memory=False, encoding="latin1", on_bad_lines="skip"):
            yield chunk
    except Exception as e:
        print(f"⚠️ Skipping unreadable CSV/TSV: {path} ({e})")

def read_parquet_safe(path: str) -> pd.DataFrame:
    try:
        return pd.read_parquet(path)
    except Exception as e:
        print(f"⚠️ Skipping unreadable parquet: {path} ({e})")
        return pd.DataFrame()

def df_breast_filter(df: pd.DataFrame) -> pd.Series:
    # True for rows where ANY column contains a breast marker string (case-insensitive).
    # Convert only object-like columns to str for speed.
    if df.empty: 
        return pd.Series([], dtype=bool)
    mask = pd.Series(False, index=df.index)
    for col in df.columns:
        s = df[col]
        if pd.api.types.is_object_dtype(s) or pd.api.types.is_string_dtype(s):
            try:
                mask |= s.astype(str, copy=False).str.contains(BREAST_REGEX, regex=True, na=False)
            except Exception:
                pass
    return mask

def vocab_hits_on_columns(df: pd.DataFrame, small_vocab: List[str]) -> List[str]:
    if not small_vocab: return []
    cols_l = [str(c).lower() for c in df.columns]
    hits = sorted({t for t in small_vocab if any(t in c for c in cols_l)})
    return hits[:100]  # keep the per-file hit list tiny

# ---------------- per-file worker ----------------
def process_one_file(path: str, out_dir: str, chunksize: int, small_vocab: List[str]) -> Tuple[str, int, int, int, str]:
    """
    Returns: (file_id, total_rows, matched_rows, ncols, note)
    Writes:
      - hits_parts/<hash>.hits.csv  (summary for this file)
      - parts/<hash>.part.csv       (matched rows; optional if none)
    """
    file_id = md5_12(path)
    hits_dir = os.path.join(out_dir, "hits_parts"); ensure_dir(hits_dir)
    parts_dir = os.path.join(out_dir, "parts"); ensure_dir(parts_dir)

    hits_csv  = os.path.join(hits_dir,  f"{file_id}.hits.csv")
    part_csv  = os.path.join(parts_dir, f"{file_id}.part.csv")

    # Resume: if hits exists, skip
    if os.path.exists(hits_csv):
        try:
            row = pd.read_csv(hits_csv, nrows=1)
            return file_id, int(row["total_rows"][0]), int(row["matched_rows"][0]), int(row["ncols"][0]), "resume-skip"
        except Exception:
            pass  # fall through to recompute if summary unreadable

    total_rows = matched_rows = ncols = 0
    wrote_header = False
    note = ""

    # Read + filter
    if path.lower().endswith(PARQ):
        df = read_parquet_safe(path)
        if df.empty:
            note = "empty-or-unreadable"
        else:
            ncols = df.shape[1]
            mask = df_breast_filter(df)
            total_rows = len(df)
            matched_rows = int(mask.sum())
            if matched_rows:
                atomic_write_csv(df[mask], part_csv, mode="w", header=True)
    else:
        # CSV/TSV chunked
        for chunk in chunk_iter_csv(path, chunksize):
            if chunk is None or chunk.empty: 
                continue
            ncols = max(ncols, chunk.shape[1])
            m = df_breast_filter(chunk)
            total_rows += len(chunk)
            if m.any():
                atomic_write_csv(chunk[m], part_csv, mode=("a" if wrote_header else "w"), header=not wrote_header)
                wrote_header = True

        matched_rows = 0
        if os.path.exists(part_csv):
            try:
                # Efficient row count without full load
                matched_rows = sum(1 for _ in open(part_csv, "r", encoding="utf-8", errors="replace")) - 1
                if matched_rows < 0: matched_rows = 0
            except Exception:
                matched_rows = 0

    # Write per-file hits summary
    col_hits = []
    try:
        # For column-name tagging, peek at a tiny sample to avoid full reload
        if path.lower().endswith(PARQ):
            df0 = read_parquet_safe(path).head(1)
        else:
            # First non-empty chunk sample
            df0 = None
            for chunk in chunk_iter_csv(path, chunksize=1000):
                if chunk is not None and not chunk.empty: df0 = chunk.head(1); break
        if df0 is not None:
            col_hits = vocab_hits_on_columns(df0, small_vocab)
    except Exception:
        pass

    hit_row = pd.DataFrame([{
        "file_id": file_id,
        "source_path": path,
        "total_rows": total_rows,
        "matched_rows": matched_rows,
        "ncols": ncols,
        "column_term_hits": ";".join(col_hits)
    }])
    atomic_write_csv(hit_row, hits_csv, mode="w", header=True)
    return file_id, total_rows, matched_rows, ncols, note

# ---------------- finalize ----------------
def finalize_outputs(out_dir: str):
    # Build summary.csv from all hits
    hits_dir = os.path.join(out_dir, "hits_parts")
    summary_csv = os.path.join(out_dir, "summary.csv")
    rows = []
    for fn in os.listdir(hits_dir):
        if fn.lower().endswith(".csv"):
            try:
                r = pd.read_csv(os.path.join(hits_dir, fn))
                rows.append(r)
            except Exception:
                pass
    if rows:
        summary = pd.concat(rows, ignore_index=True)
        atomic_write_csv(summary, summary_csv, mode="w", header=True)

    # Concatenate all parts/*.part.csv → annotated.csv (+ annotated.parquet)
    parts_dir = os.path.join(out_dir, "parts")
    annotated_csv = os.path.join(out_dir, "annotated.csv")
    annotated_parq = os.path.join(out_dir, "annotated.parquet")

    wrote = False
    if os.path.isdir(parts_dir):
        for fn in os.listdir(parts_dir):
            if not fn.lower().endswith(".csv"): 
                continue
            p = os.path.join(parts_dir, fn)
            try:
                df = pd.read_csv(p)
                if df.empty: 
                    continue
                atomic_write_csv(df, annotated_csv, mode=("a" if wrote else "w"), header=(not wrote))
                wrote = True
            except Exception:
                pass
    # Parquet mirror (only if we wrote CSV)
    if wrote:
        try:
            df_all = pd.read_csv(annotated_csv)
            if not df_all.empty:
                atomic_write_parquet(df_all, annotated_parq)
        except Exception as e:
            print(f"⚠️ Could not create annotated.parquet: {e}")

# ---------------- main ----------------
def main():
    ap = argparse.ArgumentParser(description="Concurrent, resumable structured annotation (breast-only fast filter)")
    ap.add_argument("--in",  dest="inp",  required=True, help="Input folder of structured tables")
    ap.add_argument("--vocab", required=True, help="Vocabulary file (.parquet/.csv) [used lightly for column tagging]")
    ap.add_argument("--out", required=True, help="Output folder (annotated_v2)")
    ap.add_argument("--workers", type=int, default=3, help="Concurrent helpers (2–3 recommended)")
    ap.add_argument("--chunksize", type=int, default=50000, help="CSV/TSV processing chunk size")
    ap.add_argument("--resume", action="store_true", help="Skip files that already have hits_parts/<hash>.hits.csv")
    args = ap.parse_args()

    ensure_dir(args.out)
    ensure_dir(os.path.join(args.out, "hits_parts"))
    ensure_dir(os.path.join(args.out, "parts"))

    files = list_data_files(args.inp)
    if not files:
        print(f"❌ No input files found under {args.inp}")
        sys.exit(1)

    small_vocab = load_vocab_terms(args.vocab)

    # Build list of targets respecting resume flag
    todo = []
    for p in files:
        hit = os.path.join(args.out, "hits_parts", f"{md5_12(p)}.hits.csv")
        if args.resume and os.path.exists(hit):
            continue
        todo.append(p)

    # Process concurrently
    done = len(files) - len(todo)
    total = len(files)
    print(f"Vocab (sampled for cols): {len(small_vocab)} terms")
    print(f"Scanning: total_files={total} todo={len(todo)} workers={max(1,args.workers)} chunksize={args.chunksize}")

    futs = []
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as ex:
        for p in todo:
            futs.append(ex.submit(process_one_file, p, args.out, args.chunksize, small_vocab))
        for i, fut in enumerate(as_completed(futs), start=1):
            try:
                fid, tot, hit, ncols, note = fut.result()
                done += 1
                if i % 10 == 0 or i == len(futs):
                    print(f"... {done}/{total} files")
            except Exception as e:
                print(f"⚠️ Worker failed: {e}")

    # Finalize consolidated outputs
    finalize_outputs(args.out)
    print("✅ Annotation finished")

if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception:
        traceback.print_exc()
        sys.exit(1)
