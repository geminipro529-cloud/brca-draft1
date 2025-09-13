#!/usr/bin/env python3
"""
Unified biomedical pipeline (Windows-friendly, resumable, parallelized)

Stages:
  1. Clean case lists
  2. Structured union (parallel file loading, mini report)
  2b. Optional unstructured extract
  3. Parallel annotation (vocab hits, parallelized)
  4. QC summary + final console report
"""

import os, re, glob, sys, argparse, time
from pathlib import Path
from datetime import datetime
import pandas as pd
import logging
from logging.handlers import RotatingFileHandler
from tqdm import tqdm
from tabulate import tabulate
from concurrent.futures import ProcessPoolExecutor, as_completed

# ============================================================
# Master Logger
# ============================================================
def get_logger(name="pipeline", log_dir="logs", level="INFO"):
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    log_path = Path(log_dir) / "pipeline.log"
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    lvl = getattr(logging, level.upper(), logging.INFO)
    logger.setLevel(lvl)
    fh = RotatingFileHandler(log_path, maxBytes=5_000_000, backupCount=5, encoding="utf-8")
    fh.setLevel(lvl)
    fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(lvl)
    ch.setFormatter(logging.Formatter("%(levelname)s %(name)s: %(message)s"))
    logger.addHandler(fh); logger.addHandler(ch)
    return logger

# ============================================================
# Helpers
# ============================================================
def ensure_dir(p): Path(p).mkdir(parents=True, exist_ok=True)

def norm_tokens(s: str) -> str:
    if not isinstance(s, str): return ""
    return re.sub(r"[^A-Za-z0-9_]+", " ", s.lower()).strip()

def read_csv_forgiving(path, sep=None, chunksize=None):
    """Robust CSV reader: try fast C engine, fall back to Python engine without low_memory."""
    ext = Path(path).suffix.lower()
    if sep is None:
        sep = "\t" if ext in {".tsv", ".txt"} else ","
    try:
        # Fast path: pandas C engine
        return pd.read_csv(path, sep=sep, dtype=str, low_memory=False,
                           engine="c", on_bad_lines="skip",
                           encoding="utf-8", errors="replace",
                           chunksize=chunksize)
    except Exception:
        # Fallback: pandas Python engine (no low_memory flag here!)
        return pd.read_csv(path, sep=sep, dtype=str,
                           engine="python", on_bad_lines="skip",
                           encoding="utf-8", errors="replace",
                           chunksize=chunksize)

def stage_timer(func):
    """Decorator to measure runtime of each stage."""
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        elapsed = time.time() - start
        args[-1].info(f"{func.__name__} finished in {elapsed:.2f} sec")
        return result
    return wrapper

# ============================================================
# Stage 1: Case lists
# ============================================================
@stage_timer
def stage_clean_caselists(in_glob, out_csv, logger):
    rx = re.compile(r"(MB-\d+|MTS-T\d+)")
    files = sorted(glob.glob(in_glob, recursive=True))
    if not files:
        raise SystemExit(f"CaseList: no files matched {in_glob}")
    ids = []
    for f in tqdm(files, desc="CaseList files", unit="file"):
        try:
            with open(f, "r", encoding="utf-8", errors="replace") as fh:
                for line in fh:
                    ids.extend(rx.findall(line))
        except Exception as e:
            logger.warning(f"CaseList: skip {f}: {e}")
    if not ids:
        raise SystemExit("CaseList: no sample IDs extracted")
    df = pd.DataFrame({"sample_id": sorted(set(ids))})
    ensure_dir(Path(out_csv).parent)
    df.to_csv(out_csv, index=False)
    logger.info(f"CaseList: {len(files)} files → {len(df)} unique IDs → {out_csv}")
    return out_csv

# ============================================================
# Stage 2: Structured Union (parallel)
# ============================================================
def load_one_file(fp, in_dir, chunksize=200_000):
    frames, count, status = [], 0, "ok"
    try:
        if fp.lower().endswith((".parquet",".pq")):
            df = pd.read_parquet(fp)
            df["__source"] = os.path.relpath(fp, in_dir)
            frames.append(df); count = len(df)
        else:
            try:
                for chunk in read_csv_forgiving(fp, chunksize=chunksize):
                    chunk["__source"] = os.path.relpath(fp, in_dir)
                    frames.append(chunk); count += len(chunk)
                status = "ok"
            except Exception:
                # Force Python fallback
                try:
                    for chunk in pd.read_csv(fp, dtype=str,
                                             engine="python", on_bad_lines="skip",
                                             encoding="utf-8", errors="replace",
                                             chunksize=chunksize):
                        chunk["__source"] = os.path.relpath(fp, in_dir)
                        frames.append(chunk); count += len(chunk)
                    status = "fallback-python"
                except Exception as e2:
                    status = f"skip: {e2}"
    except Exception as e:
        status, frames, count = f"skip: {e}", [], 0
    return fp, frames, count, status

@stage_timer
def stage_union_structured(in_dir, out_parquet, logger, chunksize=200_000, workers=3):
    file_list = []
    for root, _, files in os.walk(in_dir):
        for f in files:
            if f.lower().endswith((".csv", ".tsv", ".txt", ".parquet", ".pq")):
                file_list.append(os.path.join(root, f))
    if not file_list:
        raise SystemExit("Union: no candidate files found")

    frames, n_rows, report_rows = [], 0, []
    with ProcessPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(load_one_file, fp, in_dir, chunksize): fp for fp in file_list}
        for fut in tqdm(as_completed(futures), total=len(futures),
                        desc=f"Union files (parallel, {workers} workers)", unit="file"):
            fp, dfs, count, status = fut.result()
            if status.startswith("ok") or status.startswith("fallback"):
                for df in dfs: frames.append(df)
                n_rows += count
            report_rows.append({"file": fp, "rows": count, "status": status})

    union = pd.concat(frames, ignore_index=True)
    ensure_dir(Path(out_parquet).parent)
    union.to_parquet(out_parquet, index=False)
    logger.info(f"Union: {len(file_list)} files → {n_rows:,} rows → {out_parquet}")

    rep_path = str(Path(out_parquet).with_suffix(".report.csv"))
    pd.DataFrame(report_rows).to_csv(rep_path, index=False)
    logger.info(f"Union report written → {rep_path}")
    return out_parquet

# ============================================================
# Stage 2b: Unstructured Extract
# ============================================================
@stage_timer
def stage_extract_unstructured(in_dir, out_csv, logger, max_chars=800_000, batch_sents=300):
    TEXT_EXT = {".txt", ".md", ".log"}
    rx_sent = re.compile(r"(?<=[.!?])\s+|\n{2,}")
    file_list = []
    for root, _, files in os.walk(in_dir):
        for f in files:
            if Path(f).suffix.lower() in TEXT_EXT:
                file_list.append(os.path.join(root, f))
    rows = []
    for fp in tqdm(file_list, desc="Unstructured files", unit="file"):
        try:
            with open(fp, "rb") as fh:
                blob = fh.read(max_chars).decode("utf-8", errors="replace")
            sents = [s.strip() for s in rx_sent.split(blob) if s.strip()]
            for i, s in enumerate(sents[:batch_sents]):
                rows.append((os.path.relpath(fp, in_dir), i, s))
        except Exception as e:
            logger.warning(f"Unstructured: skip {fp}: {e}")
    if not rows:
        logger.warning("Unstructured: no sentences extracted")
        return None
    df = pd.DataFrame(rows, columns=["__source", "sent_idx", "sentence"])
    ensure_dir(Path(out_csv).parent)
    df.to_csv(out_csv, index=False)
    logger.info(f"Unstructured: wrote {len(df)} rows → {out_csv}")
    return out_csv

# ============================================================
# Stage 3: Parallel Annotation
# ============================================================
def annotate_chunk(chunk, vocab, text_cols):
    hits = []
    for row in chunk[text_cols].fillna("").astype(str).values.tolist():
        tokens = set()
        for cell in row: tokens.update(norm_tokens(cell).split())
        hits.append(int(len(tokens & vocab) > 0))
    return hits

@stage_timer
def stage_annotate(union_parquet, vocab_path, out_parquet, logger, workers=3):
    df = pd.read_parquet(union_parquet)
    logger.info(f"Annotate: loaded union rows={len(df):,}")

    try:
        v = pd.read_parquet(vocab_path)[["term","normalized"]] \
            if vocab_path.lower().endswith((".parquet",".pq")) \
            else pd.read_csv(vocab_path, dtype=str, usecols=["term","normalized"])
    except Exception:
        v = pd.read_csv(vocab_path, dtype=str)
        if "term" not in v.columns: v["term"] = v.iloc[:,0]
        if "normalized" not in v.columns: v["normalized"] = v["term"].str.lower().str.strip()
    vocab = set(str(x).lower().strip() for x in v["term"].tolist())

    text_cols = [c for c in df.columns if df[c].dtype == "object"]
    if not text_cols:
        text_cols = df.columns.tolist()
        logger.warning("Annotate: no object dtype columns; scanning all columns")

    chunks, step = [], max(1, len(df) // (workers * 4))
    for start in range(0, len(df), step):
        chunks.append(df.iloc[start:start+step])

    results = []
    with ProcessPoolExecutor(max_workers=workers) as ex:
        futures = [ex.submit(annotate_chunk, chunk, vocab, text_cols) for chunk in chunks]
        for fut in tqdm(as_completed(futures), total=len(futures),
                        desc=f"Annotating rows (parallel, {workers} workers)", unit="chunk"):
            results.extend(fut.result())

    df_out = df.copy()
    df_out["__has_vocab_hit"] = results
    ensure_dir(Path(out_parquet).parent)
    df_out.to_parquet(out_parquet, index=False)
    logger.info(f"Annotate: wrote {out_parquet}")
    return out_parquet

# ============================================================
# Stage 4: QC + Summary
# ============================================================
@stage_timer
def stage_qc(annotated_parquet, caselist_csv, out_csv, logger):
    df = pd.read_parquet(annotated_parquet)
    ci = pd.read_csv(caselist_csv, dtype=str)
    cand = [c for c in df.columns if re.search(r"(sample|patient|id$)", c, re.I)]
    col = cand[0] if cand else None
    qc = {
        "rows_total": len(df),
        "cols_total": len(df.columns),
        "vocab_hits": int(df["__has_vocab_hit"].sum()) if "__has_vocab_hit" in df.columns else 0,
        "sample_col_used": col or "",
        "timestamp": datetime.now().isoformat(timespec="seconds")
    }
    if col:
        found = df[col].astype(str).isin(set(ci["sample_id"]))
        qc["samples_in_caselist"] = int(found.sum())
        qc["samples_not_in_caselist"] = int((~found).sum())
        qc["duplicate_samples"] = int(df[col].duplicated(keep=False).sum())
    else:
        qc.update({"samples_in_caselist": 0, "samples_not_in_caselist": 0, "duplicate_samples": 0})
    pd.DataFrame([qc]).to_csv(out_csv, index=False)
    logger.info(f"QC: {qc}")
    return out_csv

def print_summary(qc_csv, logger):
    try:
        qc = pd.read_csv(qc_csv).iloc[0].to_dict()
    except Exception as e:
        logger.error(f"Summary: failed to load QC file: {e}")
        return
    table = [
        ["Total rows", f"{qc.get('rows_total',0):,}"],
        ["Total columns", f"{qc.get('cols_total',0):,}"],
        ["Vocab hits", f"{qc.get('vocab_hits',0):,}"],
        ["Sample col used", qc.get("sample_col_used","")],
        ["Samples in case list", f"{qc.get('samples_in_caselist',0):,}"],
        ["Samples not in case list", f"{qc.get('samples_not_in_caselist',0):,}"],
        ["Duplicate samples", f"{qc.get('duplicate_samples',0):,}"],
        ["Timestamp", qc.get("timestamp","")]
    ]
    logger.info("\n" + tabulate(table, headers=["Metric","Value"], tablefmt="pretty"))

# ============================================================
# Main
# ============================================================
def main():
    ap = argparse.ArgumentParser(description="Unified pipeline (parallel, Windows-friendly)")
    ap.add_argument("--caselist-glob", default=r"data\01_raw\STRUCTURED\**\case_lists\cases_*.txt")
    ap.add_argument("--structured-dir", default=r"data\01_raw\STRUCTURED")
    ap.add_argument("--unstructured-dir", default=r"data\01_raw\UNSTRUCTURED")
    ap.add_argument("--vocab", default=r"data\02_interim\VOCAB_COMPILED\vocabulary_enriched.parquet")
    ap.add_argument("--out", default=r"data\02_interim\PIPELINE_OUT")
    ap.add_argument("--with-unstructured", action="store_true")
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--workers", type=int, default=3, help="parallel workers (default=3)")
    args = ap.parse_args()

    logger = get_logger("pipeline")

    outdir = Path(args.out); ensure_dir(outdir)
    case_csv = outdir / "case_list_clean.csv"
    union_parq = outdir / "structured_union.parquet"
    annot_parq = outdir / "structured_annotated.parquet"
    qc_csv = outdir / "qc_summary.csv"
    unstruct_csv = outdir / "unstructured_hits.csv"

    # Stage 1
    if not args.force and case_csv.exists():
        logger.info(f"CaseList: reuse {case_csv}")
    else:
        stage_clean_caselists(args.caselist_glob, str(case_csv), logger)

    # Stage 2
    if not args.force and union_parq.exists():
        logger.info(f"Union: reuse {union_parq}")
    else:
        stage_union_structured(args.structured_dir, str(union_parq), logger,
                               chunksize=200_000, workers=args.workers)

    # Stage 2b
    if args.with_unstructured:
        if not args.force and unstruct_csv.exists():
            logger.info(f"Unstructured: reuse {unstruct_csv}")
        else:
            stage_extract_unstructured(args.unstructured_dir, str(unstruct_csv), logger)

    # Stage 3
    if not args.force and annot_parq.exists():
        logger.info(f"Annotate: reuse {annot_parq}")
    else:
        stage_annotate(str(union_parq), args.vocab, str(annot_parq), logger, workers=args.workers)

    # Stage 4
    stage_qc(str(annot_parq), str(case_csv), str(qc_csv), logger)
    print_summary(str(qc_csv), logger)

    logger.info("Pipeline finished OK")

if __name__ == "__main__":
    main()
