#!/usr/bin/env python3
# preprocess_rnaseq.py — normalize RNA-seq count tables into Parquet shards
import os, sys, argparse, gzip
import pandas as pd
from pathlib import Path

def ensure_dir(p: Path): p.mkdir(parents=True, exist_ok=True)

def detect_sep(path: Path) -> str:
    return "\t" if path.suffix.lower() in {".tsv", ".txt"} else ","

def stream_read(path: Path, sep: str, chunksize: int):
    """Open TSV/CSV/gz in chunks"""
    opener = gzip.open if path.suffix.endswith(".gz") else open
    with opener(path, "rt", encoding="utf-8", errors="ignore") as fh:
        for chunk in pd.read_csv(fh, sep=sep, chunksize=chunksize, low_memory=False):
            yield chunk

def normalize_chunk(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize: gene_id column first, all else samples"""
    cols = [c.lower() for c in df.columns]
    if "gene_id" in cols:
        df = df.rename(columns={df.columns[cols.index("gene_id")]: "gene_id"})
    elif "id" in cols:
        df = df.rename(columns={df.columns[cols.index("id")]: "gene_id"})
    else:
        df.insert(0, "gene_id", df.index.astype(str))
        df.reset_index(drop=True, inplace=True)
    return df

def process_file(path: Path, out_dir: Path, chunksize: int, shard_rows: int):
    sep = detect_sep(path)
    rows_total, shards = 0, 0
    buf = []

    for chunk in stream_read(path, sep, chunksize):
        chunk = normalize_chunk(chunk)
        buf.append(chunk)
        rows_total += len(chunk)
        if rows_total >= (shards+1)*shard_rows:
            out = pd.concat(buf, ignore_index=True)
            shard_path = out_dir / f"{path.stem}_part{shards:03d}.parquet"
            out.to_parquet(shard_path, engine="pyarrow", index=False, compression="zstd")
            print(f"[OK] {shard_path} ({len(out)} rows)")
            buf.clear()
            shards += 1
    if buf:
        out = pd.concat(buf, ignore_index=True)
        shard_path = out_dir / f"{path.stem}_part{shards:03d}.parquet"
        out.to_parquet(shard_path, engine="pyarrow", index=False, compression="zstd")
        print(f"[OK] {shard_path} ({len(out)} rows)")
    return rows_total

def main():
    ap = argparse.ArgumentParser(description="Preprocess RNA-seq count tables into Parquet shards")
    ap.add_argument("--in", dest="in_dir", required=True, help="Input dir (from YAML raw_dirs)")
    ap.add_argument("--out", dest="out_dir", required=True, help="Output dir for normalized parquet")
    ap.add_argument("--chunksize", type=int, default=50000, help="Rows per chunk when reading")
    ap.add_argument("--shard-rows", type=int, default=200000, help="Rows per output parquet file")
    args = ap.parse_args()

    in_dir, out_dir = Path(args.in_dir), Path(args.out_dir)
    ensure_dir(out_dir)

    total_files, total_rows = 0, 0
    for f in in_dir.rglob("*"):
        if not f.is_file(): continue
        if f.suffix.lower() not in {".csv",".tsv",".txt",".gz"}: continue
        try:
            rows = process_file(f, out_dir, args.chunksize, args.shard_rows)
            total_files += 1; total_rows += rows
        except Exception as e:
            print(f"[WARN] Skipped {f}: {e}")

    print(f"\n[DONE] {total_files} files → {total_rows} rows total into {out_dir}")

if __name__ == "__main__":
    main()
