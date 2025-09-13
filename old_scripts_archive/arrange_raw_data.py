#!/usr/bin/env python3
"""
arrange_raw_data.py – Reorganize messy raw folders into a minimal hierarchy.
Safe by default: copies files first, logs everything, preserves originals.

Usage examples:
  python scripts/arrange_raw_data.py --in data/01_raw --out data/01_arranged --mode copy
  python scripts/arrange_raw_data.py --in data/01_raw --out data/01_arranged --mode move
"""

import os, re, csv, shutil, argparse, datetime
from pathlib import Path

# --- Classification rules ---
KEYWORD_MAP = {
    "BRCA": "BRCA",
    "breast": "BRCA",
    "nanomed": "NANOMEDICINE",
    "nano": "NANOMEDICINE",
    "proteome": "OMICS/proteomics",
    "protein": "OMICS/proteomics",
    "methyl": "OMICS/methylation",
    "epic": "OMICS/methylation",
    "rnaseq": "OMICS/rnaseq",
    "rna_seq": "OMICS/rnaseq",
    "exosome": "OMICS/exosomes",
    "vesicle": "OMICS/exosomes",
    "meta": "META",
    "case_list": "META",
    "clinical": "META",
    "pub": "UNSTRUCTURED",
    "paper": "UNSTRUCTURED",
    "pdf": "UNSTRUCTURED",
}

EXT_MAP = {
    ".csv": "tabular",
    ".tsv": "tabular",
    ".parquet": "tabular",
    ".txt": "tabular",
    ".pdf": "docs",
    ".xls": "tabular",
    ".xlsx": "tabular",
    ".json": "tabular",
}

# --- Helpers ---
def classify_file(fp: Path) -> str:
    name = fp.name.lower()
    for key, folder in KEYWORD_MAP.items():
        if key in name:
            return folder
    # fallback: extension-based
    ext = fp.suffix.lower()
    if ext in EXT_MAP:
        if EXT_MAP[ext] == "docs":
            return "UNSTRUCTURED"
        return "OMICS/misc"
    return "_INBOX"

def safe_copy_or_move(src: Path, dst: Path, mode: str, logfh):
    dst.parent.mkdir(parents=True, exist_ok=True)
    if mode == "copy":
        shutil.copy2(src, dst)
    elif mode == "move":
        shutil.move(src, dst)
    else:
        raise ValueError(f"Unknown mode: {mode}")
    logfh.writerow([src.as_posix(), dst.as_posix(), datetime.datetime.now().isoformat()])

def arrange(in_root: Path, out_root: Path, mode: str):
    log_path = Path("logs/arrange.log")
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with open(log_path, "a", encoding="utf-8", newline="") as lf:
        logfh = csv.writer(lf)
        if lf.tell() == 0:
            logfh.writerow(["src", "dst", "timestamp"])

        count = 0
        for fp in in_root.rglob("*"):
            if not fp.is_file():
                continue
            rel_folder = classify_file(fp)
            dst = out_root / rel_folder / fp.name
            try:
                safe_copy_or_move(fp, dst, mode, logfh)
                count += 1
            except Exception as e:
                print(f"[WARN] Failed {fp}: {e}")
        print(f"[DONE] {count} files arranged into {out_root}")

def main():
    ap = argparse.ArgumentParser(description="Arrange raw data into clean hierarchy.")
    ap.add_argument("--in", dest="in_root", required=True, help="Input root folder")
    ap.add_argument("--out", dest="out_root", required=True, help="Output root folder")
    ap.add_argument("--mode", choices=["copy", "move"], default="copy",
                    help="copy (safe, default) or move (after validation)")
    args = ap.parse_args()

    arrange(Path(args.in_root), Path(args.out_root), args.mode)

if __name__ == "__main__":
    main()
