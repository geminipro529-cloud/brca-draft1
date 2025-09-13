#!/usr/bin/env python3
"""
merge_unstructured_outputs.py
Unify multiple unstructured_* folders into one folder.

- Recursively scans all unstructured_* subfolders.
- Merges annotations.jsonl into one.
- Merges doc_index.csv into one.
- Copies text_dump/ folders under unified output.
- Copies all *.json files under json/<source_folder>/.
- Copies any other files into misc/<source_folder>/ preserving structure.
- Writes manifest.csv with all discovered files.
"""

import argparse
from pathlib import Path
import shutil
import csv

def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)

def merge_annotations(all_ann_files, out_file):
    with open(out_file, "w", encoding="utf-8") as out_fh:
        for ann_file in all_ann_files:
            print(f"[INFO] Adding {ann_file}")
            with open(ann_file, "r", encoding="utf-8") as fh:
                for line in fh:
                    out_fh.write(line)
    print(f"[DONE] Merged annotations → {out_file}")

def merge_doc_indexes(all_idx_files, out_file):
    header_written = False
    with open(out_file, "w", encoding="utf-8", newline="") as out_fh:
        for idx_file in all_idx_files:
            print(f"[INFO] Adding {idx_file}")
            with open(idx_file, "r", encoding="utf-8") as fh:
                for i, line in enumerate(fh):
                    if i == 0:
                        if not header_written:
                            out_fh.write(line)
                            header_written = True
                    else:
                        out_fh.write(line)
    print(f"[DONE] Merged doc_index → {out_file}")

def copy_file(src, dest):
    ensure_dir(dest.parent)
    shutil.copy2(src, dest)

def main():
    ap = argparse.ArgumentParser(description="Merge multiple unstructured_* folders into one unified output.")
    ap.add_argument("--in-root", required=True, help="Root folder containing unstructured_* subfolders")
    ap.add_argument("--out", required=True, help="Destination folder for merged outputs")
    args = ap.parse_args()

    in_root = Path(args.in_root).resolve()
    out_dir = Path(args.out).resolve()
    ensure_dir(out_dir)

    in_dirs = [p for p in in_root.iterdir() if p.is_dir() and p.name.startswith("unstructured_")]
    if not in_dirs:
        print(f"[WARN] No unstructured_* folders found under {in_root}")
        return

    print(f"[INFO] Found {len(in_dirs)} input folders")

    ann_files, idx_files, json_files, misc_files = [], [], [], []
    manifest_rows = []

    for d in in_dirs:
        for f in d.rglob("*"):
            if f.is_dir():
                continue
            rel = f.relative_to(d)
            size = f.stat().st_size
            mtime = f.stat().st_mtime
            manifest_rows.append([d.name, str(rel), size, mtime])

            if f.name == "annotations.jsonl":
                ann_files.append(f)
            elif f.name == "doc_index.csv":
                idx_files.append(f)
            elif f.suffix.lower() == ".json":
                json_files.append((d, f))
            else:
                misc_files.append((d, f))

    # Merge annotations
    if ann_files:
        merge_annotations(ann_files, out_dir / "annotations.jsonl")

    # Merge doc_index
    if idx_files:
        merge_doc_indexes(idx_files, out_dir / "doc_index.csv")

    # Copy json files
    for d, f in json_files:
        dest = out_dir / "json" / d.name / f.relative_to(d)
        print(f"[INFO] Copying JSON {f} → {dest}")
        copy_file(f, dest)

    # Copy misc files (everything else)
    for d, f in misc_files:
        if "text_dump" in f.parts:
            dest = out_dir / "text_dump" / d.name / f.relative_to(d)
        else:
            dest = out_dir / "misc" / d.name / f.relative_to(d)
        print(f"[INFO] Copying {f} → {dest}")
        copy_file(f, dest)

    # Write manifest
    manifest_file = out_dir / "manifest.csv"
    with open(manifest_file, "w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["source_folder", "relpath", "size_bytes", "mtime"])
        writer.writerows(manifest_rows)

    print(f"[DONE] Wrote manifest → {manifest_file}")
    print(f"[FINAL] Unified unstructured outputs in {out_dir}")

if __name__ == "__main__":
    main()
