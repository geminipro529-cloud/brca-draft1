#!/usr/bin/env python3
# build_move_plan.py — Generate a src→dst move plan from report.csv
import csv, re, argparse
from pathlib import Path

BRCA_RE   = re.compile(r"(brca|breast|metabric|tcga[-_]?brca)", re.I)
NANO_RE   = re.compile(r"(nano|liposome|micelle|vesicle|exosome|cananolab|euon)", re.I)

EXT_BUCKET = {
    ".pdf": ("DOCS", None),
    ".csv": ("OMICS", "rnaseq"),
    ".tsv": ("OMICS", "rnaseq"),
    ".parquet": ("OMICS", "rnaseq"),
    ".xlsx": ("OMICS", "rnaseq"),
    ".xls": ("OMICS", "rnaseq"),
    ".json": ("CLINICAL", None),
}

def decide_scope(name: str) -> str:
    if BRCA_RE.search(name): return "BRCA"
    if NANO_RE.search(name): return "NANOMEDICINE"
    return "GENERIC"

def build_move_plan(report_csv: Path, out_csv: Path, raw_root="data/01_raw"):
    rows=[]
    with report_csv.open("r", encoding="utf-8") as f:
        r=csv.DictReader(f)
        for row in r:
            status=row["status"]
            if status not in {"MISPLACED","OUT_OF_TREE","EXT_MISMATCH"}:
                continue
            src=Path(row["file"])
            name=src.name.lower()
            ext=src.suffix.lower()
            bucket,sub=EXT_BUCKET.get(ext,("OMICS","misc"))
            scope=decide_scope(name)
            if sub:
                dst=Path(raw_root)/scope/bucket/sub/src.name
            else:
                dst=Path(raw_root)/scope/bucket/src.name
            rows.append((src,dst))
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w", encoding="utf-8", newline="") as f:
        w=csv.writer(f); w.writerow(["src","dst"])
        w.writerows([[s,d] for s,d in rows])
    print(f"[OK] Move plan written → {out_csv} ({len(rows)} entries)")

def main():
    ap=argparse.ArgumentParser(description="Build move plan from report.csv")
    ap.add_argument("--report", default="data/01_raw/_validation/report.csv")
    ap.add_argument("--out", default="data/01_raw/_validation/move_plan.csv")
    args=ap.parse_args()
    build_move_plan(Path(args.report), Path(args.out))

if __name__=="__main__":
    main()
