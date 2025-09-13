#!/usr/bin/env python3
# scripts/collect_case_lists.py
# Parse cohort membership case list .txt files into a structured table

import os, sys, argparse, traceback, re
import pandas as pd

OUTDIR = os.path.join("data", "02_interim", "case_lists")

def ensure_dir(p): os.makedirs(p, exist_ok=True)

def parse_case_list(path):
    """Return (study, list_name, ids[]) from a cBioPortal-style case list file"""
    study, list_name, ids = None, None, []
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line=line.strip()
            if line.startswith("cancer_study_identifier:"): study=line.split(":",1)[1].strip()
            elif line.startswith("case_list_name:"): list_name=line.split(":",1)[1].strip()
            elif line.startswith("case_list_ids:"): 
                ids = re.split(r"\s+", line.split(":",1)[1].strip())
    return study, list_name, ids

def collect_case_lists(indirs, outdir):
    ensure_dir(outdir)
    records=[]
    for d in indirs:
        if not os.path.isdir(d): continue
        for root, _, files in os.walk(d):
            for fn in files:
                if fn.lower().startswith("cases") and fn.lower().endswith(".txt"):
                    path=os.path.join(root,fn)
                    try:
                        study, list_name, ids=parse_case_list(path)
                        for sid in ids:
                            records.append({"study":study,"case_list":list_name,"sample_id":sid})
                    except Exception as e:
                        print(f"⚠️ Failed to parse {path}: {e}")

    if not records:
        print("⚠️ No case list files found")
        return

    df=pd.DataFrame.from_records(records)
    out_csv=os.path.join(outdir,"case_lists.csv")
    out_parq=os.path.join(outdir,"case_lists.parquet")
    df.to_csv(out_csv,index=False)
    df.to_parquet(out_parq,index=False)
    print(f"✅ case_lists saved: {out_csv} ({len(df)} rows)")

def main():
    ap=argparse.ArgumentParser(description="Collect case list files into a structured table")
    ap.add_argument("--in-dirs",nargs="+",default=["data/01_raw"],help="Dirs to scan")
    args=ap.parse_args()
    try:
        collect_case_lists(args.in_dirs,OUTDIR)
    except Exception:
        traceback.print_exc(); sys.exit(1)

if __name__=="__main__":
    main()
