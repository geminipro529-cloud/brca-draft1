#!/usr/bin/env python3
# Stage 1 (safe): Build a harmonized metadata master table + summary
# Skips rnaseq mega-tables unless explicitly allowed.

import os, sys, argparse
from pathlib import Path
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from rich.progress import Progress, BarColumn, TimeRemainingColumn, TextColumn, TaskProgressColumn
from rich.table import Table
from rich.console import Console

ALIASES = {
    "case_id":     ["case", "patient", "patient_id", "case_id"],
    "sample_id":   ["sample", "sampleid", "sample_id"],
    "portion_id":  ["portion", "portion_id"],
    "analyte_id":  ["analyte", "analyte_id"],
    "aliquot_id":  ["aliquot", "aliquot_id"],
    "slide_id":    ["slide", "slide_id"],
}
PRIORITY = ["aliquot_id","sample_id","case_id"]  # merge preference

def normalize_columns(cols):
    normed=[]
    for c in cols:
        c_low=c.strip().lower()
        match=None
        for canon,aliases in ALIASES.items():
            if c_low in aliases:
                match=canon; break
        normed.append(match if match else c_low)
    return normed

def read_any(path: Path) -> pd.DataFrame:
    ext=path.suffix.lower()
    if ext in (".csv",".tsv"):
        sep="\t" if ext==".tsv" else ","
        return pd.read_csv(path,dtype=str,sep=sep,low_memory=False)
    if ext in (".parquet",".pq"):
        return pd.read_parquet(path).astype(str)
    return pd.DataFrame()

def choose_id(cols):
    for key in PRIORITY:
        if key in cols: return key
    return None

def build_master(structured_dir: Path, workers: int, skip_rnaseq: bool, progress):
    files=[f for f in structured_dir.rglob("*") if f.is_file()]
    if skip_rnaseq:
        files=[f for f in files if "rnaseq" not in f.name.lower()]
    if not files: return pd.DataFrame()

    dfs=[]; success=0; failed=0
    task=progress.add_task("[cyan]Reading structured files", total=len(files))

    def _load(f):
        try:
            df=read_any(f)
            if df.empty: return None,False
            df.columns=normalize_columns(df.columns)
            df["__sourcefile"]=f.name
            return df,True
        except Exception as e:
            print(f"[WARN] {f} read failed: {e}", file=sys.stderr)
            return None,False

    from collections import defaultdict
    grouped=defaultdict(list)

    with ThreadPoolExecutor(max_workers=max(1,workers)) as ex:
        futs={ex.submit(_load,f):f for f in files}
        for fut in as_completed(futs):
            df,ok=fut.result()
            if ok and df is not None:
                key=choose_id(df.columns)
                if key:
                    grouped[key].append(df); success+=1
                else: failed+=1
            else: failed+=1
            progress.update(task,advance=1,
                description=f"[cyan]{success} ok / {failed} skipped")

    masters=[]
    for key,dfs_list in grouped.items():
        cat=pd.concat(dfs_list,ignore_index=True).drop_duplicates()
        masters.append(cat)

    if not masters: return pd.DataFrame()
    master=masters[0]
    for df in masters[1:]:
        join_key=[c for c in PRIORITY if c in master.columns and c in df.columns]
        if join_key:
            master=pd.merge(master,df,how="outer",on=join_key)
        else:
            master=pd.concat([master,df],ignore_index=True,sort=False)

    return master

def summarize(master: pd.DataFrame, outpath: Path):
    rows=[]
    for key in ("case_id","sample_id","portion_id","analyte_id","aliquot_id","slide_id"):
        if key in master.columns:
            rows.append((key,master[key].nunique(dropna=True)))
    rows.append(("rows_total",len(master)))
    rows.append(("cols_total",len(master.columns)))
    pd.DataFrame(rows,columns=["key","count"]).to_csv(outpath,index=False)
    return rows

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--source",required=True)
    ap.add_argument("--out",default="data/02_interim")
    ap.add_argument("--workers",type=int,default=4)
    ap.add_argument("--skip-rnaseq",action="store_true",
                    help="Skip rnaseq mega-tables (default: include all files)")
    args=ap.parse_args()

    structured_dir=Path("data")/"01_arranged"/args.source/"structured"
    if not structured_dir.exists():
        print(f"[ERROR] missing {structured_dir}",file=sys.stderr);sys.exit(1)

    outdir=Path(args.out);outdir.mkdir(parents=True,exist_ok=True)
    outpath=outdir/f"{args.source}_metadata_master.parquet"
    sumpath=outdir/f"{args.source}_metadata_summary.csv"

    console=Console()
    with Progress(TextColumn("[cyan]{task.description}"),BarColumn(),
                  TaskProgressColumn(),TimeRemainingColumn(),transient=False) as prog:
        master=build_master(structured_dir,args.workers,args.skip_rnaseq,prog)

    if master.empty:
        print("[WARN] no structured metadata tables found.");return

    master.to_parquet(outpath,index=False)
    rows=summarize(master,sumpath)

    print(f"\n[INFO] master saved → {outpath} ({len(master)} rows, {len(master.columns)} cols)")
    print(f"[INFO] summary saved → {sumpath}\n")

    t=Table(title=f"{args.source.upper()} Metadata Summary",show_lines=True)
    t.add_column("Key",style="cyan"); t.add_column("Count",style="magenta")
    for key,count in rows: t.add_row(str(key),str(count))
    console.print(t)

if __name__=="__main__": main()
