#!/usr/bin/env python3
# scripts/pipeline1_breast_only.py
# Pipeline 1 (breast-only, non-nanomedicine)
# Steps: Convert → Build structured_union (auto) → Annotate → Unstructured → Crosswalk (with fallback) → Master
# Progress bars + checkpoints. FTP removed (different data).

import os, sys, time, json, argparse, subprocess
from typing import List, Dict, Optional, Tuple
import pandas as pd

ROOT        = os.getcwd()
SCRIPTS_DIR = os.path.join(ROOT, "scripts")

BATCH_CONVERT_SCRIPT    = os.path.join(SCRIPTS_DIR, "batch_csvs_to_parquet.py")
STRUCTURED_UNION_BUILDER= os.path.join(SCRIPTS_DIR, "build_structured_union.py")
ANNOTATE_SCRIPT         = os.path.join(SCRIPTS_DIR, "annotate_structured_with_vocab.py")
UNSTRUCTURED_SCRIPT     = os.path.join(SCRIPTS_DIR, "extract_unstructured_lowmem.py")
CROSSWALK_SCRIPT        = os.path.join(SCRIPTS_DIR, "build_id_crosswalk.py")
MASTER_SCRIPT           = os.path.join(SCRIPTS_DIR, "build_master_dataset.py")

RAW_DIR                 = os.path.join("data", "01_raw")
STRUCTURED_UNION_DIR    = os.path.join("data", "02_interim", "structured_union")
VOCAB_PARQUET           = os.path.join("data", "02_interim", "VOCAB_COMPILED", "vocabulary_enriched.parquet")
UNSTRUCTURED_IN         = os.path.join("data", "01_raw", "UNSTRUCTURED")
ANNOTATED_V2_DIR        = os.path.join("data", "02_interim", "structured_annotated_v2")
UNSTRUCTURED_OUT        = os.path.join("data", "02_interim", "unstructured_lowmem")
OLD_ANNOTATED_DIR       = os.path.join("data", "02_interim", "structured_annotated")
ID_MAP_DIR              = os.path.join("data", "02_interim", "id_map")
PARQUET_OUT_DIR         = os.path.join("data", "03_processed", "parquet")
MASTER_OUT_DIR          = os.path.join("data", "03_processed", "master_dataset")

STATE_DIR               = os.path.join("data", "02_interim", "run_state")
STATE_JSON              = os.path.join(STATE_DIR, "pipeline1_breast_only_state.json")

# ---- Rich UI ----
try:
    from rich.console import Console
    from rich.progress import Progress, TextColumn, BarColumn, TimeRemainingColumn
except Exception:
    print("[ERROR] Missing 'rich'. Install with: pip install rich", file=sys.stderr)
    raise

console = Console(force_terminal=True)

# ---- helpers ----
def ensure_dir(p: str): os.makedirs(p, exist_ok=True)

def save_state(state: Dict):
    ensure_dir(os.path.dirname(STATE_JSON))
    tmp = STATE_JSON + ".part"
    with open(tmp, "w", encoding="utf-8") as f: json.dump(state, f, indent=2)
    os.replace(tmp, STATE_JSON)

def load_state() -> Dict:
    if not os.path.exists(STATE_JSON): return {}
    try:
        with open(STATE_JSON, "r", encoding="utf-8") as f: return json.load(f)
    except: return {}

def exe_stream(cmd: List[str], timeout=None, on_line=None) -> Tuple[int,float]:
    start = time.time()
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            bufsize=1, universal_newlines=True)
    try:
        for line in proc.stdout:
            if on_line: on_line(line.rstrip("\r\n"))
        rc = proc.wait(timeout=timeout)
        return rc, time.time()-start
    except subprocess.TimeoutExpired:
        proc.kill(); return 124, time.time()-start

def step_done(state, key): return state.get("steps",{}).get(key,{}).get("status")=="ok"

def mark_step(state, key, status, rc, dt, note=""):
    st=state.setdefault("steps",{})
    st[key]=dict(status=status,rc=rc,dt=round(dt,2),ts=time.strftime("%Y-%m-%d %H:%M:%S"),note=note)
    save_state(state)

def structured_union_exists() -> bool:
    return os.path.isdir(STRUCTURED_UNION_DIR) and any(
        fn.lower().endswith((".parquet",".csv")) for fn in os.listdir(STRUCTURED_UNION_DIR)
    )

# ---- crosswalk helpers ----
def dir_first_file(d: str) -> Optional[str]:
    if not os.path.isdir(d): return None
    for f in os.listdir(d):
        if f.lower().endswith((".parquet",".csv",".tsv")):
            return os.path.join(d,f)
    return None

def safe_read(path: str) -> pd.DataFrame:
    try:
        if path.endswith(".parquet"): return pd.read_parquet(path)
        if path.endswith(".csv"): return pd.read_csv(path)
        if path.endswith(".tsv"): return pd.read_csv(path,sep="\t")
    except Exception as e: console.print(f"[yellow]⚠️ {path}: {e}[/yellow]")
    return pd.DataFrame()

def atomic_write_parquet(df, out): df.to_parquet(out+".tmp",index=False); os.replace(out+".tmp",out)
def atomic_write_csv(df, out): df.to_csv(out+".tmp",index=False); os.replace(out+".tmp",out)

def write_crosswalk_outputs(df):
    ensure_dir(ID_MAP_DIR)
    for name in ("id_crosswalk","id_map"):
        atomic_write_parquet(df, os.path.join(ID_MAP_DIR,f"{name}.parquet"))
        atomic_write_csv(df, os.path.join(ID_MAP_DIR,f"{name}.csv"))

def pick_key(cols_a, cols_b):
    pref=["sample","sample_id","patient","patient_id","case_id","aliquot_id","barcode","uuid","id","ID"]
    for k in pref:
        if k in cols_a and k in cols_b: return k
    inter=list(set(cols_a)&set(cols_b))
    return inter[0] if inter else None

def build_crosswalk_fallback() -> str:
    newf, oldf = dir_first_file(ANNOTATED_V2_DIR), dir_first_file(OLD_ANNOTATED_DIR)
    if not newf or not oldf:
        write_crosswalk_outputs(pd.DataFrame())
        return "No annotated v2 or old annotated"
    df_new, df_old = safe_read(newf), safe_read(oldf)
    if df_new.empty or df_old.empty:
        write_crosswalk_outputs(pd.DataFrame()); return "Empty inputs"
    key=pick_key(df_new.columns.tolist(),df_old.columns.tolist())
    if not key:
        write_crosswalk_outputs(pd.DataFrame()); return "No common keys"
    df=pd.merge(df_new[[key]].drop_duplicates(), df_old[[key]].drop_duplicates(),
                on=key, how="outer")
    write_crosswalk_outputs(df)
    return f"Fallback built on '{key}', rows={len(df)}"

# ---- pipeline ----
def run_pipeline(args):
    state=load_state()
    will_run=[]
    if os.path.exists(BATCH_CONVERT_SCRIPT): will_run.append("convert")
    if os.path.exists(ANNOTATE_SCRIPT):      will_run.append("annotate")
    if os.path.exists(UNSTRUCTURED_SCRIPT) and os.path.isdir(UNSTRUCTURED_IN):
        will_run.append("unstructured")
    if os.path.isdir(OLD_ANNOTATED_DIR):     will_run.append("crosswalk")
    if os.path.exists(MASTER_SCRIPT):        will_run.append("master")
    total=len(will_run)

    with Progress(TextColumn("{task.description}"),BarColumn(),
                  TextColumn("{task.percentage:>3.0f}%"),"•",TimeRemainingColumn(),
                  console=console,expand=True) as progress:
        overall=progress.add_task(f"Pipeline1 breast-only ({total} steps)",total=total)

        # ---- CONVERT ----
        if "convert" in will_run:
            if args.resume and step_done(state,"convert"): progress.advance(overall,1)
            else:
                desc="1/{} Convert CSV→Parquet".format(total)
                sub=progress.add_task(desc,total=100)
                cmd=[sys.executable,BATCH_CONVERT_SCRIPT,
                     "--in-dir",RAW_DIR,"--out-dir",PARQUET_OUT_DIR,
                     "--min-mb",str(args.min_mb),"--workers",str(args.workers)]
                rc,dt=exe_stream(cmd,on_line=lambda l:progress.advance(sub,1),timeout=args.convert_timeout)
                if rc==0: mark_step(state,"convert","ok",rc,dt); progress.update(sub,completed=100); progress.advance(overall,1)
                else: mark_step(state,"convert","failed",rc,dt); return

        # ---- STRUCTURED UNION AUTO ----
        if not structured_union_exists():
            desc="Build structured_union (auto)"
            sub=progress.add_task(desc,total=100)
            if os.path.exists(STRUCTURED_UNION_BUILDER):
                cmd=[sys.executable,STRUCTURED_UNION_BUILDER,"--in-dirs","data/01_raw","data/02_interim"]
                rc,dt=exe_stream(cmd,on_line=lambda l:progress.advance(sub,5),timeout=1800)
                if rc==0: progress.update(sub,completed=100); console.print("[green]structured_union built[/green]")
                else: console.print("[red]structured_union build failed[/red]"); return
            else:
                console.print(f"[red]Missing {STRUCTURED_UNION_BUILDER}[/red]"); return

        # ---- ANNOTATE ----
        if "annotate" in will_run:
            if args.resume and step_done(state,"annotate"): progress.advance(overall,1)
            else:
                desc="Annotate structured with vocab"; sub=progress.add_task(desc,total=100)
                cmd=[sys.executable,ANNOTATE_SCRIPT,"--in",STRUCTURED_UNION_DIR,"--vocab",VOCAB_PARQUET,"--out",ANNOTATED_V2_DIR]
                rc,dt=exe_stream(cmd,on_line=lambda l:progress.advance(sub,1),timeout=args.annotate_timeout)
                if rc==0: mark_step(state,"annotate","ok",rc,dt); progress.update(sub,completed=100); progress.advance(overall,1)
                else: mark_step(state,"annotate","failed",rc,dt); return

        # ---- UNSTRUCTURED ----
        if "unstructured" in will_run:
            if args.resume and step_done(state,"unstructured"): progress.advance(overall,1)
            else:
                desc="Extract unstructured"; sub=progress.add_task(desc,total=100)
                cmd=[sys.executable,UNSTRUCTURED_SCRIPT,"--in",UNSTRUCTURED_IN,"--vocab",VOCAB_PARQUET,
                     "--out",UNSTRUCTURED_OUT,"--batch-sents","300","--max-chars","800000"]
                rc,dt=exe_stream(cmd,on_line=lambda l:progress.advance(sub,1),timeout=args.unstructured_timeout)
                if rc==0: mark_step(state,"unstructured","ok",rc,dt); progress.update(sub,completed=100); progress.advance(overall,1)
                else: mark_step(state,"unstructured","failed",rc,dt,note="optional"); console.print("[yellow]Unstructured failed, continuing[/yellow]")

        # ---- CROSSWALK ----
        if "crosswalk" in will_run:
            if args.resume and step_done(state,"crosswalk"): progress.advance(overall,1)
            else:
                desc="Crosswalk IDs"; sub=progress.add_task(desc,total=100)
                rc,dt=1,0.0
                if os.path.exists(CROSSWALK_SCRIPT):
                    cmd=[sys.executable,CROSSWALK_SCRIPT,"--in-v2",ANNOTATED_V2_DIR,"--in-old",OLD_ANNOTATED_DIR,"--out",ID_MAP_DIR,"--prefer","sample"]
                    rc,dt=exe_stream(cmd,on_line=lambda l:progress.advance(sub,3),timeout=args.crosswalk_timeout)
                if rc==0:
                    mark_step(state,"crosswalk","ok",rc,dt); progress.update(sub,completed=100); progress.advance(overall,1)
                else:
                    note=build_crosswalk_fallback()
                    mark_step(state,"crosswalk","ok",0,dt,note=f"fallback: {note}")
                    progress.update(sub,completed=100); progress.advance(overall,1)
                    console.print(f"[yellow]Crosswalk fallback used: {note}[/yellow]")

        # ---- MASTER ----
        if "master" in will_run:
            if args.resume and step_done(state,"master"): progress.advance(overall,1)
            else:
                desc="Build master dataset"; sub=progress.add_task(desc,total=100)
                cmd=[sys.executable,MASTER_SCRIPT,"--structured",ANNOTATED_V2_DIR,
                     "--quant",os.path.join("data","02_interim","quantitative"),
                     "--qual",UNSTRUCTURED_OUT,"--out",MASTER_OUT_DIR,"--breast-only"]
                rc,dt=exe_stream(cmd,on_line=lambda l:progress.advance(sub,2),timeout=args.master_timeout)
                if rc==0: mark_step(state,"master","ok",rc,dt); progress.update(sub,completed=100); progress.advance(overall,1)
                else: mark_step(state,"master","failed",rc,dt); return

        progress.update(overall,completed=total)
    console.print("[green]Pipeline1 finished[/green]")
    console.print(f"State saved at {STATE_JSON}")

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--convert-timeout",type=int,default=8*3600)
    ap.add_argument("--min-mb",type=float,default=700.0)
    ap.add_argument("--workers",type=int,default=2)
    ap.add_argument("--annotate-timeout",type=int,default=3600)
    ap.add_argument("--unstructured-timeout",type=int,default=3600)
    ap.add_argument("--crosswalk-timeout",type=int,default=1800)
    ap.add_argument("--master-timeout",type=int,default=3600)
    ap.add_argument("--resume",action="store_true")
    args=ap.parse_args()
    for d in [PARQUET_OUT_DIR,ANNOTATED_V2_DIR,UNSTRUCTURED_OUT,ID_MAP_DIR,MASTER_OUT_DIR,STATE_DIR]:
        ensure_dir(d)
    run_pipeline(args)

if __name__=="__main__": main()
