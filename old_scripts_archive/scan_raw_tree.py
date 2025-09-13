#!/usr/bin/env python3
# Quick inventory: show counts/sizes by folder & extension; writes CSVs, logs once.
from __future__ import annotations
import os, sys, csv, time, argparse
from pathlib import Path
from collections import Counter, defaultdict

LOG = Path("logs") / "log.txt"

def log(msg:str)->None:
    LOG.parent.mkdir(parents=True, exist_ok=True)
    with LOG.open("a", encoding="utf-8") as fh:
        fh.write(time.strftime("[%Y-%m-%d %H:%M:%S] ")+msg+"\n")

def walk_files(roots:list[Path])->list[Path]:
    out=[]
    for r in roots:
        if not r.exists(): continue
        for p in r.rglob("*"):
            if p.is_file(): out.append(p)
    return out

def main():
    ap = argparse.ArgumentParser(description="Scan folders and summarize contents.")
    ap.add_argument("--roots", nargs="*", default=[os.path.join("data","01_raw")])
    ap.add_argument("--out", default=os.path.join("data","01_raw","_scan"))
    args = ap.parse_args()

    roots=[Path(x) for x in args.roots]
    files=walk_files(roots)
    by_ext=Counter()
    by_dir_count=Counter()
    by_dir_bytes=defaultdict(int)
    total_bytes=0

    for p in files:
        ext=p.suffix.lower() or "<none>"
        by_ext[ext]+=1
        d=str(p.parent)
        by_dir_count[d]+=1
        try: sz=p.stat().st_size
        except Exception: sz=0
        by_dir_bytes[d]+=sz
        total_bytes+=sz

    out_dir=Path(args.out); out_dir.mkdir(parents=True, exist_ok=True)
    with (out_dir/"dirs.csv").open("w",encoding="utf-8",newline="") as fh:
        w=csv.writer(fh); w.writerow(["dir","files","bytes"])
        for d in sorted(by_dir_count, key=lambda x: (-by_dir_count[x], x))[:500]:
            w.writerow([d, by_dir_count[d], by_dir_bytes[d]])
    with (out_dir/"exts.csv").open("w",encoding="utf-8",newline="") as fh:
        w=csv.writer(fh); w.writerow(["ext","count"])
        for ext,c in by_ext.most_common(200):
            w.writerow([ext,c])

    print(f"Scanned files={len(files)} total_bytes={total_bytes}")
    print(f"[OUT] {out_dir/'dirs.csv'}  |  {out_dir/'exts.csv'}")
    log(f"[SCAN] roots={roots} files={len(files)} bytes={total_bytes} out={out_dir}")

if __name__=="__main__":
    main()
