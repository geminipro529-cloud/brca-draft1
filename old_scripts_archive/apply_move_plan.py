#!/usr/bin/env python3
# apply_move_plan.py — execute a move/copy plan from CSV with workers & progress
import os, sys, csv, argparse, shutil, time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from random import random
from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn

def parse_args():
    p = argparse.ArgumentParser(description="Apply a file move/copy plan from CSV")
    p.add_argument("--plan", required=True, help="CSV file with src,dst columns")
    p.add_argument("--mode", choices=["move","copy"], default="move")
    p.add_argument("--workers", type=int, default=4, help="Parallel workers")
    p.add_argument("--dry-run", action="store_true", help="Simulate only")
    return p.parse_args()

def safe_action(src: Path, dst: Path, mode: str, dry: bool) -> str:
    if not src.exists(): return "missing"
    if dst.exists(): return "skipped"
    if dry: return "dry"
    dst.parent.mkdir(parents=True, exist_ok=True)
    # retry 3x in case of Windows AV/lock delays
    for _ in range(3):
        try:
            if mode == "move": shutil.move(str(src), str(dst))
            else: shutil.copy2(str(src), str(dst))
            return "done"
        except Exception:
            time.sleep(0.2 + random()/5)
    return "error"

def main():
    args = parse_args()
    plan_file = Path(args.plan)
    if not plan_file.exists():
        sys.exit(f"[ERR] Plan file not found: {plan_file}")

    rows = []
    with open(plan_file, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            if not r.get("src") or not r.get("dst"): continue
            rows.append((Path(r["src"]), Path(r["dst"])))

    moved=skipped=missing=errors=dry=0
    with Progress(TextColumn("{task.description}"),
                  BarColumn(), TextColumn("{task.completed}/{task.total}"),
                  TimeRemainingColumn()) as progress:
        task = progress.add_task("Apply Move Plan", total=len(rows))
        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            futures = {ex.submit(safe_action, s, d, args.mode, args.dry_run):(s,d) for s,d in rows}
            for fut in as_completed(futures):
                res = fut.result()
                if res=="done": moved+=1
                elif res=="skipped": skipped+=1
                elif res=="missing": missing+=1
                elif res=="dry": dry+=1
                else: errors+=1
                progress.update(task, advance=1)

    print(f"Done. moved={moved} skipped={skipped} missing={missing} dry={dry} errors={errors}")

if __name__=="__main__":
    main()
