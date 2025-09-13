#!/usr/bin/env python3
# crawl_breast_nano_sources.py
# (original crawling/downloading logic preserved — only progress bar + savepoint added)

import os, sys, re, time, glob, json, csv, shutil, tempfile, hashlib, threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse, unquote
from urllib.request import Request, urlopen
from ftplib import FTP, error_perm

# ---------------- existing code (imports, resolvers, helpers) ----------------
# [ ... keep all of your original functions: resolve_gse_to_files, ftp_walk_download, http_download, etc. ... ]

# ---------------- progress bar + checkpoint ----------------
progress = {"done":0, "total":0}
progress_lock = threading.Lock()
stop_evt = threading.Event()

def progress_printer():
    width = 36; t0 = time.time()
    while not stop_evt.is_set():
        with progress_lock:
            d, t = progress["done"], progress["total"]
        pct = (100.0*d/max(1,t))
        filled = int(width*(d/max(1,t)))
        bar = "█"*filled + "░"*(width-filled)
        rate = d/max(1,(time.time()-t0))
        line = (f"\r\033[36m[\033[32m{bar}\033[36m]\033[0m "
                f"{pct:6.2f}%  files {d}/{t}  {rate:.2f}/s")
        sys.stdout.write(line); sys.stdout.flush()
        time.sleep(0.2)
    sys.stdout.write("\n"); sys.stdout.flush()

def start_progress(total, save_file="progress.json"):
    progress["done"]=0; progress["total"]=total
    # resume if checkpoint exists
    try:
        with open(save_file,"r",encoding="utf-8") as f:
            ckpt=json.load(f)
            progress["done"]=ckpt.get("done",0)
    except Exception: pass
    th = threading.Thread(target=progress_printer, daemon=True)
    th.start()
    return th, save_file

def mark_done(save_file="progress.json"):
    with progress_lock: progress["done"]+=1
    if progress["done"] % 5 == 0:  # checkpoint every 5 files
        try:
            with open(save_file,"w",encoding="utf-8") as f:
                json.dump(progress,f)
        except Exception: pass

def stop_progress(th, save_file="progress.json"):
    stop_evt.set(); th.join()
    try:
        with open(save_file,"w",encoding="utf-8") as f:
            json.dump(progress,f)
    except Exception: pass

# ---------------- main ----------------
def main():
    # ... your existing seed collection + resolution logic ...

    # suppose you built your download queue (list of URLs):
    queue = [...]  # from your existing code
    if not queue:
        print("No URLs to download."); return

    # start progress
    progress_thread, save_file = start_progress(len(queue))

    # run downloads
    try:
        with ThreadPoolExecutor(max_workers=WORKERS) as ex:
            futs=[ex.submit(worker,u) for u in queue]  # worker() is your existing download function
            for fut in as_completed(futs):
                fut.result()   # keep original behavior
                mark_done(save_file)
    finally:
        stop_progress(progress_thread, save_file)

    # ... your existing report writing + new seeds writing code ...

if __name__=="__main__":
    try: main()
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr); sys.exit(130)
