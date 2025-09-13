# File: scripts/fetch_from_manifest.py
# Purpose: Fetch datasets from a manifest folder (like fetch_nanomedicine.py)

import os, sys, argparse, time, traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.error import URLError
from download_one import download_one
def main(manifest_dir, out_root, workers, log_file):
    if not os.path.isdir(manifest_dir):
        sys.exit(f"[ERROR] Manifest dir not found: {manifest_dir}")
    os.makedirs(out_root, exist_ok=True)
    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    # Load URLs from all .txt files in manifest_dir
    urls = []
    for f in os.listdir(manifest_dir):
        if f.lower().endswith(".txt"):
            with open(os.path.join(manifest_dir, f), encoding="utf-8") as fh:
                urls.extend(line.strip() for line in fh if line.strip())

    if not urls:
        sys.exit("[ERROR] No URLs found in manifest dir")

    done = set()
    if os.path.exists(log_file):
        with open(log_file, encoding="utf-8") as lf:
            done = {line.split("\t",1)[0] for line in lf if line.startswith("OK")}

    todo = [u for u in urls if u not in done]
    if not todo:
        print("[INFO] All URLs already fetched.")
        return

    print(f"[INFO] Starting {len(todo)} downloads with {workers} workers")

    with ThreadPoolExecutor(max_workers=workers) as ex, open(log_file,"a",encoding="utf-8") as lf:
        futures = {ex.submit(download_one,u,out_root): u for u in todo}
        for fut in as_completed(futures):
            url = futures[fut]
            try:
                path = fut.result()
                lf.write(f"OK\t{url}\t{path}\n")
                print(f"[OK] {url}")
            except URLError as e:
                lf.write(f"FAIL\t{url}\t{e}\n")
                print(f"[FAIL] {url} -> {e}")
            except Exception as e:
                lf.write(f"ERR\t{url}\t{traceback.format_exc(limit=1)}\n")
                print(f"[ERR] {url} -> {e}")
            lf.flush()

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--manifest", required=True, help="Directory containing URL list .txt files")
    ap.add_argument("--out-root", required=True)
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--log", required=True)
    a = ap.parse_args()
    main(a.manifest, a.out_root, a.workers, a.log)
