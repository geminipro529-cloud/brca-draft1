# File: scripts/download_brca_batch.py
# Purpose: Crawl data\00_urls\brca\ for URL lists (txt/csv/tsv), and download concurrently with resume & logging.
# Usage:
#   python scripts\download_brca_batch.py --url-dir data\00_urls\brca --out-root data\01_raw\BRCA --workers 8 --index data\01_raw\BRCA\download_index.csv --resume
#
# Notes:
# - Requires scripts/download_one.py in the same repo (imported).
# - Parses TXT (one URL per line; '#' comments OK) and CSV/TSV (column 'url' or any cell containing a URL).
# - Writes/updates a resume index CSV: url,status,path,size,source_file,error,timestamp
# - Idempotent: download_one() already skips if final file exists and non-zero.

import os, sys, re, csv, argparse, time, threading, queue, traceback, datetime
from urllib.parse import urlsplit

# ---- Import the single-file downloader (resumable + atomic) ----
try:
    from scripts.download_one import download_one
except Exception as e:
    raise SystemExit("[ERROR] Cannot import scripts/download_one.py. Place it in scripts/ and try again.") from e

URL_RX = re.compile(r'(?i)\b(?:https?|ftp)://[^\s<>"\'\)\]]+')
CSV_EXT = {".csv", ".tsv"}
TXT_EXT = {".txt"}

def find_manifest_files(root):
    for dp, dn, fn in os.walk(root):
        for f in fn:
            ext = os.path.splitext(f.lower())[1]
            if ext in CSV_EXT or ext in TXT_EXT:
                yield os.path.join(dp, f)

def parse_txt(path):
    urls = []
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"): continue
            # Accept plain URL line or extract any URL substring
            m = URL_RX.findall(line)
            if m:
                urls.extend(m)
    return [(u, path) for u in urls]

def parse_csvlike(path):
    urls = []
    # detect delimiter by extension
    delim = "\t" if path.lower().endswith(".tsv") else ","
    with open(path, "r", encoding="utf-8", errors="ignore", newline="") as f:
        rdr = csv.reader(f, delimiter=delim)
        header = None
        for i, row in enumerate(rdr):
            if not row: continue
            if i == 0:
                header = [c.strip().lower() for c in row]
                # If a 'url' header exists, use that column only
                if "url" in header:
                    url_idx = header.index("url")
                    # read the rest with DictReader to be robust
                    f.seek(0)
                    dr = csv.DictReader(f, delimiter=delim)
                    for r in dr:
                        cell = (r.get("url") or "").strip()
                        if not cell: continue
                        m = URL_RX.findall(cell) or ([cell] if URL_RX.match(cell) else [])
                        urls.extend(m)
                    break
                # else: fall through to scan all cells
            # scan every cell for URL patterns
            for cell in row:
                s = (cell or "").strip()
                if not s: continue
                m = URL_RX.findall(s)
                if m:
                    urls.extend(m)
    return [(u, path) for u in urls]

def load_all_urls(url_dir):
    seen = set()
    tasks = []
    for mf in find_manifest_files(url_dir):
        ext = os.path.splitext(mf.lower())[1]
        items = parse_txt(mf) if ext in TXT_EXT else parse_csvlike(mf)
        for url, src in items:
            u = url.strip()
            if not u: continue
            # Basic sanity
            scheme = urlsplit(u).scheme.lower()
            if scheme not in ("http","https","ftp"): continue
            if u in seen: continue
            seen.add(u)
            tasks.append((u, src))
    return tasks

class IndexWriter:
    def __init__(self, index_path):
        self.path = index_path
        self.lock = threading.Lock()
        os.makedirs(os.path.dirname(index_path), exist_ok=True)
        if not os.path.exists(index_path) or os.path.getsize(index_path)==0:
            with open(index_path, "w", encoding="utf-8", newline="") as f:
                f.write("timestamp,url,status,path,size,source_file,error\n")

    def append(self, row):
        # retry on transient Windows locks
        for i in range(5):
            try:
                with self.lock:
                    with open(self.path, "a", encoding="utf-8", newline="") as f:
                        f.write(row + "\n")
                return
            except OSError:
                time.sleep(0.2*(i+1))
        # last attempt
        with self.lock:
            with open(self.path, "a", encoding="utf-8", newline="") as f:
                f.write(row + "\n")

def load_completed(index_path):
    done = set()
    if not os.path.exists(index_path): return done
    try:
        with open(index_path, "r", encoding="utf-8", errors="ignore") as f:
            rd = csv.DictReader(f)
            for r in rd:
                if (r.get("status") or "").upper() == "OK":
                    url = (r.get("url") or "").strip()
                    if url: done.add(url)
    except Exception:
        # corrupt index shouldn't block; proceed without resume
        pass
    return done

def main():
    ap = argparse.ArgumentParser(description="Batch downloader for BRCA URL manifests.")
    ap.add_argument("--url-dir", default=os.path.join("data","00_urls","brca"))
    ap.add_argument("--out-root", default=os.path.join("data","01_raw","BRCA"))
    ap.add_argument("--index", default=os.path.join("data","01_raw","BRCA","download_index.csv"))
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--resume", action="store_true", help="Skip URLs already marked OK in index")
    args = ap.parse_args()

    tasks = load_all_urls(args.url_dir)
    if not tasks:
        print(f"[INFO] No URLs found under {args.url_dir}")
        return

    if args.resume:
        completed = load_completed(args.index)
        tasks = [t for t in tasks if t[0] not in completed]

    total = len(tasks)
    print(f"[INFO] Found {total} URLs to process.")

    idx = IndexWriter(args.index)
    q = queue.Queue()
    for url, src in tasks: q.put((url, src))

    progress = {"done":0, "ok":0, "err":0}
    prog_lock = threading.Lock()

    def progress_cb(done_bytes, total_bytes):
        # lightweight; per-file progress omitted to avoid garbling multi-thread output
        pass

    def worker():
        while True:
            try:
                url, src = q.get_nowait()
            except queue.Empty:
                return
            ts = datetime.datetime.now().isoformat(timespec="seconds")
            try:
                path = download_one(url, args.out_root, None, progress=progress_cb)
                size = os.path.getsize(path) if os.path.exists(path) else 0
                idx.append(f'{ts},{url},OK,{path},{size},{src},')
                with prog_lock:
                    progress["ok"] += 1
            except Exception as e:
                err = str(e).replace("\n"," ")[:400]
                idx.append(f'{ts},{url},ERR,,0,{src},"{err}"')
                with prog_lock:
                    progress["err"] += 1
            finally:
                with prog_lock:
                    progress["done"] += 1
                    if progress["done"] % 10 == 0 or progress["done"] == total:
                        print(f"[PROGRESS] {progress['done']}/{total} (OK={progress['ok']} ERR={progress['err']})")
                q.task_done()

    threads = []
    for _ in range(max(1, args.workers)):
        t = threading.Thread(target=worker, daemon=True); t.start(); threads.append(t)
    for t in threads: t.join()

    print(f"[DONE] Total={total} OK={progress['ok']} ERR={progress['err']}. Index → {args.index}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[INT] Cancelled by user.")
        sys.exit(130)
