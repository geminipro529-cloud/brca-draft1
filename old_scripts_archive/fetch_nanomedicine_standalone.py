#!/usr/bin/env python3
"""
Fetch NANOMEDICINE datasets from a folder of .txt files.

Features:
- Reads all *.txt files under: data/00_urls/nanomedicine/
- Extracts only valid URLs (ignores summaries/extra text).
- Auto-swaps FTP → HTTPS (NCBI, PRIDE).
- Parallel downloads with --workers N.
- Logs incrementally (safe to stop/resume anytime).
- Resumable: --resume retries only failed URLs.
- Skips already-successful ones.
- Shows progress bar with tqdm.
- Prints summary at the end.
"""

import os, re, csv, time, requests, argparse
from pathlib import Path
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm  # pip install tqdm

# ---------- config ----------
URL_DIR = Path(r"D:\breast_cancer_project_root\data\00_urls\nanomedicine")
OUT_DIR = Path(r"D:\breast_cancer_project_root\data\01_raw\zipped\NANOMEDICINE")
INDEX_FILE = OUT_DIR / "download_index.csv"
FAILED_FILE = OUT_DIR / "failed_downloads.csv"
TIMEOUT = 30
RETRIES = 2

MIRROR_MAP = {
    "ftp://ftp.ncbi.nlm.nih.gov": "https://ftp.ncbi.nlm.nih.gov",
    "ftp://ftp.pride.ebi.ac.uk": "https://www.ebi.ac.uk/pride",
}

URL_REGEX = re.compile(r"https?://[^\s]+|ftp://[^\s]+")

# ---------- helpers ----------
def normalize_url(url: str) -> str:
    for k, v in MIRROR_MAP.items():
        if url.startswith(k):
            return url.replace(k, v)
    return url

def filename_from_url(url: str) -> str:
    parsed = urlparse(url)
    fname = os.path.basename(parsed.path)
    if not fname:
        fname = parsed.netloc.replace(".", "_")
    return fname

def download(url: str, outdir: Path) -> tuple[str, str, str]:
    """Return (url, status, path_or_error)."""
    norm_url = normalize_url(url)
    fname = filename_from_url(norm_url)
    outpath = outdir / fname

    if outpath.exists():
        return (url, "cached", str(outpath))

    for attempt in range(1, RETRIES + 1):
        try:
            r = requests.get(norm_url, stream=True, timeout=TIMEOUT)
            if r.status_code == 200:
                outdir.mkdir(parents=True, exist_ok=True)
                with open(outpath, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
                return (url, "ok", str(outpath))
            else:
                status = f"http {r.status_code}"
        except Exception as e:
            status = f"error: {e}"
        time.sleep(2)
    return (url, status, "")

def load_csv_urls(csv_path: Path, col: str) -> set[str]:
    urls = set()
    if csv_path.exists():
        with open(csv_path, newline="", encoding="utf-8") as f:
            r = csv.DictReader(f)
            for row in r:
                if col in row and row[col]:
                    urls.add(row[col])
    return urls

def load_urls_from_dir(folder: Path) -> list[str]:
    urls = set()
    for txt in folder.glob("*.txt"):
        with open(txt, encoding="utf-8", errors="ignore") as f:
            for line in f:
                urls.update(URL_REGEX.findall(line))
    return sorted(urls)

def append_row(csv_path: Path, header: list[str], row: list[str]):
    exists = csv_path.exists()
    with open(csv_path, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if not exists:
            w.writerow(header)
        w.writerow(row)

# ---------- main ----------
def main():
    ap = argparse.ArgumentParser(description="Fetch NANOMEDICINE datasets")
    ap.add_argument("--resume", action="store_true", help="retry only failed URLs")
    ap.add_argument("--workers", type=int, default=4, help="number of parallel downloads")
    args = ap.parse_args()

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # Build list of URLs
    if args.resume:
        urls = load_csv_urls(FAILED_FILE, "url")
        if not urls:
            print("✅ No failed URLs to resume.")
            return
        print(f"Resuming: {len(urls)} failed URLs from {FAILED_FILE}")
    else:
        if not URL_DIR.exists():
            raise FileNotFoundError(f"URL folder not found: {URL_DIR}")
        urls = set(load_urls_from_dir(URL_DIR))
        print(f"Found {len(urls)} unique URLs in {URL_DIR}")

    # Skip already succeeded ones
    already_ok = load_csv_urls(INDEX_FILE, "url")
    urls = [u for u in urls if u not in already_ok]
    if not urls:
        print("✅ Nothing new to fetch (all URLs already processed).")
        return

    print(f"Processing {len(urls)} URLs with {args.workers} workers")

    counts = {"ok": 0, "cached": 0, "fail": 0}

    # Parallel fetch with progress bar
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = [ex.submit(download, url, OUT_DIR) for url in urls]
        for future in tqdm(as_completed(futures), total=len(futures), unit="file", desc="Downloading"):
            url, status, path = future.result()
            ts = time.strftime("%Y-%m-%d %H:%M:%S")
            if status in ("ok", "cached"):
                append_row(INDEX_FILE, ["url", "status", "path", "timestamp"], [url, status, path, ts])
                counts[status] += 1
            else:
                append_row(FAILED_FILE, ["url", "error", "timestamp"], [url, status, ts])
                counts["fail"] += 1

    # Final summary
    print("\n=== Summary ===")
    print(f"✅ OK      : {counts['ok']}")
    print(f"🟡 Cached  : {counts['cached']}")
    print(f"❌ Failed  : {counts['fail']}")
    print(f"Logs: {INDEX_FILE}, {FAILED_FILE}")

if __name__ == "__main__":
    main()
