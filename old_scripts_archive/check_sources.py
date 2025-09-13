#!/usr/bin/env python3
# check_sources.py — recursively check all links in 00_sources
import os, re, argparse, requests
from concurrent.futures import ThreadPoolExecutor, as_completed

URL_RE = re.compile(r'(https?://\S+|ftp://\S+|10\.\d{4,9}/\S+)')

def check_url(url: str, timeout=10):
    if url.startswith("10."):  # DOI shorthand
        url = f"https://doi.org/{url}"
    try:
        r = requests.head(url, allow_redirects=True, timeout=timeout)
        return url, r.status_code < 400
    except Exception:
        return url, False

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--src", default="data/00_sources", help="Folder to scan for .txt URLs")
    p.add_argument("--outdir", default="data/02_interim/logs", help="Where to save results")
    p.add_argument("--workers", type=int, default=30)
    args = p.parse_args()

    os.makedirs(args.outdir, exist_ok=True)

    # collect links
    links = []
    for root, _, files in os.walk(args.src):
        for fn in files:
            if fn.lower().endswith(".txt"):
                with open(os.path.join(root, fn), encoding="utf-8", errors="ignore") as f:
                    for line in f:
                        for m in URL_RE.findall(line):
                            links.append(m.strip())

    print(f"[INFO] Collected {len(links)} links from {args.src}")

    results = {"working": [], "dead": []}
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = [ex.submit(check_url, u) for u in links]
        for i, fut in enumerate(as_completed(futs), 1):
            url, ok = fut.result()
            results["working" if ok else "dead"].append(url)
            if i % 200 == 0 or i == len(links):
                print(f"[{i}/{len(links)}] checked")

    for key in results:
        outpath = os.path.join(args.outdir, f"{key}_links.txt")
        with open(outpath, "w", encoding="utf-8") as f:
            f.write("\n".join(sorted(set(results[key]))))
        print(f"[SAVED] {key}: {len(results[key])} → {outpath}")

if __name__ == "__main__":
    main()
