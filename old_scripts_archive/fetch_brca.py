# File: scripts/fetch_brca.py
# Purpose: BRCA (and future) fetcher — folders/files (.txt/.csv/.tsv/.jsonl), resolvers, concurrency, resume, failed export.
# Example:
#   .venv\Scripts\python.exe scripts\fetch_brca.py --in data\00_urls\brca --out data\01_raw\NEW --workers 10 --log data\download_brca.log --unpaywall-email you@domain.com --crawl-ftp

import os, sys, re, csv, json, time, argparse, traceback
from concurrent.futures import ThreadPoolExecutor, as_completed

# Optional progress UI; script still works without it.
try:
    from tqdm import tqdm
except Exception:
    tqdm = None

# Optional colors for Windows consoles
try:
    import colorama
    try:
        colorama.just_fix_windows_console()
    except Exception:
        try:
            colorama.init()
        except Exception:
            pass
except Exception:
    colorama = None

# Colored bar helper (safe on old tqdm)
BAR_COLOURS = ["cyan", "green", "yellow", "magenta", "blue", "white", "red"]
def _tqdm_coloured(*args, **kwargs):
    if tqdm is None:
        return None
    try:
        return tqdm(*args, **kwargs)
    except TypeError:
        kwargs.pop("colour", None)
        return tqdm(*args, **kwargs)

# Optional HTTP client for resolvers (DOI/PMID, etc.); core downloads use download_one.
try:
    import requests
except Exception:
    requests = None

# Import downloader (resumable .part + atomic replace)
sys.path.append(os.path.dirname(__file__))
try:
    from download_one import download_one  # def download_one(url, out_root, progress=cb)
except Exception as e:
    raise SystemExit("[ERROR] Cannot import scripts/download_one.py. Place it in scripts/ and try again.\n" + str(e))

# ---------------- Manifest loading ----------------
def _iter_manifest_files(path: str):
    """Yield all manifest files under a folder (recursively) or a single manifest path."""
    if os.path.isdir(path):
        for r, _, files in os.walk(path):
            for f in files:
                if f.lower().endswith((".txt", ".csv", ".tsv", ".jsonl")):
                    yield os.path.join(r, f)
    elif os.path.isfile(path) and path.lower().endswith((".txt", ".csv", ".tsv", ".jsonl")):
        yield path

def _read_txt(fp):
    with open(fp, encoding="utf-8") as fh:
        for raw in fh:
            s = raw.strip()
            if s and not s.startswith("#"):
                yield s

def _read_csv_tsv(fp, url_col):
    delim = "\t" if fp.lower().endswith(".tsv") else ","
    with open(fp, encoding="utf-8", newline="") as fh:
        rdr = csv.reader(fh, delimiter=delim)
        header = next(rdr, None)
        idx = None
        if header and not header[0].isdigit():
            # Choose URL column by name or index; fallback heuristics.
            if isinstance(url_col, str) and url_col in header:
                idx = header.index(url_col)
            elif isinstance(url_col, int) and 0 <= url_col < len(header):
                idx = url_col
            else:
                for i, name in enumerate(header):
                    if name.lower() in ("url","link","href","doi","pmid","pmcid","arxiv"):
                        idx = i; break
                if idx is None: idx = 0
            for row in rdr:
                if row and idx < len(row):
                    s = (row[idx] or "").strip()
                    if s: yield s
        else:
            # No header → use first column
            if header:
                s = (header[0] or "").strip()
                if s: yield s
            for row in rdr:
                if row:
                    s = (row[0] or "").strip()
                    if s: yield s

def _read_jsonl(fp):
    with open(fp, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line: continue
            try:
                obj = json.loads(line)
            except Exception:
                continue
            for k in ("url","href","link","download_url","pdf_url"):
                if k in obj and obj[k]:
                    yield str(obj[k]).strip(); break

def load_items(path: str, url_col):
    """Load tokens (URLs/DOIs/IDs) from .txt/.csv/.tsv/.jsonl across a folder or a file; stable de-dupe."""
    items = []
    for fp in _iter_manifest_files(path):
        ext = fp.lower().rsplit(".", 1)[-1]
        if ext == "txt":           items += list(_read_txt(fp))
        elif ext in ("csv","tsv"): items += list(_read_csv_tsv(fp, url_col))
        elif ext == "jsonl":       items += list(_read_jsonl(fp))
    seen, out = set(), []
    for x in items:
        if x not in seen:
            seen.add(x); out.append(x)
    return out

# ---------------- Resolvers (tokens → concrete URLs) ----------------
_DOI    = re.compile(r"^(?:doi:)?(10\.\d{4,9}/\S+)$", re.I)
_ARXIV  = re.compile(r"^(?:arxiv:)?(\d{4}\.\d{4,5}(?:v\d+)?)$", re.I)
_PMCID  = re.compile(r"^(?:pmcid:)?(pmc\d+)$", re.I)
_PMID   = re.compile(r"^(?:pmid:)?(\d{5,9})$", re.I)
_FTPDIR = re.compile(r"^ftp://.*/$")

def resolve_targets(token: str, unpaywall_email: str | None, crawl_ftp: bool, ftp_exts: str | None):
    """Return a list of candidate URLs to try, from most to least direct."""
    s = token.strip()
    t = []

    # Direct URLs (HTTP/HTTPS/FTP)
    if s.lower().startswith(("http://","https://","ftp://")):
        if crawl_ftp and _FTPDIR.match(s):
            # Optional: expand FTP directory to file URLs
            try:
                from ftplib import FTP
                m = re.match(r"^ftp://([^/]+)(/.+)$", s, re.I)
                host, path = m.group(1), m.group(2)
                with FTP(host, timeout=30) as ftp:
                    ftp.login("anonymous","anonymous@")
                    for seg in path.strip("/").split("/"):
                        if seg: ftp.cwd(seg)
                    names = ftp.nlst()
                if ftp_exts and ftp_exts != "*":
                    allow = {x.strip().lower() for x in ftp_exts.split(",")}
                    names = [n for n in names if "." in n and n.rsplit(".",1)[-1].lower() in allow]
                for n in names:
                    if "." in n:
                        t.append(s + n if s.endswith("/") else s + "/" + n)
            except Exception:
                pass
        else:
            t.append(s)
        return t

    # DOI → Unpaywall (OA PDF/URL) → doi.org
    m = _DOI.match(s)
    if m:
        doi = m.group(1)
        if unpaywall_email and requests:
            try:
                r = requests.get(f"https://api.unpaywall.org/v2/{doi}",
                                 params={"email": unpaywall_email}, timeout=20)
                if r.ok:
                    J = r.json()
                    loc = (J.get("best_oa_location") or {}) or (J.get("oa_locations")[0] if J.get("oa_locations") else {})
                    for k in ("url_for_pdf","url"):
                        if loc.get(k): t.append(loc[k])
            except Exception:
                pass
        t.append("https://doi.org/" + doi)
        return t

    # arXiv → PDF then ABS
    m = _ARXIV.match(s)
    if m:
        aid = m.group(1)
        return [f"https://arxiv.org/pdf/{aid}.pdf", f"https://arxiv.org/abs/{aid}"]

    # PMCID → PMC PDF then landing
    m = _PMCID.match(s)
    if m:
        pmc = m.group(1).upper()
        return [f"https://www.ncbi.nlm.nih.gov/pmc/articles/{pmc}/pdf/",
                f"https://www.ncbi.nlm.nih.gov/pmc/articles/{pmc}/"]

    # PMID → try ELink to PMC → PubMed fallback
    m = _PMID.match(s)
    if m and requests:
        pmid = m.group(1)
        try:
            r = requests.get("https://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi",
                             params={"dbfrom":"pubmed","db":"pmc","id":pmid,"retmode":"json"}, timeout=20)
            if r.ok:
                J = r.json()
                ids = J.get("linksets",[{}])[0].get("linksetdbs",[{}])[0].get("links",[])
                if ids:
                    pmc = "PMC" + ids[0]
                    return [f"https://www.ncbi.nlm.nih.gov/pmc/articles/{pmc}/pdf/",
                            f"https://www.ncbi.nlm.nih.gov/pmc/articles/{pmc}/",
                            f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/"]
        except Exception:
            pass
        return [f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/"]

    # Unknown token → return as-is (may be a bare host/path handled by downloader)
    return [s]

# ---------------- Runner ----------------
def _load_done(log_path: str):
    done = set()
    if os.path.exists(log_path):
        with open(log_path, encoding="utf-8") as lf:
            for line in lf:
                if line.startswith("OK\t"):
                    try:
                        done.add(line.split("\t", 3)[1])
                    except Exception:
                        continue
    return done

def main(inp: str, out_root: str, workers: int, log_file: str,
         url_col, unpaywall_email, crawl_ftp: bool, ftp_exts: str):
    if not inp:
        raise SystemExit("[ERROR] --in/--urls/--url is required.")
    os.makedirs(out_root, exist_ok=True)
    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    if isinstance(url_col, str) and url_col.isdigit():
        url_col = int(url_col)

    items = load_items(inp, url_col)
    if not items:
        raise SystemExit(f"[ERROR] No URLs/IDs found under: {inp}")

    done_prev = _load_done(log_file)
    todo = [x for x in items if x not in done_prev]
    total, already = len(items), len(done_prev)
    if not todo:
        print(f"[INFO] All items already fetched ({already}/{total}).")
        return

    print(f"[INFO] New: {len(todo)}  |  Already OK: {already}  |  Total: {total}")
    if any((_DOI.match(x) or _PMID.match(x)) for x in todo) and not requests:
        print("[WARN] DOI/PMID resolvers disabled (install 'requests'): python -m pip install requests")

    overall = _tqdm_coloured(total=len(todo), desc="Files", unit="file", position=0, colour="cyan") if tqdm else None

    # Initialize counters BEFORE threadpool (fixes UnboundLocalError)
    ok = 0
    err = 0

    def task(item: str, pos: int):
        bar_colour = BAR_COLOURS[(max(1, pos) - 1) % len(BAR_COLOURS)]
        bar = (_tqdm_coloured(total=0, unit="B", unit_scale=True, desc=item[:40],
                              position=pos, leave=False, dynamic_ncols=True, colour=bar_colour)
               if tqdm else None)
        last_tick = time.time()

        def progress(done_b: int, total_b: int):
            nonlocal last_tick
            if bar:
                if total_b > 0 and (bar.total or 0) != total_b:
                    bar.total = total_b
                bar.n = done_b
                now = time.time()
                if now - last_tick >= 0.05:
                    bar.refresh(); last_tick = now

        try:
            last_tried = ""
            for cand in resolve_targets(item, unpaywall_email, crawl_ftp, ftp_exts):
                last_tried = cand
                try:
                    outp = download_one(cand, out_root, progress=progress)
                    return ("OK", item, cand, outp)
                except Exception:
                    continue
            return ("ERR", item, last_tried, "no candidate url succeeded")
        except Exception as e:
            return ("ERR", item, "", str(e))
        finally:
            if bar: bar.close()

    try:
        with ThreadPoolExecutor(max_workers=workers) as ex, open(log_file, "a", encoding="utf-8") as lf:
            futures = {ex.submit(task, it, (i % max(1, workers)) + 1): it for i, it in enumerate(todo)}
            for fut in as_completed(futures):
                status, item, used_url, msg = fut.result()
                lf.write(f"{status}\t{item}\t{used_url}\t{msg}\n"); lf.flush()
                if status == "OK": ok += 1
                else: err += 1
                if overall: overall.update(1)
                else: print(f"[INFO] {(ok+err)}/{len(todo)} done")
    except KeyboardInterrupt:
        print("\n[WARN] Interrupted. .part files are resumable; log flushed.")
    finally:
        if overall: overall.close()

    # Export failed items for retry/inspection
    if err:
        failed_path = os.path.splitext(log_file)[0] + "_failed.txt"
        try:
            with open(log_file, encoding="utf-8") as lf, open(failed_path, "w", encoding="utf-8") as ff:
                for line in lf:
                    if line.startswith("ERR\t"):
                        try:
                            token = line.split("\t", 2)[1]
                            ff.write(token + "\n")
                        except Exception:
                            continue
            print(f"[INFO] Failed items written to: {failed_path}")
        except Exception as e:
            print(f"[WARN] Could not write failed list: {e}")

    print(f"[SUMMARY] OK: {ok}  ERR: {err}  SKIPPED(prev OK): {already}  TOTAL(listed): {total}")
    if err:
        print("[NEXT] Re-run to retry only failures; OK entries are skipped.")

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="BRCA fetcher (folders/files; resolvers; concurrency; resume).")
    # Back-compat aliases so old docs/typos still work
    ap.add_argument("--in", "--urls", "--url", dest="inp", default=r"data\00_urls\brca",
                    help="Folder (recurses) or a single manifest file (.txt/.csv/.tsv/.jsonl)")
    ap.add_argument("--out", "--out-root", dest="out_root", default=r"data\01_raw\NEW",
                    help="Output root directory")
    ap.add_argument("-w", "--workers", dest="workers", type=int, default=5,
                    help="Parallel downloads (default=5)")
    ap.add_argument("--log", "--log-file", dest="log", default=r"data\download_brca.log",
                    help="Append-only log file")
    ap.add_argument("--url-col", dest="url_col", default=None,
                    help="CSV/TSV: header name or zero-based index of URL column")
    ap.add_argument("--unpaywall-email", "--unpaywall", dest="unpaywall_email", default=None,
                    help="Email for Unpaywall DOI resolver (optional)")
    ap.add_argument("--crawl-ftp", action="store_true",
                    help="If set, treat ftp://.../ (ending with /) as directory and fetch contained files")
    ap.add_argument("--ftp-exts", default="*",
                    help="Comma list of extensions kept when crawling FTP (e.g., gz,zip,txt). '*' for all.")
    a = ap.parse_args()
    try:
        main(a.inp, a.out_root, a.workers, a.log, a.url_col, a.unpaywall_email, a.crawl_ftp, a.ftp_exts)
    except Exception as e:
        print(f"[FATAL] {e}")
        traceback.print_exc()
        sys.exit(1)
