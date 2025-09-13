# File: scripts/fetch_brca_new.py
# Purpose: Clean+merge many URL manifests (txt/csv/tsv) from any folder depth, normalize bare/DOI links,
#          group mirrors, optionally probe, and download best source. Writes a resumable index.
# Usage (PowerShell):
#   python scripts\fetch_brca_new.py --url-dir data\00_sources --out-root data\01_raw\SOURCES --workers 8 --probe-first --ftp-list --ftp-max 50
#   # Debug the manifests without downloading:
#   python scripts\fetch_brca_new.py --url-dir data\00_sources --scan-only --verbose-scan

import os, sys, re, csv, json, time, argparse, threading, queue, datetime, signal
from urllib.parse import urlsplit
from urllib.request import Request, urlopen
from ftplib import FTP, error_perm

# ---------- Import resilient single-file downloader (resumable + atomic) ----------
def _import_download_one():
    try:
        from scripts.download_one import download_one
        return download_one
    except Exception:
        import importlib.util
        here = os.path.dirname(os.path.abspath(__file__))
        candidate = os.path.join(here, "download_one.py")
        if not os.path.exists(candidate):
            raise SystemExit("[ERROR] Cannot import scripts/download_one.py. Ensure it exists.")
        spec = importlib.util.spec_from_file_location("download_one", candidate)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod.download_one

download_one = _import_download_one()

# ---------- Constants / regexes ----------
URL_RX_SCHEMED = re.compile(r'(?i)\b(?:https?|ftp)://[^\s<>"\'\)\]]+')
CSV_EXT = {".csv", ".tsv", ".tab"}
TXT_EXT = {".txt"}

RX_GSE = re.compile(r'\b(GSE\d+|GSM\d+|GDS\d+)\b', re.I)
RX_PXD = re.compile(r'\b(PXD\d{6,})\b', re.I)
RX_DOI = re.compile(r'\b(?:doi:)?10\.\d{4,9}/[^\s]+', re.I)  # accept with/without "doi:"
RX_GDC = re.compile(r'\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b', re.I)

UA = "bc-project-fetcher/1.1 (+win; py3.11)"

# ---------- Helpers ----------
def _p(msg):  # always flush for live feedback
    print(msg, flush=True)

def _is_google_redirect(u: str) -> bool:
    host = urlsplit(u).netloc.lower()
    return host.startswith("www.google.") or host == "google.com"

def _requires_gdc_auth(u: str) -> bool:
    host = urlsplit(u).netloc.lower()
    return ("gdc.cancer.gov" in host) or ("gdc-hub.s3" in host) or ("api.gdc.cancer.gov" in host)

def _infer_key(url, note=""):
    for rx in (RX_GSE, RX_PXD, RX_GDC, RX_DOI):
        m = rx.search(url) or rx.search(note or "")
        if m: return m.group(0).upper()
    parts = urlsplit(url)
    base = os.path.basename(parts.path) or parts.netloc or "RESOURCE"
    return (parts.netloc + "::" + base).upper()

def _mirror_sort_key(u):
    sch = urlsplit(u).scheme.lower()
    return {"https": 0, "http": 1, "ftp": 2}.get(sch, 3)

def _normalize_candidate(u: str) -> str:
    """Make copy/paste-y tokens into fetchable URLs (bare domains, ftp., doi:, brackets, spaces)."""
    s = (u or "").strip()
    if not s: return ""
    s = s.strip("<> \t").rstrip("),.;]")           # trim common wrappers
    s = re.sub(r"\s+", "", s)                      # kill stray spaces (e.g., 'http ://')
    if s.lower().startswith("doi:"):               # doi:10.x → https://doi.org/10.x
        return "https://doi.org/" + s[4:]
    if s.lower().startswith("10."):                # bare DOI
        return "https://doi.org/" + s
    if s.startswith("www."):                       # bare www → https
        return "https://" + s
    if s.startswith("ftp."):                       # bare ftp. → ftp://
        return "ftp://" + s
    # bare domain/path like ncbi.nlm.nih.gov/geo/… → https://…
    if re.match(r"^[A-Za-z0-9.-]+\.[A-Za-z]{2,15}/", s):
        return "https://" + s
    return s

def _extract_urls_from_text(line: str):
    """Yield normalized URLs from a single line, handling schemed + bare + DOI."""
    out = []
    # 1) obvious schemed URLs
    for u in URL_RX_SCHEMED.findall(line):
        if not _is_google_redirect(u):
            out.append(u)
    # 2) DOI or bare tokens (fallback)
    # split on whitespace and punctuation, test each token
    for tok in re.split(r"[\s,;|]+", line):
        if not tok or "://" in tok: continue
        m = RX_DOI.search(tok)
        if m:
            out.append(_normalize_candidate(m.group(0)))
            continue
        cand = _normalize_candidate(tok)
        if cand.startswith(("http://","https://","ftp://")) and not _is_google_redirect(cand):
            out.append(cand)
    # dedupe in-order
    seen, out2 = set(), []
    for u in out:
        if u not in seen:
            seen.add(u); out2.append(u)
    return out2

def _parse_manifest_file(path, verbose=False):
    """Extract (url, note, source_file) from .txt/.csv/.tsv."""
    urls = []
    ext = os.path.splitext(path.lower())[1]
    try:
        if ext in TXT_EXT:
            with open(path, "r", encoding="utf-8", errors="ignore") as f:
                for raw in f:
                    line = raw.strip()
                    if not line or line.startswith("#"): continue
                    note = ""
                    if "#" in line:
                        url_part, note = line.split("#", 1)
                        line, note = url_part.strip(), note.strip()
                    for u in _extract_urls_from_text(line):
                        urls.append((u, note, os.path.basename(path)))
        else:
            delim = "\t" if ext in {".tsv", ".tab"} else ","
            with open(path, "r", encoding="utf-8", errors="ignore", newline="") as f:
                rd = csv.reader(f, delimiter=delim)
                header = None
                for i, row in enumerate(rd, 1):
                    if i == 1:
                        header = [c.strip().lower() for c in row] if row else []
                    if header and "url" in header:
                        idx = header.index("url")
                        if idx < len(row):
                            cell = (row[idx] or "").strip()
                            for u in _extract_urls_from_text(cell):
                                urls.append((u, "", os.path.basename(path)))
                    else:
                        for cell in row:
                            for u in _extract_urls_from_text(cell or ""):
                                urls.append((u, "", os.path.basename(path)))
    except Exception as e:
        if verbose: _p(f"[SCAN-WARN] {path}: {e}")
    return urls

def _load_all_urls(url_dir, verbose=False):
    files_scanned = 0
    raw = []
    for dp, _, files in os.walk(url_dir):
        for f in files:
            if f.lower().endswith(tuple(TXT_EXT | CSV_EXT)):
                files_scanned += 1
                p = os.path.join(dp, f)
                urls = _parse_manifest_file(p, verbose=verbose)
                raw.extend(urls)
                if verbose:
                    _p(f"[SCAN] {p} → {len(urls)} urls")
    # de-dupe by URL
    seen, cleaned = set(), []
    for u, note, src in raw:
        if u not in seen:
            seen.add(u); cleaned.append((u, note, src))
    _p(f"[INFO] Scanned files: {files_scanned} | Raw URLs: {len(raw)} | Unique URLs: {len(cleaned)}")
    return cleaned

# ---------- Probing ----------
def _probe_http(u, timeout=30):
    for method in ("HEAD", "GET"):
        try:
            req = Request(u, method=method, headers={"User-Agent": UA})
            if method == "GET":
                req.add_header("Range", "bytes=0-0")
            with urlopen(req, timeout=timeout) as resp:
                code = getattr(resp, "status", 200)
                if 200 <= code < 400:
                    clen = resp.getheader("Content-Length")
                    size = int(clen) if (clen and clen.isdigit()) else None
                    return True, size
        except Exception:
            continue
    return False, None

def _probe_ftp(u):
    p = urlsplit(u); path = p.path or "/"
    is_dir = path.endswith("/")
    return (not is_dir), is_dir

# ---------- Hardened FTP expansion (NLST → MLSD → LIST, timeout, passive/active) ----------
def _ftp_expand_dir(u, max_files=100, passive=True, list_timeout=25):
    p = urlsplit(u)
    host = p.hostname; port = p.port or 21
    user = p.username or "anonymous"; pwd = p.password or "anonymous@"
    dirpath = p.path or "/"
    if not dirpath.endswith("/"):  # already a file
        return [u]

    start = time.time()
    try:
        with FTP() as ftp:
            ftp.connect(host, port, timeout=list_timeout)
            ftp.set_pasv(passive)
            ftp.login(user, pwd)
            for seg in dirpath.strip("/").split("/"):
                if not seg: continue
                if time.time() - start > list_timeout: return [u]
                ftp.cwd(seg)

            names = []
            try:
                names = ftp.nlst()
            except Exception:
                names = []

            if not names or names == ['.','..']:
                mls_files = []
                try:
                    for name, facts in ftp.mlsd():
                        if facts.get('type') == 'file':
                            mls_files.append(name)
                except Exception:
                    try:
                        tmp = []
                        def _mlsd_cb(line):
                            parts = line.split(";")
                            if parts and "type=file" in parts[0]:
                                tmp.append(line.split()[-1])
                        ftp.retrlines("MLSD", _mlsd_cb)
                        if tmp: mls_files = tmp
                    except Exception:
                        pass
                if mls_files: names = mls_files

            if not names:
                lst = []
                def _list_cb(line):
                    if not line: return
                    if line[0].lower() != 'd':
                        parts = line.split(maxsplit=8)
                        if parts: lst.append(parts[-1])
                try:
                    ftp.retrlines("LIST", _list_cb)
                except Exception:
                    pass
                if lst: names = lst

            out = []
            for name in names[:max_files]:
                if name in (".",".."): continue
                if time.time() - start > list_timeout:
                    return [f"ftp://{host}{dirpath}{n}" for n in out] or [u]
                is_file = True
                try:
                    ftp.size(name)
                except error_perm:
                    is_file = False
                if is_file: out.append(name)

            return [f"ftp://{host}{dirpath}{n}" for n in out] or [u]
    except Exception:
        return [u]

# ---------- Index writer (thread-safe) ----------
class IndexWriter:
    def __init__(self, csv_path, jsonl_path=None):
        self.csv_path = csv_path
        self.jsonl_path = jsonl_path
        self.lock = threading.Lock()
        os.makedirs(os.path.dirname(csv_path), exist_ok=True)
        if not os.path.exists(csv_path) or os.path.getsize(csv_path) == 0:
            with open(csv_path, "w", encoding="utf-8", newline="") as f:
                f.write("timestamp,key,dataset,url,mirror_rank,status,local_path,size,source_file,error\n")
        if jsonl_path:
            os.makedirs(os.path.dirname(jsonl_path), exist_ok=True)

    def _norm(self, p):
        return (p or "").replace("\\", "/")

    def append(self, row: dict):
        line = "{ts},{key},{ds},{url},{rk},{st},{lp},{sz},{src},{err}\n".format(
            ts=row.get("timestamp",""),
            key=row.get("key",""),
            ds=row.get("dataset",""),
            url=row.get("url",""),
            rk=row.get("mirror_rank",0),
            st=row.get("status",""),
            lp=self._norm(row.get("local_path","")),
            sz=row.get("size",0),
            src=row.get("source_file",""),
            err=(row.get("error","") or "").replace("\n"," ").replace(",",";")
        )
        for i in range(5):
            try:
                with self.lock:
                    with open(self.csv_path, "a", encoding="utf-8", newline="") as f:
                        f.write(line)
                break
            except OSError:
                time.sleep(0.2*(i+1))
        if self.jsonl_path:
            for i in range(5):
                try:
                    with self.lock:
                        with open(self.jsonl_path, "a", encoding="utf-8") as f:
                            f.write(json.dumps(row, ensure_ascii=False) + "\n")
                    break
                except OSError:
                    time.sleep(0.2*(i+1))

def _load_completed(csv_path):
    done = set()
    if not os.path.exists(csv_path): return done
    try:
        with open(csv_path, "r", encoding="utf-8", errors="ignore") as f:
            rd = csv.DictReader(f)
            for r in rd:
                if (r.get("status") or "").upper() == "OK":
                    k = (r.get("key") or "").strip()
                    if k: done.add(k)
    except Exception:
        pass
    return done

# ---------- Main ----------
def main():
    ap = argparse.ArgumentParser(description="Multi-manifest fetcher with mirrors, probing, resume, and FTP expansion.")
    ap.add_argument("--url-dir", default=os.path.join("data","00_urls","brca"), help="Folder containing .txt/.csv/.tsv manifests (recurses)")
    ap.add_argument("--out-root", default=os.path.join("data","01_raw","BRCA"), help="Download root directory")
    ap.add_argument("--workers", type=int, default=8, help="Concurrent worker threads")
    ap.add_argument("--index", default=os.path.join("data","01_raw","BRCA","fetch_index.csv"), help="Index CSV path")
    ap.add_argument("--jsonl", default=None, help="Optional JSONL output path")
    ap.add_argument("--resume", action="store_true", help="Skip dataset keys already marked OK in index CSV")
    ap.add_argument("--probe-first", action="store_true", help="Probe mirrors before downloading to avoid slow failures")
    ap.add_argument("--ftp-list", action="store_true", help="Expand FTP directories (non-recursive)")
    ap.add_argument("--ftp-max", type=int, default=100, help="Max files to list per FTP directory with --ftp-list")
    ap.add_argument("--ftp-active", action="store_true", help="Use active FTP mode (default is passive)")
    ap.add_argument("--ftp-list-timeout", type=int, default=25, help="Timeout (sec) per FTP dir listing")
    ap.add_argument("--scan-only", action="store_true", help="Scan & normalize manifests, print stats, then exit")
    ap.add_argument("--verbose-scan", action="store_true", help="Log per-file url counts during scan")
    args = ap.parse_args()

    # Ctrl+C safety
    signal.signal(signal.SIGINT, lambda *_: (_p("[INT] KeyboardInterrupt received."), sys.exit(130)))

    # 1) Scan manifests (recursive) with normalization + stats
    rows = _load_all_urls(args.url_dir, verbose=args.verbose_scan)
    if not rows:
        _p(f"[INFO] No URLs recognized under {args.url_dir}. If you have bare links or DOIs, they should now be normalized; double-check file extensions (.txt/.csv/.tsv).")
        return
    if args.scan_only:
        _p("[DONE] Scan-only complete. Use --probe-first / --ftp-list to fetch.")
        return

    # 2) Optional FTP expansion (time-bounded)
    expanded = []
    for (u, note, src) in rows:
        scheme = urlsplit(u).scheme.lower()
        if args.ftp_list and scheme == "ftp":
            path = urlsplit(u).path or "/"
            if path.endswith("/"):
                _p(f"[FTP] Expanding dir (timeout {args.ftp_list_timeout}s): {u}")
                files = _ftp_expand_dir(
                    u,
                    max_files=args.ftp_max,
                    passive=(not args.ftp_active),
                    list_timeout=args.ftp_list_timeout
                )
                if files and files != [u]:
                    expanded.extend((fu, note, src) for fu in files)
                    _p(f"[FTP] Found {len(files)} files under {u}")
                else:
                    _p(f"[FTP] Expand timeout/empty for {u} (keeping dir URL)")
                    expanded.append((u, note, src))
                continue
        expanded.append((u, note, src))

    # 3) Group mirrors by inferred key
    groups = {}
    for u, note, src in expanded:
        k = _infer_key(u, note)
        groups.setdefault(k, []).append((u, note, src))

    # 4) Build worklist: prefer https > http > ftp; de-dupe mirrors per key
    work = []
    for key, lst in groups.items():
        uniq, seen = [], set()
        for u, note, src in sorted(lst, key=lambda t: _mirror_sort_key(t[0])):
            if u in seen: continue
            seen.add(u); uniq.append((u, note, src))
        work.append((key, uniq))
    work.sort(key=lambda t: t[0])

    writer = IndexWriter(args.index, args.jsonl)

    # 5) Resume
    if args.resume:
        done = _load_completed(args.index)
        work = [w for w in work if w[0] not in done]

    total = len(work)
    _p(f"[INFO] Dataset keys: {total}. Output → {args.out_root}")

    if total == 0:
        _p("[DONE] Nothing to fetch (all mirrors satisfied or only auth/dir links present).")
        return

    # 6) Workers
    q = queue.Queue()
    for item in work: q.put(item)

    progress = {"total": total, "completed": 0, "ok": 0, "err": 0}
    plock = threading.Lock()

    def reporter():
        while True:
            time.sleep(15)
            with plock:
                c, o, e = progress["completed"], progress["ok"], progress["err"]
            _p(f"[PROGRESS] {c}/{total} keys (OK={o} ERR={e})")
            if c >= total: return

    def http_probe_ok(u):
        ok, _ = _probe_http(u)
        return ok

    def worker():
        while True:
            try:
                key, mirrors = q.get_nowait()
            except queue.Empty:
                return
            ts = datetime.datetime.now().isoformat(timespec="seconds")
            dataset = key
            success = False
            for rank, (u, note, src) in enumerate(mirrors, start=1):
                if _requires_gdc_auth(u):
                    writer.append(dict(timestamp=ts,key=key,dataset=dataset,url=u,mirror_rank=rank,status="AUTH_REQUIRED",
                                       local_path="",size=0,source_file=src,error="GDC links require token/client"))
                    continue
                if args.probe_first:
                    sch = urlsplit(u).scheme.lower()
                    if sch in ("http","https"):
                        if not http_probe_ok(u):
                            writer.append(dict(timestamp=ts,key=key,dataset=dataset,url=u,mirror_rank=rank,status="PROBE_FAIL",
                                               local_path="",size=0,source_file=src,error="HTTP probe failed"))
                            continue
                    elif sch == "ftp":
                        is_file, is_dir = _probe_ftp(u)
                        if is_dir and not args.ftp_list:
                            writer.append(dict(timestamp=ts,key=key,dataset=dataset,url=u,mirror_rank=rank,status="SKIP_DIR",
                                               local_path="",size=0,source_file=src,error="FTP dir; use --ftp-list"))
                            continue
                try:
                    path = download_one(u, args.out_root, None)
                    try: size = os.path.getsize(path)
                    except OSError: size = 0
                    writer.append(dict(timestamp=ts,key=key,dataset=dataset,url=u,mirror_rank=rank,status="OK",
                                       local_path=path,size=size,source_file=src,error=""))
                    for r2, (u2, note2, src2) in enumerate(mirrors[rank:], start=rank+1):
                        writer.append(dict(timestamp=ts,key=key,dataset=dataset,url=u2,mirror_rank=r2,status="MIRROR_SKIP",
                                           local_path=path,size=size,source_file=src2,error="already satisfied"))
                    success = True
                    break
                except Exception as e:
                    msg = str(e).replace("\n"," ")[:600]
                    writer.append(dict(timestamp=ts,key=key,dataset=dataset,url=u,mirror_rank=rank,status="ERR",
                                       local_path="",size=0,source_file=src,error=msg))
                    continue
            with plock:
                progress["completed"] += 1
                (progress["ok"] if success else progress["err"])  # no-op for readability
                if success: progress["ok"] += 1
                else:       progress["err"] += 1
            _p(f"[OK] {key} (mirrors tried: {len(mirrors)})" if success else f"[ERR] {key} (no working mirrors)")
            q.task_done()

    rt = threading.Thread(target=reporter, daemon=True); rt.start()
    threads = []
    for _ in range(max(1, args.workers)):
        t = threading.Thread(target=worker, daemon=True)
        t.start(); threads.append(t)
    for t in threads: t.join()
    rt.join(timeout=1)

    _p(f"[DONE] Attempted {total} dataset keys. Index → {writer.csv_path}")
    if writer.jsonl_path:
        _p(f"[INFO] JSONL → {writer.jsonl_path}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        _p("\n[INT] Cancelled by user.")
        sys.exit(130)
