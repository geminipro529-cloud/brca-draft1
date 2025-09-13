#!/usr/bin/env python3
# fetch_recursive.py — robust recursive fetcher for HTTP directory listings + FTP trees
# - Recurses "folders within folders" under each seed URL's path (HTTP/HTTPS, FTP)
# - Concurrent downloads, colorful global progress bar
# - Resume via visited/done ledgers, deduped queue
# - Windows-long-path safe; atomic writes; minimal deps

import os, sys, re, json, time, threading, hashlib, shutil, tempfile
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse, urljoin, unquote
from urllib.request import Request, urlopen
from ftplib import FTP

# ---------- CLI ----------
OUT_DIR   = os.path.join("data","01_raw","RECURSIVE")
SEEDS     = os.path.join("data","00_urls")  # file or folder; any *.txt/*.lst/*.urls parsed for URLs
WORKERS   = 12
MAX_DEPTH = 10           # recursion depth per seed (directories)
INCLUDE_EXT = None       # e.g. ".txt,.csv,.tsv,.gz"; None = all
VERBOSE   = False

a = sys.argv[1:]
while a:
    x = a.pop(0)
    if x in ("-h","--help"):
        print("Usage: python scripts/fetch_recursive.py "
              "[--seeds FILE|DIR] [--out-dir DIR] [--workers N] "
              "[--max-depth N] [--include-ext .csv,.tsv] [--verbose]")
        sys.exit(0)
    elif x=="--seeds": SEEDS = a.pop(0)
    elif x=="--out-dir": OUT_DIR = a.pop(0)
    elif x=="--workers": WORKERS = max(1, int(a.pop(0)))
    elif x=="--max-depth": MAX_DEPTH = max(0, int(a.pop(0)))
    elif x=="--include-ext": INCLUDE_EXT = set(s.strip().lower() for s in a.pop(0).split(",") if s.strip())
    elif x=="--verbose": VERBOSE = True

# ---------- Globals ----------
UA = {"User-Agent":"recursive-fetch/1.0"}
COLOR = (os.name!="nt") or bool(os.environ.get("WT_SESSION") or os.environ.get("ANSICON") or os.environ.get("TERM_PROGRAM")=="vscode")
def _c(s, code): return f"\x1b[{code}m{s}\x1b[0m" if COLOR else s
def sha1(s:str)->str: return hashlib.sha1(s.encode("utf-8")).hexdigest()

def safe_join(base,*parts):
    p = os.path.join(base,*parts)
    return ("\\\\?\\"+os.path.abspath(p)) if os.name=="nt" and not p.startswith("\\\\?\\") else p

def atomic_move(src,dst):
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    for i in range(6):
        try:
            if os.path.exists(dst): os.replace(src,dst)
            else: shutil.move(src,dst)
            return
        except PermissionError:
            time.sleep(0.4*(i+1))
    raise

# ---------- Trust (nanomedicine-friendly defaults, harmless if unused) ----------
TRUST_DOMAINS = {
    "ncbi.nlm.nih.gov":5,"ebi.ac.uk":5,"pride.ebi.ac.uk":5,"proteomexchange.org":5,"biostudies.ebi.ac.uk":5,
    "metabolomicsworkbench.org":5,"reactome.org":5,"wikipathways.org":5,"clinicaltrials.gov":5,"cancer.gov":5,
    "cananolab.cancer.gov":5,"europepmc.org":5,"openalex.org":5,"api.unpaywall.org":5,"api.datacite.org":5,
    "zenodo.org":4,"figshare.com":4,"datadryad.org":4,"osf.io":4,"massive.ucsd.edu":4,"jpostdb.org":4,"uniprot.org":4,"kegg.jp":4,
    "github.com":3,"raw.githubusercontent.com":3,"gitlab.com":3,"arxiv.org":3,"biorxiv.org":3,"medrxiv.org":3,
    "nanohub.org":3,"nature.com":3,"pnas.org":3,"cell.com":3,"pubs.acs.org":3,"rsc.org":3,"sciencedirect.com":3,"link.springer.com":3,
    "researchgate.net":1,"academia.edu":1,"medium.com":1,"quora.com":1,"reddit.com":1,
    "pinterest.com":0,"facebook.com":0,"twitter.com":0,"t.co":0,"bit.ly":0,"goo.gl":0,"tinyurl.com":0,
}
def _host(h): return (h or "").lower().split(":")[0]
def domain_score(url:str)->int:
    h = _host(urlparse(url).hostname)
    parts = h.split(".")
    for i in range(len(parts)-2, -1, -1):
        key = ".".join(parts[i:])
        if key in TRUST_DOMAINS: return TRUST_DOMAINS[key]
    return 1

# ---------- Progress ----------
prog = {"queued":0,"done":0,"active":0,"fail":0}
prog_lock = threading.Lock()
stop_evt = threading.Event()

def progress_bar():
    width=36; t0=time.time()
    while not stop_evt.is_set():
        with prog_lock:
            q=prog["queued"]; d=prog["done"]; a=prog["active"]; f=prog["fail"]
        tot=max(1,q); pct=100*d/tot; filled=int(width*d/tot)
        bar="█"*filled+"░"*(width-filled); rate=d/max(1e-9,time.time()-t0)
        sys.stdout.write(f"\r{_c('[','36')}{_c(bar,'32')}{_c(']','36')} {pct:6.2f}%  "
                         f"items {d}/{q}  active {a}  fail {f}  {rate:.2f}/s")
        sys.stdout.flush(); time.sleep(0.2)
    print("")

# ---------- URL utils ----------
A_HREF_RE = re.compile(r'href=["\']([^"\']+)["\']', re.I)
def is_http(u): return u.lower().startswith(("http://","https://"))
def is_ftp(u):  return u.lower().startswith("ftp://")
def looks_dir_url(u): return u.endswith("/")
def path_within_scope(url:str, scope_prefix:str)->bool:
    p = urlparse(url).path or "/"
    return p.startswith(scope_prefix)

def normalize_link(page_url:str, link:str)->str|None:
    if not link or link.startswith("#"): return None
    if link.lower().startswith("mailto:"): return None
    absu = urljoin(page_url, link)
    pr = urlparse(absu)
    if not pr.scheme in ("http","https"): return None
    # strip fragments
    if pr.fragment:
        absu = absu.split("#",1)[0]
    return absu

def infer_filename_from_url(u:str)->str:
    pr = urlparse(u); path = unquote(pr.path)
    if not path or path.endswith("/"): return "index.html"
    name = os.path.basename(path)
    return name or "download.bin"

def local_path_for(u:str)->str:
    pr = urlparse(u); path = unquote(pr.path).lstrip("/")
    if not path or path.endswith("/"):
        path = os.path.join(path, "index.html")
    return os.path.join(pr.scheme, pr.netloc, path)

# ---------- HTTP ----------
def http_get(url:str, timeout=45):
    req = Request(url, headers=UA)
    return urlopen(req, timeout=timeout)

def http_fetch_file(url:str, out_dir:str)->str:
    rel = local_path_for(url)
    dst = safe_join(out_dir, rel)
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    fd,tmp = tempfile.mkstemp(prefix=".part-", dir=os.path.dirname(dst)); os.close(fd)
    written=0; total=None
    try:
        with http_get(url) as r, open(tmp,"wb") as f:
            total = int(r.headers.get("Content-Length") or "0") or None
            while True:
                b = r.read(1024*1024)
                if not b: break
                f.write(b)
                written += len(b)
        atomic_move(tmp, dst)
        return "ok"
    except Exception as e:
        try: os.remove(tmp)
        except: pass
        return f"fail:{e.__class__.__name__}"

def http_list_dir(url:str, scope_prefix:str)->tuple[set[str],set[str]]:
    """
    Return (files, dirs) URLs under scope, extracted from an index/HTML listing.
    Only keeps same-scheme+host and same path prefix; ignores parent '..'.
    """
    files, dirs = set(), set()
    try:
        with http_get(url) as r:
            ctype = (r.headers.get("Content-Type") or "").lower()
            body  = r.read(1024*1024*4).decode("utf-8","ignore")  # 4MB cap for index
    except Exception:
        return files, dirs

    if "text/html" not in ctype and not looks_dir_url(url):
        # not an HTML listing; treat as file-URL
        files.add(url); return files, dirs

    # Parse links
    base = url
    for m in A_HREF_RE.finditer(body):
        link = normalize_link(base, m.group(1))
        if not link: continue
        pr = urlparse(link)
        if (pr.scheme, pr.netloc) != (urlparse(base).scheme, urlparse(base).netloc): 
            continue  # keep within host
        if not path_within_scope(link, scope_prefix): 
            continue  # keep within seed path
        # Avoid parent folders
        if link.endswith("../") or link.endswith("/.."): 
            continue
        # Classify
        if link.endswith("/"):
            dirs.add(link)
        else:
            files.add(link)
    # If the current URL looks like file (no trailing slash) but is a listing page, we already harvested children.
    return files, dirs

# ---------- FTP ----------
def ftp_walk_and_fetch(seed:str, out_dir:str)->str:
    pr = urlparse(seed); host, path = pr.hostname, pr.path or "/"
    ok = fail = 0
    try:
        ftp = FTP(host, timeout=60); ftp.login()
        def walk(cur, rel=""):
            nonlocal ok, fail
            items=[]
            try: ftp.retrlines(f"LIST {cur}", items.append)
            except Exception: 
                # maybe it's a file
                relc = rel.lstrip("/")
                dst = safe_join(out_dir, pr.scheme, pr.netloc, relc or infer_filename_from_url(seed))
                os.makedirs(os.path.dirname(dst), exist_ok=True)
                fd,tmp = tempfile.mkstemp(prefix=".part-", dir=os.path.dirname(dst)); os.close(fd)
                try:
                    with open(tmp,"wb") as f: ftp.retrbinary("RETR "+cur, f.write)
                    atomic_move(tmp,dst); ok += 1
                except Exception:
                    try: os.remove(tmp)
                    except: pass
                    fail += 1
                return
            for line in items:
                cols = line.split(maxsplit=8)
                if len(cols)<9: continue
                name=cols[8]; remote = f"{cur.rstrip('/')}/{name}"
                relc = os.path.join(rel, name)
                if line.startswith("d"):
                    walk(remote, relc)
                else:
                    dst = safe_join(out_dir, pr.scheme, pr.netloc, relc.lstrip("/"))
                    os.makedirs(os.path.dirname(dst), exist_ok=True)
                    fd,tmp = tempfile.mkstemp(prefix=".part-", dir=os.path.dirname(dst)); os.close(fd)
                    try:
                        with open(tmp,"wb") as f: ftp.retrbinary("RETR "+remote, f.write)
                        atomic_move(tmp, dst); ok += 1
                    except Exception:
                        try: os.remove(tmp)
                        except: pass
                        fail += 1
        walk(path, path.lstrip("/"))
        try: ftp.quit()
        except: pass
    except Exception:
        return "fail:FTP"
    return "ok" if ok and not fail else ("partial" if ok else "fail")

# ---------- Seed handling ----------
URL_RE = re.compile(r'\b(?:https?|ftp)://[^\s"\'<>()]+', re.I)

def iter_seed_files(p:str):
    if os.path.isdir(p):
        for r,_,fs in os.walk(p):
            for fn in fs:
                if os.path.splitext(fn.lower())[1] in (".txt",".lst",".urls",".csv"):
                    yield os.path.join(r,fn)
    else:
        yield p

def load_seeds(p:str)->list[str]:
    seeds=[]
    for fp in iter_seed_files(p):
        try:
            with open(fp,"r",encoding="utf-8",errors="ignore") as f:
                for line in f:
                    for m in URL_RE.finditer(line):
                        seeds.append(m.group(0).strip())
        except Exception:
            continue
    # de-dupe
    seen=set(); out=[]
    for s in seeds:
        if s not in seen: seen.add(s); out.append(s)
    return out

# ---------- Persistence / Resume ----------
PROG_DIR   = os.path.join(OUT_DIR, "_progress")
VISITED_TXT= os.path.join(PROG_DIR, "visited.txt")
DONE_TXT   = os.path.join(PROG_DIR, "done.txt")
LOG_PATH   = os.path.join(OUT_DIR, "download.log")

def load_set(path)->set[str]:
    s=set()
    try:
        with open(path,"r",encoding="utf-8") as f:
            for line in f:
                line=line.strip()
                if line: s.add(line)
    except FileNotFoundError:
        pass
    return s

def append_line(path, text):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path,"a",encoding="utf-8") as f: f.write(text+"\n")

# ---------- Worker ----------
visited_lock = threading.Lock()
queue_lock   = threading.Lock()

def file_allowed(url:str)->bool:
    if not INCLUDE_EXT: return True
    name = infer_filename_from_url(url).lower()
    _,ext = os.path.splitext(name)
    return (ext in INCLUDE_EXT) or (name in INCLUDE_EXT)

def process_http(target:str, scope_prefix:str, depth:int, out_dir:str, done:set, q:deque, logf):
    """
    If 'target' is a directory listing → enqueue children.
    If it's a file → download.
    """
    # Directory vs file decision:
    # 1) If target endswith '/' → treat as dir.
    # 2) Else try listing; if not HTML/listing, treat as file.
    files=set(); dirs=set()
    if looks_dir_url(target):
        f, d = http_list_dir(target, scope_prefix); files |= f; dirs |= d
    else:
        # try listing anyway (some servers serve 'index?' without slash)
        f, d = http_list_dir(target, scope_prefix)
        if f or d:
            files |= f; dirs |= d
        else:
            # treat as file
            if file_allowed(target):
                st = http_fetch_file(target, out_dir)
                if st=="ok":
                    append_line(DONE_TXT, target); logf.write(f"OK\t{target}\n")
                else:
                    logf.write(f"FAIL\t{target}\t{st}\n")
                with prog_lock:
                    if st.startswith("fail"): prog["fail"] += 1
                    prog["done"] += 1
            return

    # enqueue files
    for u in sorted(files):
        if not file_allowed(u): continue
        if sha1(u) in done: continue
        rel = local_path_for(u)
        dst = safe_join(out_dir, rel)
        if os.path.exists(dst):  # already downloaded
            append_line(DONE_TXT, u)
            with prog_lock: prog["done"] += 1
            continue
        # schedule file download
        with queue_lock:
            q.append(("file", u, scope_prefix, depth, out_dir))

    # enqueue subdirs
    if depth < MAX_DEPTH:
        for durl in sorted(dirs):
            with queue_lock:
                q.append(("dir", durl, scope_prefix, depth+1, out_dir))

def download_http_file(url:str, out_dir:str, logf):
    st = http_fetch_file(url, out_dir)
    if st=="ok":
        append_line(DONE_TXT, url); logf.write(f"OK\t{url}\n")
    else:
        logf.write(f"FAIL\t{url}\t{st}\n")
        with prog_lock: prog["fail"] += 1
    with prog_lock: prog["done"] += 1

def process_item(item, done:set, q:deque, logf):
    kind, url, scope_prefix, depth, out_dir = item
    with prog_lock: prog["active"] += 1
    try:
        if is_ftp(url):
            # for an FTP root, fetch entire tree once
            st = ftp_walk_and_fetch(url, out_dir)
            if st.startswith("ok") or st=="partial":
                append_line(DONE_TXT, url); logf.write(f"OK\t{url}\n")
            else:
                logf.write(f"FAIL\t{url}\t{st}\n"); with_fail=True
            with prog_lock:
                prog["done"] += 1
                if st.startswith("fail"): prog["fail"] += 1
            return

        if is_http(url):
            # If this is a directory task:
            if kind=="dir":
                process_http(url, scope_prefix, depth, out_dir, done, q, logf)
                return
            # File task:
            if kind=="file":
                download_http_file(url, out_dir, logf)
                return

            # Seed task (unknown yet)
            if url.endswith("/"):
                process_http(url, scope_prefix, depth, out_dir, done, q, logf)
            else:
                # Try as listing, else as file
                files, dirs = http_list_dir(url, scope_prefix)
                if files or dirs:
                    # treat as directory page
                    for f in sorted(files):
                        if file_allowed(f) and sha1(f) not in done:
                            with queue_lock: q.append(("file", f, scope_prefix, depth, out_dir))
                    if depth < MAX_DEPTH:
                        for d in sorted(dirs):
                            with queue_lock: q.append(("dir", d, scope_prefix, depth+1, out_dir))
                else:
                    # straight file
                    download_http_file(url, out_dir, logf)
        else:
            # Unsupported scheme; mark done to avoid loops
            append_line(DONE_TXT, url); logf.write(f"SKIP\t{url}\n")
            with prog_lock: prog["done"] += 1
    finally:
        with prog_lock: prog["active"] -= 1

# ---------- Main ----------
def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    os.makedirs(PROG_DIR, exist_ok=True)

    # Load seeds
    seeds = load_seeds(SEEDS)
    if not seeds:
        print("No seeds found in", SEEDS, file=sys.stderr); sys.exit(1)

    # Load ledgers
    visited = load_set(VISITED_TXT)
    done    = set(sha1(u) for u in load_set(DONE_TXT))

    # Build initial queue (respect scope: same scheme+host, path prefix)
    q = deque()
    for s in seeds:
        try:
            pr = urlparse(s)
            if pr.scheme not in ("http","https","ftp"): 
                continue
            scope_prefix = pr.path if pr.path else "/"
            # Only queue if unseen
            h = sha1(s)
            if h not in done and h not in visited:
                with queue_lock:
                    q.append(("seed", s, scope_prefix, 0, OUT_DIR))
                append_line(VISITED_TXT, h)
        except Exception:
            continue

    with prog_lock: prog["queued"] = len(q)
    stop_evt.clear()
    bar = threading.Thread(target=progress_bar, daemon=True); bar.start()

    log_path = os.path.join(OUT_DIR,"download.log")
    try:
        with open(log_path, "a", encoding="utf-8") as logf:
            # Waves: we keep pulling from q while adding new items inside worker
            while True:
                batch=[]
                with queue_lock:
                    while q and len(batch) < max(2*WORKERS, 64):
                        batch.append(q.popleft())
                if not batch:
                    # no batch now; if queue is empty after workers flush, we finish
                    if prog["active"]==0:
                        break
                    time.sleep(0.2)
                    continue

                # bump queued count for new batch items (plus any children will adjust later)
                with prog_lock: prog["queued"] += len(batch)

                with ThreadPoolExecutor(max_workers=WORKERS) as ex:
                    futs = [ex.submit(process_item, it, done, q, logf) for it in batch]
                    for _ in as_completed(futs): 
                        pass
                # loop continues; child tasks were enqueued into q by workers
    finally:
        stop_evt.set(); bar.join()

    print(_c(f"\nDone. items={prog['done']}/{prog['queued']} fail={prog['fail']}", "36"))
    print(_c("Outputs:", "36"), OUT_DIR)
    print(_c("Progress:", "33"), PROG_DIR)
    print(_c("Log:", "33"), LOG_PATH)

if __name__=="__main__":
    try: main()
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr); sys.exit(130)
