#!/usr/bin/env python3
# source_finder.py — discover URLs/DOIs inside downloaded files, filter to BRCA + nanomedicine,
# optionally validate online, score, and (optionally) append to seeds. Windows-safe, no deps.

import os, sys, re, csv, json, time, glob, hashlib, threading, shutil, tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse
from urllib.request import Request, urlopen

# ---------------- Config (defaults; override via CLI) ----------------
ROOT_DIR = os.path.join("data", "01_raw")
SEED_DIR = os.path.join("data", "00_urls")
OUT_DIR  = os.path.join(ROOT_DIR, "_source_finder")
WORKERS  = 12
ONLINE   = False            # --online to enable HEAD/GET validation
MAX_AGE_DAYS = None         # e.g. --max-age-days 365
APPEND_SEEDS = False        # --append-seeds to auto-grow datasets.txt
VERBOSE = False

# ---------------- CLI (minimal, robust) ----------------
a = sys.argv[1:]
while a:
    x = a.pop(0)
    if x in ("-h","--help"):
        print("Usage: python scripts/source_finder.py "
              "[--root DIR] [--seed-dir DIR] [--out DIR] "
              "[--workers N] [--online] [--max-age-days N] [--append-seeds] [--verbose]")
        sys.exit(0)
    elif x == "--root": ROOT_DIR = a.pop(0)
    elif x == "--seed-dir": SEED_DIR = a.pop(0)
    elif x == "--out": OUT_DIR = a.pop(0)
    elif x == "--workers": WORKERS = max(1, int(a.pop(0)))
    elif x == "--online": ONLINE = True
    elif x == "--max-age-days": MAX_AGE_DAYS = int(a.pop(0))
    elif x == "--append-seeds": APPEND_SEEDS = True
    elif x == "--verbose": VERBOSE = True

# ---------------- Utilities ----------------
URL_RE = re.compile(r'\b(?:https?|ftp)://[^\s"\'<>)]+', re.I)
DOI_RE = re.compile(r'10\.\d{4,9}/[^\s"\'<>]+', re.I)
COLOR  = (os.name!="nt") or bool(os.environ.get("WT_SESSION") or os.environ.get("ANSICON") or os.environ.get("TERM_PROGRAM")=="vscode")
def _c(s, code): return f"\x1b[{code}m{s}\x1b[0m" if COLOR else s
def sha1(s:str)->str: return hashlib.sha1(s.encode("utf-8")).hexdigest()
UA = {"User-Agent":"source-finder/1.0"}

TRUST_DOMAINS = {
    "ncbi.nlm.nih.gov": 5, "ebi.ac.uk": 5, "europepmc.org": 5, "clinicaltrials.gov":5,
    "reactome.org": 5, "wikipathways.org": 5, "kegg.jp": 5, "zenodo.org": 4, "figshare.com": 4,
    "datadryad.org": 4, "proteomexchange.org": 5, "pride.ebi.ac.uk": 5, "massive.ucsd.edu": 4,
    "pubchem.ncbi.nlm.nih.gov": 5, "cancer.gov": 5, "cananolab.cancer.gov": 5,
    "github.com": 3, "raw.githubusercontent.com": 3, "ngdc.cncb.ac.cn": 4
}

def is_relevant_text(t:str)->bool:
    t = t.lower()
    return ("breast" in t or "brca" in t or "mammary" in t) and ("nano" in t or "particle" in t or "nanomed" in t)

def domain_score(url:str)->int:
    try:
        host = urlparse(url).hostname or ""
        # allow subdomains to inherit
        parts = host.split(".")
        for i in range(len(parts)-2, -1, -1):
            d = ".".join(parts[i:])
            if d in TRUST_DOMAINS: return TRUST_DOMAINS[d]
        return 1
    except Exception:
        return 1

def safe_makedirs(p:str):
    os.makedirs(p, exist_ok=True)

def write_atomic(path:str, data:str, mode="w", encoding="utf-8"):
    safe_makedirs(os.path.dirname(path))
    tmp = path + ".part"
    with open(tmp, mode, encoding=encoding) as f: f.write(data)
    if os.path.exists(path): os.replace(tmp, path)
    else: os.rename(tmp, path)

def iter_seed_files(seed_dir:str):
    for r,_,fs in os.walk(seed_dir):
        for fn in fs:
            if os.path.splitext(fn.lower())[1] in (".txt",".csv",".lst",".tsv",".urls"):
                yield os.path.join(r,fn)

def load_existing_seeds(seed_dir:str)->set[str]:
    have=set()
    for p in iter_seed_files(seed_dir):
        try:
            with open(p,"r",encoding="utf-8",errors="ignore") as f:
                for line in f:
                    line=line.strip()
                    if not line or line.startswith("#"): continue
                    if URL_RE.search(line): have.add(URL_RE.search(line).group(0))
                    for m in DOI_RE.finditer(line): have.add(m.group(0))
        except Exception: pass
    return have

def read_text_snippet(path:str, limit=5*1024*1024)->str:
    try:
        with open(path,"rb") as f:
            data = f.read(limit)
        try: return data.decode("utf-8")
        except UnicodeDecodeError:
            try: return data.decode("latin-1")
            except UnicodeDecodeError: return ""
    except Exception:
        return ""

def extract_from_text(text:str):
    urls=set(m.group(0) for m in URL_RE.finditer(text))
    dois=set(m.group(0).strip(").,;]") for m in DOI_RE.finditer(text))
    return urls, dois

def scan_file(path:str):
    ext = os.path.splitext(path.lower())[1]
    urls, dois = set(), set()
    # fast skip: binaries likely not parseable
    if ext in (".gz",".zip",".tar",".tgz",".bz2",".7z",".rar",".pdf",".bam",".fastq",".fasta",".nii",".dcm"):
        return urls, dois
    try:
        if ext in (".json",".ndjson"):
            text = read_text_snippet(path)
            try:
                obj = json.loads(text)
            except Exception:
                # fall back to regex if not clean JSON
                return extract_from_text(text)
            # walk JSON
            stack=[obj]
            while stack:
                v=stack.pop()
                if isinstance(v, dict):
                    for k,val in v.items():
                        if isinstance(val,(dict,list)): stack.append(val)
                        elif isinstance(val,str):
                            u,d = extract_from_text(val)
                            urls |= u; dois |= d
                elif isinstance(v, list):
                    stack.extend(v)
                elif isinstance(v, str):
                    u,d = extract_from_text(v)
                    urls |= u; dois |= d
            return urls, dois
        else:
            text = read_text_snippet(path)
            # crude HTML anchor extraction still covered by URL regex in text
            return extract_from_text(text)
    except Exception:
        return urls, dois

# ---------------- Online validation (optional) ----------------
def head_info(url:str, timeout=20):
    try:
        req = Request(url, headers=UA, method="HEAD")
        with urlopen(req, timeout=timeout) as r:
            status = getattr(r, "status", 200)
            lm = r.headers.get("Last-Modified") or r.headers.get("Date") or ""
            return status, lm
    except Exception:
        # try minimal GET (some endpoints disallow HEAD)
        try:
            req = Request(url, headers=UA)
            with urlopen(req, timeout=timeout) as r:
                status = getattr(r, "status", 200)
                lm = r.headers.get("Last-Modified") or r.headers.get("Date") or ""
                return status, lm
        except Exception as e:
            return None, ""

def age_days_from_http_date(datestr:str)->int|None:
    # very tolerant; just parse RFC-ish by slicing month names
    try:
        import email.utils, datetime
        tt = email.utils.parsedate_to_datetime(datestr)
        return max(0, (datetime.datetime.utcnow() - tt.replace(tzinfo=None)).days)
    except Exception:
        return None

# ---------------- Progress bar ----------------
prog = {"files":0,"total":0,"active":0}
prog_lock = threading.Lock()
stop_evt = threading.Event()

def progress_bar():
    width=36; t0=time.time()
    while not stop_evt.is_set():
        with prog_lock:
            f=prog["files"]; t=prog["total"]; a=prog["active"]
        pct=100.0*f/max(1,t); filled=int(width*f/max(1,t))
        bar="█"*filled+"░"*(width-filled); rate=f/max(1e-9,time.time()-t0)
        sys.stdout.write(f"\r{_c('[','36')}{_c(bar,'32')}{_c(']','36')} {pct:6.2f}% files {f}/{t} active {a} {rate:.2f}/s")
        sys.stdout.flush(); time.sleep(0.2)
    print("")

# ---------------- Main work ----------------
def main():
    safe_makedirs(OUT_DIR)
    urls_all, dois_all = set(), set()
    # enumerate files
    files=[]
    for r,_,fs in os.walk(ROOT_DIR):
        for fn in fs:
            files.append(os.path.join(r,fn))
    with prog_lock: prog["total"]=len(files)

    bar = threading.Thread(target=progress_bar, daemon=True); bar.start()

    try:
        with ThreadPoolExecutor(max_workers=WORKERS) as ex:
            futs = {ex.submit(scan_file, p): p for p in files}
            for fut in as_completed(futs):
                p = futs[fut]
                try:
                    u,d = fut.result()
                    # Relevance filter: prefer matches already in the path or text
                    rel_hint = is_relevant_text(os.path.basename(p))
                    # Keep everything found, but relevance will be scored later
                    urls_all |= u; dois_all |= d
                except Exception:
                    pass
                finally:
                    with prog_lock:
                        prog["files"] += 1
    finally:
        stop_evt.set(); bar.join()

    # Dedup + sort
    urls_all = sorted(urls_all)
    dois_all  = sorted(set(x.strip(").,;]") for x in dois_all))

    # Existing seeds (to avoid duplicates in next_seeds)
    existing = load_existing_seeds(SEED_DIR)

    # Score and optionally validate
    rows=[]
    if ONLINE or MAX_AGE_DAYS is not None:
        for u in urls_all:
            status, lm = head_info(u) if ONLINE else (None,"")
            age = age_days_from_http_date(lm) if lm else None
            rel = is_relevant_text(u)
            trust = domain_score(u)
            ok_age = (age is None) or (MAX_AGE_DAYS is None) or (age <= MAX_AGE_DAYS)
            rows.append((u, urlparse(u).hostname or "", trust, int(rel), status if status is not None else "", lm, age if age is not None else ""))
    else:
        for u in urls_all:
            rel = is_relevant_text(u)
            trust = domain_score(u)
            rows.append((u, urlparse(u).hostname or "", trust, int(rel), "", "", ""))

    # Write outputs
    write_atomic(os.path.join(OUT_DIR,"urls_found.txt"), "\n".join(urls_all)+"\n" if urls_all else "")
    write_atomic(os.path.join(OUT_DIR,"dois_found.txt"), "\n".join(dois_all)+"\n" if dois_all else "")
    # Scored TSV
    tsv_path = os.path.join(OUT_DIR,"sources_scored.tsv")
    safe_makedirs(os.path.dirname(tsv_path))
    with open(tsv_path+".part","w",newline="",encoding="utf-8") as f:
        w=csv.writer(f, delimiter="\t")
        w.writerow(["url","domain","trust_score","relevant_brca_nano","http_status","last_modified","age_days"])
        for r in rows: w.writerow(r)
    if os.path.exists(tsv_path): os.replace(tsv_path+".part",tsv_path)
    else: os.rename(tsv_path+".part", tsv_path)

    # next_seeds: relevant + reasonably trusted + (if online) reachable + not already in seeds
    next_seeds=[]
    for (u, dom, trust, rel, status, lm, age) in rows:
        if not rel: continue
        if trust < 3: continue
        if ONLINE and status not in (200, 301, 302): continue
        if MAX_AGE_DAYS is not None and isinstance(age,int) and age > MAX_AGE_DAYS: continue
        if (u in existing): continue
        next_seeds.append(u)
    next_seeds = sorted(set(next_seeds))
    write_atomic(os.path.join(OUT_DIR,"next_seeds.txt"), "\n".join(next_seeds)+"\n" if next_seeds else "")

    # Optional: append to master datasets seed (deduped)
    if APPEND_SEEDS and next_seeds:
        candidates = [os.path.join(SEED_DIR,"breast_nanomedicine_datasets.txt"),
                      os.path.join(SEED_DIR,"_auto_datasets.txt")]
        target = candidates[0] if os.path.exists(candidates[0]) else candidates[1]
        safe_makedirs(SEED_DIR)
        try:
            with open(target,"r",encoding="utf-8") as f:
                have=set(l.strip() for l in f if l.strip())
        except FileNotFoundError:
            have=set()
        new=[u for u in next_seeds if u not in have]
        if new:
            with open(target,"a",encoding="utf-8") as f:
                for u in new: f.write(u+"\n")

    # Summary JSON
    summary = {
        "root_scanned": ROOT_DIR,
        "total_files": len(files),
        "urls_found": len(urls_all),
        "dois_found": len(dois_all),
        "next_seeds": len(next_seeds),
        "online_validation": ONLINE,
        "max_age_days": MAX_AGE_DAYS,
        "appended_to_seeds": APPEND_SEEDS
    }
    write_atomic(os.path.join(OUT_DIR,"source_finder_summary.json"), json.dumps(summary, indent=2))

    print(_c(f"\nDone. Files scanned={len(files)} URLs={len(urls_all)} DOIs={len(dois_all)} next_seeds={len(next_seeds)}", "36"))
    print(_c(f"Outputs: {OUT_DIR}", "36"))
    if APPEND_SEEDS:
        print(_c(f"Seeds updated in {SEED_DIR}", "33"))

if __name__ == "__main__":
    try: main()
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr); sys.exit(130)
