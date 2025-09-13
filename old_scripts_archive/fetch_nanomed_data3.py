#!/usr/bin/env python3
# fetch_nanomed_data.py — Nanomedicine data fetcher with crawling, DOI resolution, resume, and logging
import os, sys, re, time, glob, json, hashlib, shutil, tempfile, threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse, unquote, quote
from urllib.request import Request, urlopen
from ftplib import FTP

# -------- CLI (minimal, robust) --------
def usage():
    print("Usage: python fetch_nanomed_data.py --url-file FOLDER_OR_FILE --out-dir DIR "
          "[--workers N] [--progress-base PATH] [--unpaywall-email EMAIL] [--verbose]", file=sys.stderr)
    sys.exit(2)

URL_ARG=None; OUT_DIR=None; WORKERS=8; VERBOSE=False
PROGRESS_BASE=None; UNPAYWALL_EMAIL=None
a=sys.argv[1:]
while a:
    x=a.pop(0)
    if x in ("-h","--help"): usage()
    elif x=="--url-file": URL_ARG=a.pop(0) if a else usage()
    elif x=="--out-dir": OUT_DIR=a.pop(0) if a else usage()
    elif x=="--workers": WORKERS=int(a.pop(0)) if a else usage()
    elif x=="--progress-base": PROGRESS_BASE=a.pop(0) if a else usage()
    elif x=="--unpaywall-email": UNPAYWALL_EMAIL=a.pop(0) if a else usage()
    elif x=="--verbose": VERBOSE=True
    else: URL_ARG=x
URL_ARG = URL_ARG or "data/00_urls"
OUT_DIR = OUT_DIR or "data/01_raw/NANOMEDICINE"
WORKERS = max(1, WORKERS)
PROGRESS_BASE = PROGRESS_BASE or os.path.join(OUT_DIR, "_progress", "fetch_progress")

# -------- Logging, color, constants --------
UA = {"User-Agent":"nanomed-fetch/3.1"}
URL_RE = re.compile(r'\b(?:https?|ftp)://[^\s"\'<>()]+', re.I)
DOI_RE = re.compile(r'10\.\d{4,9}/[^\s"\'<>]+', re.I)
LOG = os.path.join("logs", "log.txt")
os.makedirs(os.path.dirname(LOG), exist_ok=True)

def log(msg: str):
    with open(LOG, "a", encoding="utf-8") as fh:
        fh.write(time.strftime("[%Y-%m-%d %H:%M:%S] ") + msg + "\n")

def safe_join(base,*parts):
    p = os.path.join(base,*parts)
    if os.name=="nt" and not p.startswith("\\\\?\\"):
        p = "\\\\?\\" + os.path.abspath(p)
    return p

def short_path_for(url):
    u = urlparse(url); path = unquote(u.path).lstrip("/") or "index"
    if u.query:
        h = hashlib.sha1(u.query.encode("utf-8")).hexdigest()[:10]
        b,e = os.path.splitext(path); path = f"{b}_{h}{e or ''}"
    return os.path.join(u.scheme or "http", u.netloc or "unknown", path)

def atomic_write(dst,tmp):
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    for i in range(6):
        try:
            if os.path.exists(dst): os.replace(tmp, dst)
            else: shutil.move(tmp, dst)
            return
        except PermissionError:
            time.sleep(0.5*(i+1))
    raise

# -------- seed expansion --------
def _expand_sources(arg):
    if os.path.isdir(arg):
        out=[]
        for r,_,fs in os.walk(arg):
            for fn in fs:
                if fn.lower().endswith((".txt",".csv",".lst")):
                    out.append(os.path.join(r,fn))
        return out
    if any(ch in arg for ch in "*?["): return glob.glob(arg)
    return [arg]

def iter_seeds(path_or_dir):
    seen=set()
    for p in _expand_sources(path_or_dir):
        try:
            with open(p,"r",encoding="utf-8",errors="ignore") as f:
                for line in f:
                    line=line.strip()
                    if not line or line.startswith("#"): continue
                    for m in URL_RE.finditer(line):
                        u=m.group(0).strip()
                        if u not in seen: seen.add(u); yield u
                    for m in DOI_RE.finditer(line):
                        doi=m.group(0).strip(").,;")
                        if doi not in seen: seen.add(doi); yield doi
        except Exception as e:
            log(f"[SEEDS][WARN] {p}: {e}")

# -------- HTTP/FTP --------
def http_download(url,dst):
    tmp_fd, tmp = tempfile.mkstemp(prefix=".part-", dir=os.path.dirname(dst))
    os.close(tmp_fd)
    try:
        req = Request(url, headers=UA)
        with urlopen(req, timeout=60) as r, open(tmp, "wb") as f:
            while True:
                b = r.read(1024*256)
                if not b: break
                f.write(b)
        atomic_write(dst, tmp)
        return "ok", None
    except Exception as e:
        try: os.remove(tmp)
        except: pass
        return "fail", f"{type(e).__name__}: {e}"

def ftp_connect(host): ftp=FTP(host,timeout=60); ftp.login(); return ftp

def ftp_walk(host,path,dst_root):
    ok=0; fail=0
    try:
        with ftp_connect(host) as ftp:
            def walk(cur, rel=""):
                nonlocal ok, fail
                items=[]
                try: ftp.retrlines(f"LIST {cur}", items.append)
                except Exception: return
                for line in items:
                    parts=line.split(maxsplit=8)
                    if len(parts)<9: continue
                    name=parts[8]; remote=f"{cur.rstrip('/')}/{name}"
                    relc=os.path.join(rel,name)
                    if line.startswith("d"):
                        walk(remote, relc)
                    else:
                        dst = safe_join(dst_root, relc)
                        os.makedirs(os.path.dirname(dst), exist_ok=True)
                        tmp_fd, tmp = tempfile.mkstemp(prefix=".part-", dir=os.path.dirname(dst))
                        os.close(tmp_fd)
                        try:
                            with open(tmp,"wb") as f: ftp.retrbinary("RETR "+remote, f.write)
                            atomic_write(dst,tmp); ok+=1
                        except Exception:
                            fail+=1
            walk(path or "/","")
        return ("ok" if ok else "fail"), f"ftp files ok={ok} fail={fail}"
    except Exception as e:
        return "fail", f"{type(e).__name__}: {e}"

# -------- DOI & simple crawlers --------
def _json_get(u):
    try:
        with urlopen(Request(u,headers=UA), timeout=30) as r:
            return json.loads(r.read().decode("utf-8","ignore"))
    except: return None

def resolve_doi(doi,email=None):
    urls=set()
    d=_json_get(f"https://www.ebi.ac.uk/europepmc/webservices/rest/search?query=DOI:%22{quote(doi)}%22&format=json&pageSize=1")
    if d and "resultList" in d:
        for it in d["resultList"].get("result",[]):
            for ft in (it.get("fullTextUrlList") or {}).get("fullTextUrl",[]):
                if ft.get("url"): urls.add(ft["url"])
            if it.get("pmcid"): urls.add(f"https://www.ncbi.nlm.nih.gov/pmc/articles/{it['pmcid']}/pdf")
    if email:
        d=_json_get(f"https://api.unpaywall.org/v2/{quote(doi)}?email={quote(email)}")
        if d:
            for loc in [d.get("best_oa_location") or {}, d.get("oa_location") or {}]:
                for k in ("url_for_pdf","url"):
                    if loc.get(k): urls.add(loc[k])
    d=_json_get(f"https://api.openalex.org/works/doi:{quote(doi)}")
    if d:
        for loc in d.get("locations",[]):
            for k in ("pdf_url","oa_url","url"):
                if loc.get(k): urls.add(loc[k])
    d=_json_get(f"https://api.datacite.org/dois/{quote(doi)}")
    if d:
        a=(d.get("data") or {}).get("attributes") or {}
        if a.get("url"): urls.add(a["url"])
    return urls

def crawl_expand(url):
    found=set()
    try:
        if "clinicaltrials.gov/api" in url:
            d=_json_get(url)
            for it in (d or {}).get("studies",[]):
                sec=it.get("protocolSection") or {}
                sid=(sec.get("identificationModule") or {}).get("nctId")
                if sid: found.add(f"https://clinicaltrials.gov/study/{sid}")
        elif "metabolomicsworkbench.org" in url and "rest" in url:
            d=_json_get(url)
            if isinstance(d,list):
                for it in d:
                    if "study_id" in it:
                        found.add(f"https://www.metabolomicsworkbench.org/data/DRCCMetadata.php?Mode=Study&StudyID={it['study_id']}")
        elif "ebi.ac.uk/pride/ws" in url:
            d=_json_get(url)
            for it in (d or []):
                for k in ("downloadLink","fileDownloadLink"):
                    if isinstance(it,dict) and it.get(k): found.add(it[k])
    except: pass
    return found

# -------- small state files around PROGRESS_BASE --------
def _paths(base):
    d=os.path.dirname(base) or "."
    os.makedirs(d, exist_ok=True)
    return base+".json", base+".done", os.path.join(d,"unresolved_dois.txt"), os.path.join(d,"dead_links.txt"), os.path.join(d,"crawl_discovered.txt")

# -------- worker --------
def process_one(u, out_dir, done_path, unres_path, dead_path, crawl_path):
    rel = short_path_for(u); dst = safe_join(out_dir, rel)
    os.makedirs(os.path.dirname(dst), exist_ok=True)

    try:
        if u.lower().startswith("ftp://"):
            status, msg = ftp_walk(urlparse(u).hostname, urlparse(u).path, os.path.dirname(dst))
            if status == "ok":
                log(f"[OK][FTP] {u} :: {msg}")
                with open(done_path,"a") as f: f.write(hashlib.sha1(u.encode()).hexdigest()+"\t"+u+"\n")
                return "ok"
            else:
                log(f"[FAIL][FTP] {u} :: {msg}")
                with open(dead_path,"a") as f: f.write(u+"\n")
                return "fail"

        if DOI_RE.fullmatch(u):
            alts = resolve_doi(u, UNPAYWALL_EMAIL)
            for au in alts:
                st, err = http_download(au, dst)
                if st == "ok":
                    log(f"[ALT_OK][DOI] {u} -> {au}")
                    with open(done_path,"a") as f: f.write(hashlib.sha1(u.encode()).hexdigest()+"\t"+u+"\n")
                    return "ok"
            with open(unres_path,"a") as f: f.write(u+"\n")
            log(f"[FAIL][DOI] {u} :: unresolved")
            return "fail"

        if any(k in u for k in ("clinicaltrials","metabolomicsworkbench","ebi.ac.uk/pride/ws")):
            for link in crawl_expand(u):
                with open(crawl_path,"a") as f: f.write(link+"\n")
                log(f"[CRAWL] {u} -> {link}")
            return "crawl"

        st, err = http_download(u, dst)
        if st == "ok":
            with open(done_path,"a") as f: f.write(hashlib.sha1(u.encode()).hexdigest()+"\t"+u+"\n")
            log(f"[OK] {u}")
            return "ok"
        else:
            with open(dead_path,"a") as f: f.write(u+"\n")
            log(f"[FAIL] {u} :: {err}")
            return "fail"

    except Exception as e:
        with open(dead_path,"a") as f: f.write(u+"\n")
        log(f"[FAIL] {u} :: {type(e).__name__}: {e}")
        return "fail"

# -------- main with Rich progress (never >100%) --------
def main():
    try:
        from rich.console import Console
        from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn
    except Exception:
        print("[ERROR] Install 'rich' for progress UI: pip install rich", file=sys.stderr); sys.exit(1)

    urls = list(iter_seeds(URL_ARG))
    if not urls:
        print("No seeds found", file=sys.stderr); sys.exit(1)

    os.makedirs(OUT_DIR, exist_ok=True)
    json_path, done_path, unres_path, dead_path, crawl_path = _paths(PROGRESS_BASE)
    with open(json_path, "w") as f: json.dump({"total": len(urls)}, f)

    console = Console()
    ok = fail = crawled = 0

    with Progress(
        TextColumn("[bold cyan]Fetch"), BarColumn(),
        TextColumn("{task.percentage:>3.0f}%"),
        TextColumn("• {task.completed}/{task.total}"),
        TextColumn("• ok={task.fields[ok]} fail={task.fields[fail]} crawl={task.fields[crawl]}"),
        TimeRemainingColumn(),
        console=console, refresh_per_second=8, transient=False
    ) as progress:
        t = progress.add_task("Fetch", total=len(urls), ok=0, fail=0, crawl=0)

        # thread pool; only main thread updates the bar
        with ThreadPoolExecutor(max_workers=WORKERS) as ex:
            futs = [ex.submit(process_one, u, OUT_DIR, done_path, unres_path, dead_path, crawl_path) for u in urls]
            for fut in as_completed(futs):
                status = fut.result()
                if status == "ok": ok += 1
                elif status == "crawl": crawled += 1
                else: fail += 1
                progress.update(t, advance=1, ok=ok, fail=fail, crawl=crawled)

    console.print(f"[green]Done.[/green] ok={ok} crawl={crawled} fail={fail}")
    log(f"[SUMMARY] ok={ok} crawl={crawled} fail={fail} total={len(urls)}")

if __name__=="__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted", file=sys.stderr); sys.exit(130)
