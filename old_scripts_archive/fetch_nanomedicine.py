#!/usr/bin/env python3
# fetch_nanomed_data.py — Nanomedicine data fetcher with crawling, DOI resolution, resume, and logging

import os, sys, re, time, glob, json, hashlib, shutil, tempfile, threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse, unquote, quote
from urllib.request import Request, urlopen
from ftplib import FTP, error_perm

# ---------------- CLI ----------------
def usage():
    print("Usage: python fetch_nanomed_data.py --url-file FOLDER --out-dir DIR "
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
URL_ARG=URL_ARG or "data/00_urls"
OUT_DIR=OUT_DIR or "data/01_raw/NANOMEDICINE"
WORKERS=max(1,WORKERS)
if PROGRESS_BASE is None:
    PROGRESS_BASE=os.path.join(OUT_DIR,"_progress","fetch_progress")

# ---------------- Utils ----------------
UA={"User-Agent":"nanomed-fetch/3.0"}
URL_RE=re.compile(r'\b(?:https?|ftp)://[^\s"\'<>()]+',re.I)
DOI_RE=re.compile(r'10\.\d{4,9}/[^\s"\'<>]+',re.I)
COLOR=(os.name!="nt") or bool(os.environ.get("WT_SESSION") or os.environ.get("ANSICON") or os.environ.get("TERM_PROGRAM")=="vscode")
def _c(s,code): return f"\x1b[{code}m{s}\x1b[0m" if COLOR else s

def safe_join(base,*parts):
    p=os.path.join(base,*parts)
    return ("\\\\?\\"+os.path.abspath(p)) if os.name=="nt" and not p.startswith("\\\\?\\") else p

def short_path_for(url):
    u=urlparse(url); path=unquote(u.path).lstrip("/") or "index"
    if u.query:
        h=hashlib.sha1(u.query.encode("utf-8")).hexdigest()[:10]
        b,e=os.path.splitext(path); path=f"{b}_{h}{e or ''}"
    return os.path.join(u.scheme or "http", u.netloc or "unknown", path)

def atomic_write(dst,tmp):
    os.makedirs(os.path.dirname(dst),exist_ok=True)
    for i in range(6):
        try:
            if os.path.exists(dst): os.replace(tmp,dst)
            else: shutil.move(tmp,dst)
            return
        except PermissionError: time.sleep(0.5*(i+1))
    raise

# ---------------- Seeds ----------------
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
        except Exception: continue

# ---------------- HTTP/FTP ----------------
def http_download(url,dst):
    fd,tmp=tempfile.mkstemp(prefix=".part-",dir=os.path.dirname(dst)); os.close(fd)
    try:
        with urlopen(Request(url,headers=UA),timeout=60) as r, open(tmp,"wb") as f:
            while True:
                b=r.read(1024*1024)
                if not b: break
                f.write(b)
        atomic_write(dst,tmp); return "ok"
    except Exception as e:
        try: os.remove(tmp)
        except: pass
        return f"fail:{e.__class__.__name__}"

def ftp_connect(host): ftp=FTP(host,timeout=60); ftp.login(); return ftp
def ftp_walk(host,path,dst_root):
    ok=0; fail=0
    with ftp_connect(host) as ftp:
        def walk(cur,rel=""):
            nonlocal ok,fail
            items=[]; ftp.retrlines(f"LIST {cur}",items.append)
            for line in items:
                parts=line.split(maxsplit=8)
                if len(parts)<9: continue
                name=parts[8]; remote=f"{cur.rstrip('/')}/{name}"
                relc=os.path.join(rel,name)
                if line.startswith("d"): walk(remote,relc)
                else:
                    dst=safe_join(dst_root,relc); os.makedirs(os.path.dirname(dst),exist_ok=True)
                    fd,tmp=tempfile.mkstemp(prefix=".part-",dir=os.path.dirname(dst)); os.close(fd)
                    try:
                        with open(tmp,"wb") as f: ftp.retrbinary(f"RETR "+remote,f.write)
                        atomic_write(dst,tmp); ok+=1
                    except: fail+=1
        walk(path or "/","")
    return ok,fail

# ---------------- DOI resolvers ----------------
def _json_get(u):
    try: 
        with urlopen(Request(u,headers=UA),timeout=30) as r:
            return json.loads(r.read().decode("utf-8","ignore"))
    except: return None

def resolve_doi(doi,email=None):
    urls=set()
    # EuropePMC
    d=_json_get(f"https://www.ebi.ac.uk/europepmc/webservices/rest/search?query=DOI:%22{quote(doi)}%22&format=json&pageSize=1")
    if d and "resultList" in d:
        for it in d["resultList"].get("result",[]):
            for ft in (it.get("fullTextUrlList") or {}).get("fullTextUrl",[]):
                if ft.get("url"): urls.add(ft["url"])
            if it.get("pmcid"): urls.add(f"https://www.ncbi.nlm.nih.gov/pmc/articles/{it['pmcid']}/pdf")
    # Unpaywall
    if email:
        d=_json_get(f"https://api.unpaywall.org/v2/{quote(doi)}?email={quote(email)}")
        if d:
            for k in ("best_oa_location","oa_location"):
                loc=d.get(k) or {}
                for kk in ("url_for_pdf","url"): 
                    if loc.get(kk): urls.add(loc[kk])
    # OpenAlex
    d=_json_get(f"https://api.openalex.org/works/doi:{quote(doi)}")
    if d:
        for loc in d.get("locations",[]):
            for k in ("pdf_url","oa_url","url"):
                if loc.get(k): urls.add(loc[k])
    # DataCite
    d=_json_get(f"https://api.datacite.org/dois/{quote(doi)}")
    if d:
        a=(d.get("data") or {}).get("attributes") or {}
        if a.get("url"): urls.add(a["url"])
    return urls

# ---------------- Progress + logs ----------------
prog={"done":0,"total":0,"active":0,"fail":0}
prog_lock=threading.Lock(); stop_evt=threading.Event()
def _paths(base):
    d=os.path.dirname(base) or "."; os.makedirs(d,exist_ok=True)
    return base+".json", base+".done", os.path.join(d,"unresolved_dois.txt"), os.path.join(d,"dead_links.txt"), os.path.join(d,"crawl_discovered.txt")

def progress_bar():
    width=36; t0=time.time()
    while not stop_evt.is_set():
        with prog_lock: d,t,a,f=prog["done"],prog["total"],prog["active"],prog["fail"]
        pct=100*d/max(1,t); filled=int(width*d/max(1,t))
        bar="█"*filled+"░"*(width-filled); rate=d/max(1e-9,time.time()-t0)
        sys.stdout.write(f"\r{_c('[' ,'36')}{_c(bar,'32')}{_c(']','36')} {pct:6.2f}% files {d}/{t} active {a} fail {f} {rate:.2f}/s")
        sys.stdout.flush(); time.sleep(0.2)
    print("")

# ---------------- Crawl engine ----------------
def crawl_expand(url):
    found=set()
    try:
        if "clinicaltrials.gov/api" in url:
            d=_json_get(url); 
            for it in (d or {}).get("studies",[]): 
                if "protocolSection" in it: 
                    sid=it["protocolSection"]["identificationModule"]["nctId"]
                    found.add(f"https://clinicaltrials.gov/study/{sid}")
        elif "metabolomicsworkbench.org" in url:
            if "rest" in url:
                d=_json_get(url)
                if isinstance(d,list):
                    for it in d:
                        if "study_id" in it: 
                            found.add(f"https://www.metabolomicsworkbench.org/data/DRCCMetadata.php?Mode=Study&StudyID={it['study_id']}")
        elif "ebi.ac.uk/pride/ws" in url:
            d=_json_get(url); 
            for it in (d or []): 
                for k in ("downloadLink","fileDownloadLink"): 
                    if it.get(k): found.add(it[k])
        elif url.endswith("/") and any(x in url for x in ("reactome","wikipathways")):
            with urlopen(Request(url,headers=UA),timeout=30) as r:
                txt=r.read().decode("utf-8","ignore")
                for m in re.findall(r'href="([^"]+)"',txt): 
                    if m.endswith((".zip",".gmt",".txt")): found.add(url+m)
    except: pass
    return found

# ---------------- Worker ----------------
def process(u,out_dir,logf,done_set,done_path,unres_path,dead_path,crawl_path,file_lock):
    rel=short_path_for(u); dst=safe_join(out_dir,rel)
    os.makedirs(os.path.dirname(dst),exist_ok=True)
    with prog_lock: prog["active"]+=1
    try:
        if u.lower().startswith("ftp://"):
            ok,fail=ftp_walk(urlparse(u).hostname,urlparse(u).path,os.path.dirname(dst))
            status="ok" if ok else "fail"
        elif DOI_RE.fullmatch(u):
            alt=resolve_doi(u,UNPAYWALL_EMAIL)
            if alt:
                for au in alt:
                    st=http_download(au,dst)
                    if st=="ok": logf.write(f"ALT_OK\t{u}\t{au}\n"); return
            with open(unres_path,"a") as f: f.write(u+"\n"); status="fail"
        elif any(k in u for k in ("clinicaltrials","metabolomicsworkbench","reactome","wikipathways","ebi.ac.uk/pride/ws")):
            for link in crawl_expand(u):
                with open(crawl_path,"a") as f: f.write(link+"\n")
                logf.write(f"CRAWL_DISCOVERED\t{u}\t{link}\n")
            status="crawl"
        else:
            status=http_download(u,dst)
        if status=="ok": 
            uid=hashlib.sha1(u.encode()).hexdigest()
            with file_lock: open(done_path,"a").write(uid+"\t"+u+"\n")
            logf.write(f"OK\t{u}\n")
        elif status.startswith("fail"):
            logf.write(f"FAIL\t{u}\t{status}\n")
            with open(dead_path,"a") as f: f.write(u+"\n")
            with prog_lock: prog["fail"]+=1
    except Exception as e:
        logf.write(f"FAIL\t{u}\t{e}\n")
        with open(dead_path,"a") as f: f.write(u+"\n")
        with prog_lock: prog["fail"]+=1
    finally:
        with prog_lock: prog["active"]-=1; prog["done"]+=1

# ---------------- Main ----------------
def main():
    urls=list(iter_seeds(URL_ARG))
    if not urls: print("No seeds found",file=sys.stderr); sys.exit(1)
    os.makedirs(OUT_DIR,exist_ok=True)
    global json_path
    json_path,done_path,unres_path,dead_path,crawl_path=_paths(PROGRESS_BASE)
    done_set=set()
    with open(json_path,"w") as f: json.dump(prog,f)
    with open(os.path.join(OUT_DIR,"download.log"),"a",encoding="utf-8") as logf:
        stop_evt.clear(); th=threading.Thread(target=progress_bar,daemon=True); th.start()
        file_lock=threading.Lock()
        with ThreadPoolExecutor(max_workers=WORKERS) as ex:
            futs=[ex.submit(process,u,OUT_DIR,logf,done_set,done_path,unres_path,dead_path,crawl_path,file_lock) for u in urls]
            for _ in as_completed(futs): pass
        stop_evt.set(); th.join()

if __name__=="__main__":
    try: main()
    except KeyboardInterrupt: print("\nInterrupted",file=sys.stderr); sys.exit(130)
