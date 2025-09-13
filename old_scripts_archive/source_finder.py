# -*- coding: utf-8 -*-
"""
Source Finder (breast-cancer-ready):
- Input: --url-dir <folder of *.txt/*.csv/logs> or --url-file <single file>
- Output: --registry <CSV>  (atomic, idempotent)
- Behavior: find URLs (regex, tolerant of logs), auto-repair, dedupe, verify (HTTP/FTP), retry+timeout,
            progress bar, minimal logs, Windows-friendly.

Examples (PowerShell):
  .venv\Scripts\python.exe scripts\source_finder.py --url-dir data\00_urls\brca_nanomedicine --registry data\02_interim\source_registry.csv --workers 16
  .venv\Scripts\python.exe scripts\source_finder.py --url-file data\00_urls\clean_brca_all.txt --registry data\02_interim\source_registry.csv --topic breast_cancer --force
"""
import os, sys, re, csv, time, ssl, socket, argparse, hashlib, random, traceback
from datetime import datetime, timezone
from urllib.parse import urlparse, parse_qs, unquote_plus
from urllib.request import Request, urlopen
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    import ftplib
except Exception:
    ftplib = None  # guarded below

# --- Nanomedicine-focused trust domains (0–5) + scoring helper -----------------
# Paste this block after your imports in BOTH scripts.

TRUST_DOMAINS: dict[str,int] = {
    # Primary repositories / registries / clinical / OA resolvers (5)
    "ncbi.nlm.nih.gov": 5,          # GEO/SRA/PMC/BioProject
    "pubchem.ncbi.nlm.nih.gov": 5,  # Bioassays/chemistry
    "ebi.ac.uk": 5,                 # PRIDE, ArrayExpress/BioStudies, ChEMBL, MetaboLights (subdomains inherit)
    "pride.ebi.ac.uk": 5,           # PRIDE CDN
    "proteomexchange.org": 5,       # ProteomeXchange registry
    "biostudies.ebi.ac.uk": 5,      # ArrayExpress under BioStudies
    "metabolomicsworkbench.org": 5, # MWB studies + REST API
    "reactome.org": 5,              # Pathway KB + full dumps
    "wikipathways.org": 5,          # Community pathways + GMT
    "clinicaltrials.gov": 5,        # Trials registry + API
    "cancer.gov": 5,                # NCI oncology content
    "cananolab.cancer.gov": 5,      # Curated nanomaterials
    "europepmc.org": 5,             # OA full text finder
    "api.unpaywall.org": 5,         # OA resolver for DOIs
    "openalex.org": 5,              # Scholarly graph + OA
    "api.datacite.org": 5,          # DOI metadata / landing URLs
    "doi.org": 5,                   # DOI resolver root

    # Reputable open data / chemistry / proteomics hubs (4)
    "zenodo.org": 4, "figshare.com": 4, "datadryad.org": 4, "osf.io": 4,
    "massive.ucsd.edu": 4, "jpostdb.org": 4, "uniprot.org": 4, "kegg.jp": 4,
    "nist.gov": 4, "epa.gov": 4, "comptox.epa.gov": 4,
    "ngdc.cncb.ac.cn": 4,           # NanoMiner
    "data.enanomapper.net": 4,      # eNanoMapper
    "infrastructure.nanocommons.eu": 4,  # NanoCommons services

    # Useful but mixed-signal sources (3)
    "nanocommons.github.io": 3,
    "github.com": 3, "raw.githubusercontent.com": 3, "gitlab.com": 3,
    "arxiv.org": 3, "biorxiv.org": 3, "medrxiv.org": 3,
    "nature.com": 3, "sciencemag.org": 3, "pnas.org": 3, "cell.com": 3,
    "pubs.acs.org": 3, "rsc.org": 3, "link.springer.com": 3,
    "onlinelibrary.wiley.com": 3, "sciencedirect.com": 3,
    "nanohub.org": 3,

    # Low-signal / mirrors (1)
    "researchgate.net": 1, "academia.edu": 1, "medium.com": 1,
    "quora.com": 1, "reddit.com": 1,

    # Retired/unstable but historically relevant (1)
    "ncihub.org": 1, "nbi.oregonstate.edu": 1, "nanominer.cs.tut.fi": 1,

    # Ignore / URL shorteners / social (0)
    "pinterest.com": 0, "facebook.com": 0, "twitter.com": 0,
    "t.co": 0, "bit.ly": 0, "goo.gl": 0, "tinyurl.com": 0,
}

def _host_from(url_or_host: str) -> str:
    """Accept a URL or bare host; return normalized host (lowercase, no port)."""
    from urllib.parse import urlparse
    if "://" in url_or_host:
        h = urlparse(url_or_host).hostname or ""
    else:
        h = url_or_host
    return h.lower().split(":")[0]

def domain_score(url_or_host: str) -> int:
    """
    Score 0–5 using suffix inheritance: 'sub.a.b' tries 'sub.a.b','a.b','b'.
    Unknown hosts default to 1 (low but not excluded).
    """
    host = _host_from(url_or_host)
    if not host: return 1
    parts = host.split(".")
    for i in range(len(parts)-2, -1, -1):          # try longest suffix to TLD
        key = ".".join(parts[i:])
        if key in TRUST_DOMAINS:
            return TRUST_DOMAINS[key]
    return TRUST_DOMAINS.get(host, 1)

def domain_allowed(url_or_host: str, min_score: int = 3) -> bool:
    """Convenience predicate to keep only hosts scoring >= min_score."""
    return domain_score(url_or_host) >= min_score
# -------------------------------------------------------------------------------


# ---------- tiny console UI ----------
def _supports_color():
    return os.name != "nt" or bool(os.environ.get("WT_SESSION") or os.environ.get("ANSICON") or os.environ.get("TERM_PROGRAM") == "vscode")
COLOR = _supports_color()
def _c(code): return f"\x1b[{code}m" if COLOR else ""
def info(msg): print(f"{_c('36')}[INFO]{_c('0')} {msg}", flush=True)
def warn(msg): print(f"{_c('33')}[WARN]{_c('0')} {msg}", flush=True)
def err(msg):  print(f"{_c('31')}[ERR ]{_c('0')} {msg}",  flush=True)

def progress(done, total, prefix="Checking"):
    total = max(total, 1); width = 36
    filled = int(width * done / total)
    bar = "█"*filled + "░"*(width-filled)
    pct = int(100 * done / total)
    sys.stdout.write(f"\r{_c('35')}{prefix}{_c('0')} {bar} {pct:3d}%   •  {done}/{total}")
    sys.stdout.flush()
    if done >= total:
        sys.stdout.write("\n"); sys.stdout.flush()

# ---------- helpers ----------
HEADERS = {"User-Agent": "SourceFinder/1.0 (Windows; Python 3.11)"}
TIMEOUT = 20
RETRY = 3

def sha1(s: str) -> str: return hashlib.sha1((s or "").encode("utf-8","ignore")).hexdigest()
def safe_now() -> str:   return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def atomic_write_csv(path, rows, fieldnames):
    tmp = path + ".tmp"
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(tmp, "w", encoding="utf-8", newline="") as f:
        wr = csv.DictWriter(f, fieldnames=fieldnames)
        wr.writeheader()
        for r in rows:
            wr.writerow({k: r.get(k,"") for k in fieldnames})
    # Replace atomically; retry once for AV locks
    try:
        if os.path.exists(path): os.replace(tmp, path)
        else: os.rename(tmp, path)
    except PermissionError:
        time.sleep(0.6); os.replace(tmp, path)

def log_append(log_path, line):
    os.makedirs(os.path.dirname(log_path) or ".", exist_ok=True)
    with open(log_path, "a", encoding="utf-8", newline="") as f:
        f.write(line.replace("\n"," ") + "\n")

# ---------- repo & URL normalization ----------
def guess_repo(url: str) -> str:
    u = (url or "").lower()
    if "ncbi.nlm.nih.gov/geo" in u: return "NCBI GEO"
    if "ncbi.nlm.nih.gov/sra" in u or "sra-download" in u: return "NCBI SRA"
    if "gdc.cancer.gov" in u or "api.gdc.cancer.gov" in u: return "NCI GDC"
    if "ebi.ac.uk" in u or "arrayexpress" in u: return "EBI"
    if "pride.ebi.ac.uk" in u or "proteomexchange" in u: return "ProteomeXchange/PRIDE"
    if "zenodo" in u: return "Zenodo"
    if "figshare" in u: return "Figshare"
    if "osf.io" in u: return "OSF"
    if u.startswith("ftp://"): return "FTP (generic)"
    if u.startswith("https://github.com") or u.startswith("https://raw.githubusercontent.com"): return "GitHub"
    return "Web (generic)"

def guess_access(url: str) -> str:
    s = (urlparse(url).scheme or "").lower()
    if s in ("http","https"): return "HTTP"
    if s == "ftp": return "FTP"
    return s.upper() or "UNKNOWN"

TLD_RX = r"(?:org|gov|edu|com|net|ac\.uk|co\.uk|nih\.gov)"
def _unwrap_google(u: str) -> str:
    try:
        p = urlparse(u)
        if p.netloc.endswith("google.com") and p.path.startswith("/search"):
            q = parse_qs(p.query).get("q", [""])[0]
            if q.startswith(("http://","https://")): return unquote_plus(q.strip())
    except Exception:
        pass
    return u

def fix_url(u: str) -> str:
    if not u: return u
    u = _unwrap_google(u.strip())
    u = u.replace(" ", "")  # kill control spaces inside URL
    # Insert :// if missing after http/https variants
    if u.startswith("httpswww."): u = "https://www." + u[len("httpswww."):]
    elif u.startswith("httpwww."): u = "http://www." + u[len("httpwww."):]
    elif u.startswith("https") and not u.startswith("https://"): u = "https://" + u[len("https"):]
    elif u.startswith("http") and not (u.startswith("http://") or u.startswith("https://")): u = "http://" + u[len("http"):]
    # Glue fix after TLD (domain+path stuck)
    if u.startswith(("http://","https://")):
        scheme, rest = u.split("://",1)
        if "/" not in rest:
            mm = re.match(rf"^([a-z0-9.-]+\.{TLD_RX})(.+)$", rest, re.I)
            if mm: u = f"{scheme}://{mm.group(1)}/{mm.group(2).lstrip('/')}"
    # Site-specific normalizations
    u = re.sub(r"(proteomecentral\.proteomexchange\.org)cgiGetDatasetID=",
               r"\1/cgi/GetDataset?ID=", u, flags=re.I)
    u = re.sub(r"(www\.ncbi\.nlm\.nih\.gov)geoqueryacc\.cgiacc=",
               r"\1/geo/query/acc.cgi?acc=", u, flags=re.I)
    u = re.sub(r"(www\.ncbi\.nlm\.nih\.gov)bioproject(?!/)",
               r"\1/bioproject/", u, flags=re.I)
    return u

URL_RX = re.compile(r'(?i)\b(?:https?|ftp)://[^\s<>"\')]+')
def extract_urls_from_line(line: str):
    # Extract URLs from any line (works for logs like "ERR ... URL ...")
    return URL_RX.findall(line or "")

def read_all_urls_from_file(path: str):
    urls = []
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            for ln in f:
                # Prefer regex extraction so comments/columns/log-tags don't break us
                for u in extract_urls_from_line(ln):
                    urls.append(u)
        # If CSV has a 'url' column but regex found nothing (edge), fallback to CSV parse
        if not urls and path.lower().endswith(".csv"):
            f.seek(0)  # type: ignore
            with open(path, "r", encoding="utf-8", errors="ignore", newline="") as g:
                rd = csv.DictReader(g)
                col = next((c for c in ("url","URL","link","href") if c in (rd.fieldnames or [])), None)
                if col:
                    for row in rd:
                        u = (row.get(col) or "").strip()
                        if u: urls.append(u)
    except Exception as e:
        err(f"Failed reading {path}: {e}")
    return urls

def discover_urls(url_dir=None, url_file=None):
    files = []
    if url_file:
        if os.path.isfile(url_file): files.append(url_file)
        else: warn(f"--url-file not found: {url_file}")
    if url_dir and os.path.isdir(url_dir):
        for root,_,names in os.walk(url_dir):
            for n in names:
                if n.lower().endswith((".txt",".csv",".list",".log")):
                    files.append(os.path.join(root, n))
    if not files:
        warn("No input url files found."); return []

    all_pairs = []
    for p in files:
        for u in read_all_urls_from_file(p):
            all_pairs.append((u, p))

    # Normalize + dedupe
    cleaned, seen = [], set()
    for u, src in all_pairs:
        u = fix_url(u)
        if not u: continue
        # Drop obvious junk (search pages with no unwrap)
        if "google.com/search?" in u and "q=" not in u: continue
        key = u.lower()
        if key not in seen:
            seen.add(key); cleaned.append((u, src))
    return cleaned

# ---------- HTTP/FTP checks ----------
def http_check(url):
    ctx = ssl.create_default_context()
    for attempt in range(1, RETRY+1):
        try:
            req = Request(url, headers=HEADERS, method="HEAD")
            with urlopen(req, timeout=TIMEOUT, context=ctx) as r:
                code = getattr(r, "status", 200)
                return ("OK" if 200 <= code < 400 else "BAD", code, "")
        except Exception as e1:
            try:
                req = Request(url, headers=HEADERS, method="GET")
                with urlopen(req, timeout=TIMEOUT, context=ctx) as r:
                    code = getattr(r, "status", 200)
                    _ = r.read(1024)  # prove connectivity
                    return ("OK" if 200 <= code < 400 else "BAD", code, "")
            except Exception as e2:
                if attempt >= RETRY:
                    return "ERR", 0, (str(e2) or str(e1))[:240]
                time.sleep(0.4*attempt + random.random()*0.3)
    return "ERR", 0, "exhausted"

def ftp_check(url):
    if not ftplib:
        return "ERR", 0, "ftplib missing"
    p = urlparse(url)
    host, path = p.hostname, (p.path or "/")
    if not host:
        return "ERR", 0, "no host"
    for attempt in range(1, RETRY+1):
        try:
            with ftplib.FTP() as ftp:
                ftp.connect(host, p.port or 21, timeout=TIMEOUT)
                ftp.login("anonymous", "anonymous@")
                ftp.set_pasv(True)
                if path.endswith("/"):
                    _ = ftp.nlst(path)  # directory exists?
                else:
                    try:
                        _ = ftp.size(path)  # file exists?
                    except Exception:
                        parent = os.path.dirname(path) or "/"
                        _ = ftp.nlst(parent)
                return "OK", 226, ""
        except Exception as e:
            if attempt >= RETRY:
                return "ERR", 0, (str(e)[:240] or "ftp failed")
            time.sleep(0.5*attempt + random.random()*0.4)
    return "ERR", 0, "exhausted"

def check_one(row):
    url = row["url"]
    access = row["access"]
    if access == "HTTP":
        status, code, reason = http_check(url)
    elif access == "FTP":
        status, code, reason = ftp_check(url)
    else:
        status, code, reason = "SKIP", 0, f"unsupported scheme: {access}"
    row["status"] = status
    row["code"] = str(code)
    row["reason"] = reason
    row["last_checked_utc"] = safe_now()
    return row

# ---------- registry IO ----------
def load_registry(path):
    rows = {}
    if path and os.path.isfile(path):
        try:
            with open(path, "r", encoding="utf-8", errors="ignore", newline="") as f:
                rd = csv.DictReader(f)
                for r in rd:
                    rows[r.get("id","")] = r
        except Exception as e:
            warn(f"Could not read registry (will rebuild): {e}")
    return rows

# ---------- main ----------
def main():
    ap = argparse.ArgumentParser(description="Discover, normalize, verify sources and write registry.")
    ap.add_argument("--url-dir", help="Folder with *.txt/*.csv/*.log")
    ap.add_argument("--url-file", help="Single file with URLs")
    ap.add_argument("--registry", required=True, help="Output CSV registry path")
    ap.add_argument("--topic", default="", help="Topic tag (e.g., breast_cancer)")
    ap.add_argument("--priority", type=int, default=2, help="1=high, 2=normal, 3=low")
    ap.add_argument("--workers", type=int, default=min(16, max(2, (os.cpu_count() or 4)*2)), help="Concurrent workers")
    ap.add_argument("--force", action="store_true", help="Recheck even if status=OK")
    args = ap.parse_args()

    url_pairs = discover_urls(args.url_dir, args.url_file)
    info(f"Discovered URL entries: {len(url_pairs)}")
    if not url_pairs:
        err("No URLs found in input path(s). Exiting."); sys.exit(1)

    reg = load_registry(args.registry)
    out_rows = list(reg.values())
    by_url = { (r.get("url") or "").strip().lower(): r for r in out_rows }

    # Add/update rows
    new_count = 0
    for url, src in url_pairs:
        key = (url or "").strip().lower()
        if key in by_url:
            r = by_url[key]
            if not r.get("topic") and args.topic:
                r["topic"] = args.topic
            continue
        rid = sha1(url)[:16]
        row = {
            "id": rid,
            "url": url,
            "topic": args.topic or os.path.basename(os.path.dirname(src)) or "",
            "repo": guess_repo(url),
            "access": guess_access(url),
            "priority": str(args.priority),
            "status": "NEW",
            "code": "",
            "reason": "",
            "last_checked_utc": "",
            "notes": "",
            "added_from": os.path.relpath(src, start=os.getcwd()) if os.path.isabs(src) or os.path.exists(src) else src
        }
        out_rows.append(row); by_url[key] = row; new_count += 1

    info(f"New added: {new_count} | Total in registry: {len(out_rows)}")
    if not out_rows:
        err("No sources to write into registry. Exiting."); sys.exit(1)

    # What to verify
    to_check = [r for r in out_rows if args.force or (r.get("status") in ("", "NEW", "ERR", "BAD", None))]
    if to_check:
        info(f"Verifying endpoints: {len(to_check)} (workers={args.workers})")
        done = 0; progress(done, len(to_check))
        log_path = os.path.join(os.path.dirname(args.registry) or ".", "logs", "source_finder.log")
        with ThreadPoolExecutor(max_workers=max(1, args.workers)) as ex:
            futs = { ex.submit(check_one, r): r for r in to_check }
            for fut in as_completed(futs):
                r = futs[fut]
                try:
                    res = fut.result()
                    r.update(res)
                except Exception as e:
                    r["status"], r["code"], r["reason"], r["last_checked_utc"] = "ERR", "0", f"worker: {e}", safe_now()
                done += 1
                progress(done, len(to_check))
                log_append(log_path, f"{r['status']}\t{r['code']}\t{r['url']}\t{r['reason']}")
    else:
        info("Nothing to verify (use --force to recheck).")

    # Sort & write
    out_rows.sort(key=lambda r: (int(r.get("priority") or 9), r.get("repo",""), r.get("url","")))
    fields = ["id","url","topic","repo","access","priority","status","code","reason","last_checked_utc","notes","added_from"]
    atomic_write_csv(args.registry, out_rows, fields)
    info(f"Registry saved: {args.registry}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        err("Interrupted by user.")
    except Exception as e:
        err(f"Fatal error: {e}")
        traceback.print_exc()
        sys.exit(2)
