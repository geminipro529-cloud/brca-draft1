#!/usr/bin/env python3
"""
Super Fetcher: NANOMEDICINE + Breast Cancer datasets

- Integrates Tier 1 + Tier 2 sources automatically:
  GEO, PRIDE, Metabolomics Workbench, ClinicalTrials.gov, CananoLab,
  Reactome, KEGG, WikiPathways, PubChem, NanoCommons
- Also reads all *.txt files under data/00_urls/nanomedicine/
- Deduplicates and merges URLs
- Parallel downloads (--workers N)
- Incremental logging (safe resume)
- Supports --resume (retry failed only)
- Progress bar + summary report
"""

import os, re, csv, time, requests, argparse
from pathlib import Path
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import xml.etree.ElementTree as ET

# ---------- config ----------
URL_DIR = Path(r"D:\breast_cancer_project_root\data\00_urls\nanomedicine")
OUT_DIR = Path(r"D:\breast_cancer_project_root\data\01_raw\zipped\NANOMEDICINE")
INDEX_FILE = OUT_DIR / "download_index.csv"
FAILED_FILE = OUT_DIR / "failed_downloads.csv"
TIMEOUT = 30
RETRIES = 2

URL_REGEX = re.compile(r"https?://[^\s]+|ftp://[^\s]+")

MIRROR_MAP = {
    "ftp://ftp.ncbi.nlm.nih.gov": "https://ftp.ncbi.nlm.nih.gov",
    "ftp://ftp.pride.ebi.ac.uk": "https://www.ebi.ac.uk/pride",
}

# ---------- helpers ----------
def normalize_url(url: str) -> str:
    for k, v in MIRROR_MAP.items():
        if url.startswith(k):
            return url.replace(k, v)
    return url

def filename_from_url(url: str) -> str:
    parsed = urlparse(url)
    fname = os.path.basename(parsed.path)
    return fname if fname else parsed.netloc.replace(".", "_")

def download(url: str, outdir: Path) -> tuple[str, str, str]:
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

def append_row(csv_path: Path, header: list[str], row: list[str]):
    exists = csv_path.exists()
    with open(csv_path, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if not exists:
            w.writerow(header)
        w.writerow(row)

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

# ---------- crawlers ----------
def crawl_geo(query="nanomedicine breast cancer", retmax=20) -> list[str]:
    base = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"
    search_url = f"{base}esearch.fcgi?db=gds&term={query}&retmax={retmax}"
    urls = []
    try:
        r = requests.get(search_url, timeout=TIMEOUT)
        root = ET.fromstring(r.text)
        ids = [id_elem.text for id_elem in root.findall(".//Id")]
        for gse_id in ids:
            supp_url = f"https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=GSE{gse_id}"
            urls.append(supp_url)
    except Exception as e:
        print(f"⚠️ GEO crawl failed: {e}")
    return urls

def crawl_pride(query="nanoparticle breast cancer") -> list[str]:
    api = f"https://www.ebi.ac.uk/pride/ws/archive/project/list?keyword={query}&pageSize=20&page=0"
    urls = []
    try:
        r = requests.get(api, timeout=TIMEOUT)
        if r.status_code == 200:
            data = r.json()
            for proj in data.get("list", []):
                if "accession" in proj:
                    urls.append(f"https://www.ebi.ac.uk/pride/archive/projects/{proj['accession']}")
    except Exception as e:
        print(f"⚠️ PRIDE crawl failed: {e}")
    return urls

def crawl_mwb(query="nanoparticle breast cancer") -> list[str]:
    api = f"https://www.metabolomicsworkbench.org/rest/study/search/{query}"
    urls = []
    try:
        r = requests.get(api, timeout=TIMEOUT)
        if r.status_code == 200:
            studies = r.text.strip().splitlines()
            for sid in studies:
                if sid.startswith("ST"):
                    urls.append(f"https://www.metabolomicsworkbench.org/data/DRCCMetadata.php?Mode=Study&StudyID={sid}")
    except Exception as e:
        print(f"⚠️ MWB crawl failed: {e}")
    return urls

def crawl_clinicaltrials(query="nanoparticle breast cancer") -> list[str]:
    api = f"https://clinicaltrials.gov/api/v2/studies?query.term={query}&format=json&pageSize=20"
    urls = []
    try:
        r = requests.get(api, timeout=TIMEOUT)
        if r.status_code == 200:
            studies = r.json().get("studies", [])
            for st in studies:
                sid = st.get("protocolSection", {}).get("identificationModule", {}).get("nctId")
                if sid:
                    urls.append(f"https://clinicaltrials.gov/study/{sid}")
    except Exception as e:
        print(f"⚠️ ClinicalTrials crawl failed: {e}")
    return urls

def crawl_cananolab(query="breast cancer") -> list[str]:
    # Simplified placeholder (real API is SOAP/REST hybrid)
    urls = [f"https://cananolab.cancer.gov/search?keyword={query}"]
    return urls

def crawl_pathways() -> list[str]:
    urls = [
        "https://reactome.org/download/current/ReactomePathways.gmt.zip",
        "https://reactome.org/download/current/ReactomePathways.txt",
        "https://data.wikipathways.org/current/gmt/",
        "https://www.kegg.jp/pathway/hsa05224",
    ]
    return urls

def crawl_pubchem_drugs() -> list[str]:
    urls = [
        "https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/name/paclitaxel/JSON",
        "https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/name/doxorubicin/JSON",
    ]
    return urls

def crawl_nanocommons() -> list[str]:
    return ["https://enanomapper.adma.ai/datamodel/"]

# ---------- main ----------
def main():
    ap = argparse.ArgumentParser(description="Super Fetcher for Nanomedicine")
    ap.add_argument("--resume", action="store_true", help="retry only failed URLs")
    ap.add_argument("--workers", type=int, default=4, help="parallel downloads")
    args = ap.parse_args()

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    urls = set()
    if args.resume:
        urls = load_csv_urls(FAILED_FILE, "url")
        print(f"Resuming {len(urls)} failed URLs")
    else:
        if URL_DIR.exists():
            urls.update(load_urls_from_dir(URL_DIR))
        urls.update(crawl_geo())
        urls.update(crawl_pride())
        urls.update(crawl_mwb())
        urls.update(crawl_clinicaltrials())
        urls.update(crawl_cananolab())
        urls.update(crawl_pathways())
        urls.update(crawl_pubchem_drugs())
        urls.update(crawl_nanocommons())

    already_ok = load_csv_urls(INDEX_FILE, "url")
    urls = [u for u in urls if u not in already_ok]

    if not urls:
        print("✅ Nothing new to fetch.")
        return

    print(f"Processing {len(urls)} URLs with {args.workers} workers")

    counts = {"ok": 0, "cached": 0, "fail": 0}
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

    print("\n=== Summary ===")
    print(f"✅ OK      : {counts['ok']}")
    print(f"🟡 Cached  : {counts['cached']}")
    print(f"❌ Failed  : {counts['fail']}")
    print(f"Logs: {INDEX_FILE}, {FAILED_FILE}")

if __name__ == "__main__":
    main()
