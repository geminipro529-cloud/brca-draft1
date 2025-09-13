#!/usr/bin/env python3
# scripts/ingest_enrich_and_gap.py — Ingest → Enrich → Crosswalk-ready ledger + Gap Report
# Input:  data/00_urls/_index/sources.csv  (from make_sources_manifest.py)
#         data/01_raw/**                   (fetched files + optional PROVENANCE.json)
# Output: data/02_interim/enriched_sources.csv
#         data/00_urls/_index/sources_with_status.csv (atomic copy; original untouched unless --update-manifest 1)
#         data/00_urls/_index/todo_<topic>.txt (remaining URLs)
#         data/_validation/reports/gap_report_<YYYY-MM-DD>.md
# Notes:  Minimal deps (stdlib). Windows-safe. Idempotent. Progress text if Rich is missing.
import os, sys, csv, json, re, argparse, time, hashlib, concurrent.futures
from datetime import datetime

# --- optional pretty progress ---
try:
    from rich.progress import Progress, BarColumn, TimeElapsedColumn, TimeRemainingColumn, TextColumn
    RICH=True
except Exception:
    RICH=False

UTF8="utf-8"
CHUNK=1024*1024

TIER1_HINTS=("nih.gov","ncbi.nlm.nih.gov","gdc.cancer.gov","ebi.ac.uk","ega-","pdc.cancer.gov","ensembl.org","ucsc.edu","uniprot.org")
SKIP_DIRS=("_validation","logs","reports","_cache",".git",".venv")

def sha256(path):
    h=hashlib.sha256()
    with open(path,"rb",buffering=0) as f:
        while True:
            b=f.read(CHUNK)
            if not b: break
            h.update(b)
    return h.hexdigest()

def safe_read_json(path):
    try:
        with open(path,"r",encoding=UTF8) as f: return json.load(f)
    except Exception: return None

def write_atomic(path, rows_or_text, header=None):
    tmp=path+".tmp"
    if isinstance(rows_or_text, str):
        with open(tmp,"w",encoding=UTF8, newline="\n") as f: f.write(rows_or_text)
    else:
        with open(tmp,"w",encoding=UTF8, newline="") as f:
            w=csv.DictWriter(f, fieldnames=header); w.writeheader()
            for r in rows_or_text: w.writerow(r)
    os.replace(tmp, path)

def load_manifest(p):
    rows=[]; hdr=None
    with open(p,"r",encoding=UTF8, newline="") as f:
        rdr=csv.DictReader(f); hdr=rdr.fieldnames
        for r in rdr: rows.append(dict(r))
    return rows, hdr

def scan_files(root):
    files=[]
    for dp, dn, fn in os.walk(root):
        if any(part in SKIP_DIRS for part in dp.replace("\\","/").split("/")): continue
        for name in fn:
            if name.lower()=="provenance.json": continue
            path=os.path.join(dp,name)
            try:
                st=os.stat(path)
            except OSError:
                continue
            files.append({"path":path,"size":st.st_size,"mtime":int(st.st_mtime),"name":name,"parent":os.path.basename(dp)})
    return files

def find_provenance_for(path):
    prov=os.path.join(os.path.dirname(path),"PROVENANCE.json")
    j=safe_read_json(prov)
    if not j: return None
    return {"url": j.get("url") or j.get("source_url"), "checksum": j.get("checksum_sha256")}

def sha8(s): return hashlib.sha256((s or "").encode(UTF8,errors="ignore")).hexdigest()[:8]

def match_candidates(man_row, file_index, url_to_paths):
    url=man_row.get("url","")
    uid=man_row.get("uid","")
    leaf=(man_row.get("leaf","") or "").lower()
    s8=sha8(url)
    # 1) Provenance URL match
    if url in url_to_paths: return url_to_paths[url][:1]
    # 2) UID / sha8 hinted in filename
    hits=[f for f in file_index if (uid and uid in f["name"]) or (s8 and s8 in f["name"])]
    if hits: return hits[:1]
    # 3) Leaf + domain hint in parent dir
    dom=(man_row.get("domain","") or "").split(":")[0].lower()
    hits=[f for f in file_index if (leaf and leaf in f["name"].lower()) and (dom and dom in f["parent"].lower())]
    return hits[:1] if hits else []

def index_by_provenance(files, workers):
    url_to_paths={}
    # light parallel provenance scan
    def job(f):
        p=find_provenance_for(f["path"])
        return (f["path"], p)
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
        for path, prov in ex.map(job, files):
            if prov and prov.get("url"):
                url_to_paths.setdefault(prov["url"], []).append({"path":path})
    return url_to_paths

def main():
    ap=argparse.ArgumentParser(description="Ingest & enrich fetched files, update status, and produce Gap Report.")
    ap.add_argument("--manifest", default="data/00_urls/_index/sources.csv")
    ap.add_argument("--raw-root", default="data/01_raw")
    ap.add_argument("--out-dir",  default="data/02_interim")
    ap.add_argument("--update-manifest", type=int, default=0, help="1=write status copy beside manifest (atomic)")
    ap.add_argument("--hash", type=int, default=0, help="1=compute sha256 for matched files")
    ap.add_argument("--workers", type=int, default=8)
    args=ap.parse_args()

    os.environ.setdefault("PYTHONIOENCODING","utf-8")
    ts=date_str=datetime.now().strftime("%Y-%m-%d")

    if not os.path.exists(args.manifest):
        print(f"[ERROR] Manifest not found: {args.manifest}", file=sys.stderr); sys.exit(2)
    if not os.path.isdir(args.raw_root):
        print(f"[ERROR] Raw root not found: {args.raw_root}", file=sys.stderr); sys.exit(2)
    if not os.path.isdir(args.out_dir):
        os.makedirs(args.out_dir, exist_ok=True)

    # Load manifest
    manifest, hdr = load_manifest(args.manifest)
    needed_cols=["topic","url","domain","leaf","uid","added_at","status"]
    for c in needed_cols:
        if c not in (hdr or []):
            print(f"[ERROR] Manifest missing column: {c}", file=sys.stderr); sys.exit(2)

    # Scan raw files
    files=scan_files(args.raw_root)

    # Optional progress
    bar=None
    if RICH:
        bar=Progress(TextColumn("[bold blue]Ingest[/]"), BarColumn(), TimeElapsedColumn(), TimeRemainingColumn(), transient=False)
        bar.start()
        t_scan=bar.add_task("index", total=len(files))

    # Build provenance index
    url_to_paths=index_by_provenance(files, args.workers)
    if RICH: bar.update(t_scan, completed=len(files))

    # Enrich & match
    enriched=[]
    missing_by_topic={}
    matched=0
    for r in manifest:
        cand=match_candidates(r, files, url_to_paths)
        if cand:
            matched+=1
            path=cand[0]["path"]
            item={
                "topic": r.get("topic",""),
                "url": r.get("url",""),
                "domain": r.get("domain",""),
                "uid": r.get("uid",""),
                "path": path,
                "size": os.path.getsize(path),
                "mtime": int(os.path.getmtime(path)),
                "status": "fetched",
                "fetched_at": int(time.time()),
                "checksum_sha256": ""
            }
            if args.hash:
                try: item["checksum_sha256"]=sha256(path)
                except Exception: item["checksum_sha256"]=""
            enriched.append(item)
        else:
            missing_by_topic.setdefault(r.get("topic","other"), []).append(r.get("url",""))

    if RICH: bar.stop()

    # Write enriched_sources.csv
    out_csv=os.path.join(args.out_dir,"enriched_sources.csv")
    write_atomic(out_csv, enriched, header=["topic","url","domain","uid","path","size","mtime","status","fetched_at","checksum_sha256"])

    # Rebuild TODOs per topic + prioritized “important” first
    todo_dir=os.path.dirname(args.manifest)
    for topic, urls in missing_by_topic.items():
        # rank: Tier1 domains first, then frequency
        tier1=[u for u in urls if any(h in u.lower() for h in TIER1_HINTS)]
        other=[u for u in urls if u not in tier1]
        text="\n".join(tier1 + other) + ("\n" if urls else "")
        write_atomic(os.path.join(todo_dir, f"todo_{topic}.txt"), text)

    # Optionally write a manifest copy with status
    if args.update_manifest:
        # map: url -> fetched?
        fetched_urls=set([e["url"] for e in enriched])
        updated=[]
        for r in manifest:
            u=r.get("url","")
            r=dict(r)
            r["status"]="fetched" if u in fetched_urls else (r.get("status") or "queued")
            updated.append(r)
        man_out=os.path.join(todo_dir,"sources_with_status.csv")
        write_atomic(man_out, updated, header=list(updated[0].keys()) if updated else hdr)

    # Gap Report
    reports=os.path.join("data","_validation","reports")
    os.makedirs(reports, exist_ok=True)
    gap_md=os.path.join(reports, f"gap_report_{date_str}.md")

    # domain summary of missing
    domain_counts={}
    total_missing=sum(len(v) for v in missing_by_topic.values())
    for urls in missing_by_topic.values():
        for u in urls:
            m=re.search(r"https?://([^/]+)/", u+"/")
            dom=(m.group(1).lower() if m else "unknown")
            domain_counts[dom]=domain_counts.get(dom,0)+1
    top_domains=sorted(domain_counts.items(), key=lambda kv:(-kv[1], kv[0]))[:15]

    rep=[
        f"# Gap Report — {date_str}",
        f"- Enriched rows (matched files): {len(enriched):,}",
        f"- Missing URLs total: {total_missing:,}",
        "",
        "## Top missing domains",
        *(f"- {d}: {n}" for d,n in top_domains),
        "",
        "## Missing by topic",
    ]
    for t, urls in sorted(missing_by_topic.items(), key=lambda kv: (-len(kv[1]), kv[0])):
        rep.append(f"### {t}  (missing: {len(urls)})")
        # show Tier1 first for actionability
        tier1=[u for u in urls if any(h in u.lower() for h in TIER1_HINTS)]
        other=[u for u in urls if u not in tier1]
        sample=tier1[:20] + other[:20]  # keep report concise
        rep += [*(f"- {u}" for u in sample)]
        if len(urls)>len(sample): rep.append(f"- … and {len(urls)-len(sample)} more")
        rep.append("")
    write_atomic(gap_md, "\n".join(rep))

    print(f"[OK] Enriched → {out_csv}")
    print(f"[OK] TODOs → {todo_dir}/todo_<topic>.txt")
    print(f"[OK] Gap report → {gap_md}")

if __name__=="__main__":
    main()
