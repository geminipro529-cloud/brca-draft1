#!/usr/bin/env python3
# scripts/make_sources_manifest.py — build canonical URL manifest + per-topic TODO shards
# Usage (PowerShell from project root):
#   (.venv) python scripts/make_sources_manifest.py --in data/00_urls --out data/00_urls/_index --topics brca brca_nanomedicine nanomedicine other --workers 8

from __future__ import annotations
import os, sys, csv, re, time, argparse, hashlib
from urllib.parse import urlparse
from pathlib import Path

# Optional pretty progress (no hard dependency)
try:
    from rich.progress import Progress, BarColumn, TimeElapsedColumn, TimeRemainingColumn, TextColumn
    RICH=True
except Exception:
    RICH=False

UTF8="utf-8"

def norm_url(u:str) -> str:
    if not u: return ""
    u = u.strip().replace("\\","/")
    u = re.sub(r"\s+", "", u)
    # strip trailing punctuation that often slips in from copy/paste
    u = u.rstrip("),.;]")
    return u

def slugify(s:str) -> str:
    s=re.sub(r"[^A-Za-z0-9._-]+","-", s).strip("-")
    return s[:120] if len(s)>120 else s

def sha8(s:str) -> str:
    return hashlib.sha256((s or "").encode(UTF8, errors="ignore")).hexdigest()[:8]

def ok_dir(p:Path):
    p.mkdir(parents=True, exist_ok=True)

def atomic_write_text(path:Path, text:str):
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding=UTF8, newline="\n")
    tmp.replace(path)

def atomic_write_csv(path:Path, rows:list[dict], header:list[str]):
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding=UTF8, newline="") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()
        for r in rows:
            w.writerow({k:r.get(k,"") for k in header})
    tmp.replace(path)

def scan_url_files(root:Path):
    """Yield (topic, file_path) for .txt/.csv/.tsv under root; topic = first subdir."""
    exts={".txt",".csv",".tsv"}
    for dirpath, _, files in os.walk(root):
        for name in files:
            if os.path.splitext(name)[1].lower() not in exts: continue
            p = Path(dirpath)/name
            rel = p.relative_to(root)
            topic = (rel.parts[0].lower() if len(rel.parts)>1 else "other")
            yield topic, p

def iter_urls_from_file(p:Path):
    """Yield (url, line_no) from txt/csv/tsv; ignores comments/blank lines."""
    ext = p.suffix.lower()
    if ext == ".txt":
        with p.open("r", encoding=UTF8, errors="ignore") as f:
            for i, line in enumerate(f, 1):
                s=line.strip()
                if not s or s.startswith("#"): continue
                # support "URL, comment" style lines
                s = s.split()[0]
                if s.startswith("http"): yield s, i
    else:
        delim = "\t" if ext==".tsv" else ","
        with p.open("r", encoding=UTF8, errors="ignore", newline="") as f:
            rdr = csv.reader(f, delimiter=delim)
            for i, row in enumerate(rdr, 1):
                for cell in row:
                    c = (cell or "").strip()
                    if c.startswith("http"):
                        yield c, i
                        break

def load_existing_manifest(path:Path):
    """Return (rows, seen_keys) preserving prior status; key = (topic,url)."""
    rows=[]; seen=set()
    if not path.exists(): return rows, seen
    try:
        with path.open("r", encoding=UTF8, newline="") as f:
            rdr=csv.DictReader(f)
            for r in rdr:
                topic=(r.get("topic") or "other").lower()
                url=norm_url(r.get("url") or "")
                if not url: continue
                rows.append(dict(r))
                seen.add((topic,url))
    except Exception:
        # if corrupt, ignore existing to avoid blocking; user can inspect file
        pass
    return rows, seen

def build_row(topic:str, url:str, source_file:Path, line_no:int):
    u = norm_url(url)
    parts = urlparse(u)
    domain = (parts.netloc or "").lower()
    leaf = os.path.basename(parts.path) or "index"
    ext_hint = os.path.splitext(leaf)[1].lower().lstrip(".")
    uid = f"{topic}__{domain}__{slugify(leaf)}__{sha8(u)}"
    return {
        "topic": topic,
        "url": u,
        "domain": domain,
        "leaf": leaf,
        "ext_hint": ext_hint,
        "source_file": str(source_file),
        "source_line": line_no,
        "uid": uid,
        "added_at": int(time.time()),
        "status": "queued"
    }

def main():
    ap = argparse.ArgumentParser(description="Compile de-duped URL manifest + per-topic TODOs from data/00_urls/**")
    ap.add_argument("--in",  dest="in_dir",  required=True, help="Input root (e.g., data/00_urls)")
    ap.add_argument("--out", dest="out_dir", required=True, help="Output dir (e.g., data/00_urls/_index)")
    ap.add_argument("--topics", nargs="+", default=["brca","brca_nanomedicine","nanomedicine","other"])
    ap.add_argument("--workers", type=int, default=8, help="Reserved for future parallel checks")
    ap.add_argument("--rebuild", type=int, default=0, help="1 = ignore existing manifest and rebuild fresh")
    args = ap.parse_args()

    in_root = Path(args.in_dir).resolve()
    out_dir = Path(args.out_dir).resolve()
    if not in_root.exists():
        print(f"[ERROR] Input folder not found: {in_root}", file=sys.stderr); sys.exit(2)
    ok_dir(out_dir)

    manifest_path = out_dir/"sources.csv"
    todo_paths = {t: out_dir/f"todo_{t}.txt" for t in args.topics}

    # Load existing manifest to be truly idempotent
    rows, seen = ([], set()) if args.rebuild else load_existing_manifest(manifest_path)

    url_files = list(scan_url_files(in_root))
    added = 0
    if RICH:
        cols=[TextColumn("[bold blue]Indexing[/]"), BarColumn(), TimeElapsedColumn(), TimeRemainingColumn()]
        with Progress(*cols, transient=False) as prog:
            task = prog.add_task("scan", total=len(url_files))
            for topic, path in url_files:
                if topic not in args.topics: topic="other"
                for u, ln in iter_urls_from_file(path):
                    u = norm_url(u)
                    if not u.startswith("http"): continue
                    key=(topic,u)
                    if key in seen: continue
                    rows.append(build_row(topic, u, path, ln))
                    seen.add(key)
                    added += 1
                prog.advance(task)
    else:
        for topic, path in url_files:
            if topic not in args.topics: topic="other"
            for u, ln in iter_urls_from_file(path):
                u = norm_url(u)
                if not u.startswith("http"): continue
                key=(topic,u)
                if key in seen: continue
                rows.append(build_row(topic, u, path, ln))
                seen.add(key)
                added += 1

    # Write manifest (atomic). Preserve existing order; new rows append at end.
    header=["topic","url","domain","leaf","ext_hint","source_file","source_line","uid","added_at","status"]
    atomic_write_csv(manifest_path, rows, header)

    # Build TODO per topic from manifest status != fetched
    pending_by_topic = {t: [] for t in args.topics}
    for r in rows:
        t=(r.get("topic") or "other").lower()
        if t not in pending_by_topic: continue
        st=(r.get("status") or "queued").lower()
        if st != "fetched":
            pending_by_topic[t].append(r.get("url") or "")

    # Write TODOs (atomic)
    for t, p in todo_paths.items():
        urls = [u for u in pending_by_topic.get(t, []) if u]
        atomic_write_text(p, "\n".join(urls) + ("\n" if urls else ""))

    print(f"[OK] Manifest → {manifest_path}  (total rows={len(rows)}, added now={added})")
    for t, p in todo_paths.items():
        try:
            n = sum(1 for _ in open(p, encoding=UTF8))
        except Exception:
            n = 0
        print(f"[OK] TODO[{t}] → {p}  (urls={n})")

if __name__ == "__main__":
    main()
