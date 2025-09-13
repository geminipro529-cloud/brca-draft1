#!/usr/bin/env python3
# ingest_manual.py — import manually downloaded files into canonical layout with checksums + registry
# Windows-safe, idempotent. Minimal deps: rich
import os, sys, re, csv, time, argparse, hashlib, shutil, random
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import os, glob
from pathlib import Path

try:
    from rich.console import Console
    from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn
except ImportError:
    print("[ERR] Please: pip install rich", file=sys.stderr); sys.exit(1)

console = Console()
REG_CSV = Path("data/_validation/manual_registry.csv")
DONE_LOG = Path("data/_validation/completed_downloads.log")

ACC_PATTERNS = [
    r"(GSE\d+)", r"(SRR\d+)", r"(ERR\d+)", r"(PRJ[EDNA][A-Z0-9]+)",
    r"(TCGA-[A-Z]{2,}-[A-Z]{2,})", r"(E-\w{2,5}-\d+)", r"(PXD\d+)"
]

def sha256_file(p: Path, chunk=1024*1024) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        while True:
            b = f.read(chunk)
            if not b: break
            h.update(b)
    return h.hexdigest()

def safe_copy(src: Path, dst: Path, attempts=3):
    tmp = dst.with_suffix(dst.suffix + ".tmp")
    for i in range(attempts):
        try:
            dst.parent.mkdir(parents=True, exist_ok=True)
            with src.open("rb", buffering=0) as fi, tmp.open("wb", buffering=0) as fo:
                shutil.copyfileobj(fi, fo, length=1024*1024)
            os.replace(tmp, dst)  # atomic swap
            return
        except Exception as e:
            time.sleep(0.3 + random.random()*0.7)
            if i == attempts-1:
                raise e
        finally:
            if tmp.exists():
                try: tmp.unlink()
                except: pass

def guess_dataset(name: str, override: str|None) -> str:
    if override: return override
    n = name.lower()
    if "brca" in n and ("nano" in n or "particle" in n): return "brca_nanomedicine"
    if "brca" in n or "breast" in n: return "BRCA"
    if "nano" in n or "particle" in n or "exosome" in n or "liposome" in n: return "nanomedicine"
    return "misc"

def guess_accession(name: str) -> str:
    for pat in ACC_PATTERNS:
        m = re.search(pat, name, flags=re.I)
        if m: return m.group(1)
    return Path(name).stem  # fallback by filename stem

def already_registered(sha: str) -> bool:
    if REG_CSV.exists():
        with REG_CSV.open("r", encoding="utf-8", newline="") as f:
            for row in csv.DictReader(f):
                if row.get("sha256")==sha: return True
    return False

def write_registry(rec: dict):
    REG_CSV.parent.mkdir(parents=True, exist_ok=True)
    write_header = not REG_CSV.exists()
    with REG_CSV.open("a", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=[
            "dataset","accession","dest_path","bytes","sha256","source","notes","imported_at"
        ])
        if write_header: w.writeheader()
        w.writerow(rec)
    # also mark as completed for fetchers that read this log
    with DONE_LOG.open("a", encoding="utf-8") as f:
        f.write(f"{rec['sha256']}\t{rec['dest_path']}\t{rec.get('source','')}\n")

def sidecars(dest: Path, sha: str, source: str|None, notes: str|None):
    # .sha256 and .source.txt next to the file
    (dest.parent / (dest.name + ".sha256")).write_text(sha + "\n", encoding="utf-8")
    meta = []
    if source: meta.append(f"source: {source}")
    if notes:  meta.append(f"notes: {notes}")
    meta.append(f"sha256: {sha}")
    meta.append(f"imported_at: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    (dest.parent / (dest.name + ".source.txt")).write_text("\n".join(meta) + "\n", encoding="utf-8")

from pathlib import Path

def collect_inputs(paths):
    out = []
    for p in paths:
        p = Path(p)
        if p.is_file():
            out.append(p)
        elif p.is_dir():
            # recurse into all subfiles
            out.extend([x for x in p.rglob("*") if x.is_file()])
    return out


def main():
    ap = argparse.ArgumentParser(description="Ingest manually downloaded files into data/01_raw/* with registry.")
    ap.add_argument("--inputs", nargs="+", required=True, help="Files/dirs/globs to ingest (e.g. D:\\Downloads\\*.zip)")
    ap.add_argument("--dataset", choices=["BRCA","brca_nanomedicine","nanomedicine","misc"], help="Override dataset bucket")
    ap.add_argument("--dest-root", default="data/01_raw", help="Root of raw storage")
    ap.add_argument("--source", help="URL/DOI or brief source label")
    ap.add_argument("--notes", help="Optional notes")
    ap.add_argument("--mode", choices=["move","copy"], default="move", help="Move (default) or copy from inputs")
    ap.add_argument("--workers", type=int, default=8, help="Parallel hashing/ingest workers")
    args = ap.parse_args()

    inputs = collect_inputs([Path(x) for x in args.inputs])
    if not inputs:
        console.print("[red]No files found to ingest.[/red]"); sys.exit(2)

    dest_root = Path(args.dest_root)
    dest_root.mkdir(parents=True, exist_ok=True)

    def handle_one(src: Path):
        try:
            dataset = guess_dataset(src.name, args.dataset)
            acc = guess_accession(src.name)
            dest_dir = dest_root / dataset / acc
            dest = dest_dir / src.name

            sha = sha256_file(src)
            if already_registered(sha) and dest.exists():
                return f"[SKIP] {src.name} (already registered)"

            if args.mode == "copy":
                safe_copy(src, dest)
            else:
                dest_dir.mkdir(parents=True, exist_ok=True)
                try:
                    os.replace(src, dest)  # fast move within disk
                except OSError:
                    # cross-device: copy then remove
                    safe_copy(src, dest)
                    try: src.unlink()
                    except: pass

            sidecars(dest, sha, args.source, args.notes)
            write_registry({
                "dataset": dataset,
                "accession": acc,
                "dest_path": str(dest),
                "bytes": dest.stat().st_size,
                "sha256": sha,
                "source": args.source or "",
                "notes":  args.notes or "",
                "imported_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            })
            return f"[OK] {src.name} → {dest}"
        except Exception as e:
            return f"[ERR] {src}: {e}"

    with Progress(TextColumn("[progress.description]{task.description}"),
                  BarColumn(), TextColumn("{task.completed}/{task.total}"),
                  TimeRemainingColumn(), console=console) as progress:
        task = progress.add_task("[cyan]Ingesting manual files...", total=len(inputs))
        msgs=[]
        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            futs = {ex.submit(handle_one, p): p for p in inputs}
            for fut in as_completed(futs):
                progress.update(task, advance=1)
                msgs.append(fut.result())

    for m in msgs: console.print(m)
    console.print(f"[green]Manual ingest complete. Registry → {REG_CSV}[/green]")

if __name__ == "__main__":
    main()
