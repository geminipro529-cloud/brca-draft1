#!/usr/bin/env python3
# scripts/clean_and_normalize.py — normalize names, enforce layout, dedupe, and (optionally) apply moves
# Windows + Python 3.11 safe. Minimal deps: rich. Outputs live in data/_validation/.
import os, sys, re, csv, time, argparse, hashlib, shutil, random
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    from rich.console import Console
    from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn
except ImportError:
    print("[ERR] Please install rich: pip install rich", file=sys.stderr); sys.exit(1)

console = Console()

ACC_PATTERNS = [
    r"(GSE\d+)", r"(GSEx\d+)", r"(SRR\d+)", r"(ERR\d+)", r"(PRJ[EDNA][A-Z0-9]+)",
    r"(TCGA-[A-Z]{2,}-[A-Z]{2,})", r"(E-\w{2,5}-\d+)", r"(PXD\d+)"
]
DATASET_NAMES = {"BRCA","brca_nanomedicine","nanomedicine","misc"}

def sha256_file(p: Path, chunk=1024*1024) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for b in iter(lambda: f.read(chunk), b""):
            h.update(b)
    return h.hexdigest()

def normalize_name(name: str) -> str:
    base, ext = os.path.splitext(name)
    base = re.sub(r"\s+", "_", base.strip().lower())
    base = re.sub(r"[^a-z0-9._-]", "_", base)
    base = re.sub(r"_+", "_", base).strip("_")
    return base + ext.lower()

def guess_accession(path: Path) -> str:
    name = path.name
    for pat in ACC_PATTERNS:
        m = re.search(pat, name, flags=re.I)
        if m: return m.group(1)
    # try parent folders
    for part in reversed(path.parts):
        for pat in ACC_PATTERNS:
            m = re.search(pat, part, flags=re.I)
            if m: return m.group(1)
    return Path(name).stem  # fallback

def guess_dataset(path: Path, explicit: str|None, root: Path) -> str:
    if explicit: return explicit
    # if already under a known dataset at top-level, keep it
    rel = path.relative_to(root)
    if rel.parts:
        top = rel.parts[0]
        if top in DATASET_NAMES: return top
        if "brca" in top.lower() and ("nano" in top.lower() or "particle" in top.lower()):
            return "brca_nanomedicine"
        if top.lower().startswith("brca") or "breast" in top.lower(): return "BRCA"
        if any(k in top.lower() for k in ("nano","particle","exosome","liposome")): return "nanomedicine"
    # infer from filename
    n = path.name.lower()
    if "brca" in n and ("nano" in n or "particle" in n): return "brca_nanomedicine"
    if "brca" in n or "breast" in n: return "BRCA"
    if any(k in n for k in ("nano","particle","exosome","liposome")): return "nanomedicine"
    return "misc"

def safe_move(src: Path, dst: Path, attempts=3):
    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.with_suffix(dst.suffix + ".tmp")
    for i in range(attempts):
        try:
            # prefer atomic rename if same device
            try:
                os.replace(src, tmp)
            except OSError:
                # cross-device: copy then remove
                with src.open("rb", buffering=0) as fi, tmp.open("wb", buffering=0) as fo:
                    shutil.copyfileobj(fi, fo, length=1024*1024)
                try: src.unlink()
                except: pass
            os.replace(tmp, dst)
            return
        except Exception:
            time.sleep(0.3 + random.random()*0.7)
        finally:
            if tmp.exists():
                try: tmp.unlink()
                except: pass
    raise OSError(f"Failed to move {src} -> {dst}")

def build_plan(root: Path, dataset_override: str|None, quarantine: Path, rename: bool, workers: int):
    files = [p for p in root.rglob("*") if p.is_file() and "_validation" not in p.parts and "_dupes" not in p.parts]
    plan, log_rows = [], []
    sha_seen = {}  # sha -> canonical dest

    def inspect(p: Path):
        try:
            sha = sha256_file(p)
        except Exception as e:
            return {"type":"error","path":str(p),"error":str(e)}
        ds = guess_dataset(p, dataset_override, root)
        acc = guess_accession(p)
        norm = normalize_name(p.name) if rename else p.name
        dst = root / ds / acc / norm
        action = "noop" if p.resolve() == dst.resolve() else "move"
        return {"type":"ok","src":str(p), "dst":str(dst), "sha":sha, "dataset":ds, "accession":acc, "action":action}

    with Progress(TextColumn("[progress.description]{task.description}"),
                  BarColumn(), TextColumn("{task.completed}/{task.total}"),
                  TimeRemainingColumn(), console=console) as progress:
        task = progress.add_task(f"[cyan]Hashing & planning {root}...", total=len(files))
        results=[]
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futs = {ex.submit(inspect, p): p for p in files}
            for fut in as_completed(futs):
                progress.update(task, advance=1)
                results.append(fut.result())

    for r in results:
        if r.get("type")!="ok":
            log_rows.append(["error", r.get("path"), "", "", "", "", r.get("error")])
            continue
        sha = r["sha"]
        if sha in sha_seen and Path(r["src"]) != Path(sha_seen[sha]):
            # duplicate → quarantine
            qdst = quarantine / sha / Path(r["src"]).name
            plan.append(["quarantine", r["src"], str(qdst), sha, r["dataset"], r["accession"]])
            log_rows.append(["duplicate", r["src"], str(qdst), sha, r["dataset"], r["accession"], "quarantine"])
        else:
            sha_seen[sha] = r["dst"]
            if r["action"]=="move":
                plan.append(["move", r["src"], r["dst"], sha, r["dataset"], r["accession"]])
                log_rows.append(["move", r["src"], r["dst"], sha, r["dataset"], r["accession"], ""])
            else:
                log_rows.append(["keep", r["src"], r["dst"], sha, r["dataset"], r["accession"], ""])

    return plan, log_rows

def apply_plan(plan, workers: int):
    def do(row):
        action, src, dst = row[0], Path(row[1]), Path(row[2])
        try:
            if action=="move":
                safe_move(src, dst); return f"[OK] {src.name} → {dst}"
            elif action=="quarantine":
                safe_move(Path(src), Path(dst)); return f"[DUPE→Q] {Path(src).name}"
            else:
                return f"[SKIP] {src}"
        except Exception as e:
            return f"[ERR] {src} → {dst}: {e}"

    with Progress(TextColumn("[progress.description]{task.description}"),
                  BarColumn(), TextColumn("{task.completed}/{task.total}"),
                  TimeRemainingColumn(), console=console) as progress:
        task = progress.add_task("[cyan]Applying plan...", total=len(plan))
        msgs=[]
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futs = {ex.submit(do, row): row for row in plan}
            for fut in as_completed(futs):
                progress.update(task, advance=1)
                msgs.append(fut.result())
    for m in msgs: console.print(m)

def main():
    ap = argparse.ArgumentParser(description="Clean/normalize raw files into {dataset}/{accession}/file with dedupe.")
    ap.add_argument("--root", default="data/01_raw")
    ap.add_argument("--dataset", choices=["BRCA","brca_nanomedicine","nanomedicine","misc"], help="Force dataset bucket for EVERYTHING (usually leave unset).")
    ap.add_argument("--no-rename", action="store_true", help="Do not normalize file names")
    ap.add_argument("--workers", type=int, default=12)
    ap.add_argument("--apply", action="store_true", help="Apply moves; otherwise dry-run only")
    ap.add_argument("--out-plan", default="data/_validation/move_plan.csv")
    ap.add_argument("--out-log",  default="data/_validation/clean_log.csv")
    ap.add_argument("--quarantine", default="data/01_raw/_dupes")
    args = ap.parse_args()

    root = Path(args.root); root.mkdir(parents=True, exist_ok=True)
    Path(args.out_plan).parent.mkdir(parents=True, exist_ok=True)
    quarantine = Path(args.quarantine)

    plan, rows = build_plan(root, args.dataset, quarantine, not args.no_rename, args.workers)

    # Write CSVs (atomic)
    with open(args.out_plan, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f); w.writerow(["action","src","dst","sha256","dataset","accession"]); w.writerows(plan)
    with open(args.out_log, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f); w.writerow(["result","src","dst","sha256","dataset","accession","note"]); w.writerows(rows)

    console.print(f"[green]Dry-run complete.[/green] Plan → {args.out_plan} ; Log → {args.out_log}")
    console.print(f"[yellow]Moves NOT applied.[/yellow] Use --apply to execute.")

    if args.apply and plan:
        apply_plan(plan, args.workers)
        console.print(f"[green]Applied {len(plan)} actions. Re-run audit next.[/green]")

if __name__ == "__main__":
    main()
