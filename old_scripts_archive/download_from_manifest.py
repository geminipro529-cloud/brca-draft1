#!/usr/bin/env python3
# download_from_manifest.py — fetch from CSV/TXT manifests or a directory of them
# Supports:
#   - CSV with columns: category,name,url,out_subdir,mode,notes
#   - TXT with lines of URLs and '#' comments; section headers like '# [PROTEOMICS]'
#   - A directory path containing any mix of the above (recurses)
from __future__ import annotations
import os, sys, csv, time, argparse
from pathlib import Path
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Iterable, Dict, List, Tuple

# deps
try:
    import requests
except ImportError:
    print("[ERROR] Missing 'requests'. Install: pip install requests", file=sys.stderr); sys.exit(1)
try:
    from rich.console import Console
    from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn
except ImportError:
    print("[ERROR] Missing 'rich'. Install: pip install rich", file=sys.stderr); sys.exit(1)

# logger (central log.txt)
try:
    from utils.logger_setup import get_logger
except Exception:
    import logging
    def get_logger(name: str):
        lg = logging.getLogger(name)
        if not lg.handlers:
            lg.setLevel(logging.INFO)
            Path("logs").mkdir(parents=True, exist_ok=True)
            fh = logging.FileHandler(Path("logs")/"log.txt", encoding="utf-8")
            fmt = logging.Formatter("[%(asctime)s] [%(name)s] %(levelname)s: %(message)s", "%Y-%m-%d %H:%M:%S")
            fh.setFormatter(fmt); lg.addHandler(fh)
        return lg

logger = get_logger("download_from_manifest")
console = Console()

# ---------- utils ----------
def ensure_dir(p: Path): p.mkdir(parents=True, exist_ok=True)
def guess_filename(url: str, name_hint: str) -> str:
    b = os.path.basename(urlparse(url).path)
    if b and b not in {"/", ""}: return b
    safe = "".join(ch if ch.isalnum() or ch in ("-","_") else "_" for ch in (name_hint or "file"))
    return safe or "file"
def looks_like_portal(headers: dict) -> bool:
    ct = (headers.get("content-type") or "").lower()
    return "text/html" in ct or "text/xhtml" in ct

def head_content_length(url: str, timeout: Tuple[int,int]) -> int | None:
    try:
        h = requests.head(url, allow_redirects=True, timeout=timeout)
        if 200 <= h.status_code < 400:
            cl = h.headers.get("content-length")
            return int(cl) if cl and cl.isdigit() else None
    except Exception:
        return None
    return None

# ---------- manifest readers ----------
REQUIRED_COLS = {"category","name","url","out_subdir","mode","notes"}

def read_csv_manifest(path: Path) -> List[Dict[str,str]]:
    with path.open("r", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        cols = {c.strip() for c in (rdr.fieldnames or [])}
        missing = REQUIRED_COLS - cols
        if missing:
            raise ValueError(f"{path}: Manifest missing columns: {sorted(missing)}")
        rows = []
        for row in rdr:
            if not (row.get("url") or "").strip(): continue
            rows.append({k: (row.get(k) or "").strip() for k in REQUIRED_COLS})
        return rows

def read_txt_manifest(path: Path) -> List[Dict[str,str]]:
    """TXT format: URLs per line; '#' for comments; optional section headers '# [SECTION]' or '# SECTION'."""
    rows: List[Dict[str,str]] = []
    current_cat = "TXT"
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for raw in f:
            line = raw.strip()
            if not line: continue
            if line.startswith("#"):
                # Header like '# [PROTEOMICS]' or '# PROTEOMICS'
                hdr = line.lstrip("#").strip()
                if hdr.startswith("[") and hdr.endswith("]"):
                    current_cat = hdr[1:-1].strip() or current_cat
                elif hdr:
                    current_cat = hdr
                continue
            if not line.startswith(("http://","https://")):
                continue
            nm = urlparse(line).path.rsplit("/", 1)[-1] or urlparse(line).netloc
            rows.append({
                "category": current_cat,
                "name": nm,
                "url": line,
                "out_subdir": current_cat.upper().replace(" ", "_"),
                "mode": "direct",
                "notes": "",
            })
    return rows

def iter_manifests(path: Path) -> List[Dict[str,str]]:
    """Return normalized rows from a file or a directory (recursively)."""
    if path.is_file():
        if path.suffix.lower() == ".csv":
            return read_csv_manifest(path)
        elif path.suffix.lower() == ".txt":
            return read_txt_manifest(path)
        else:
            raise ValueError(f"Unsupported manifest file type: {path.suffix}")
    # directory → recurse for *.csv and *.txt
    rows: List[Dict[str,str]] = []
    for p in path.rglob("*"):
        if not p.is_file(): continue
        try:
            if p.suffix.lower() == ".csv":
                rows += read_csv_manifest(p)
            elif p.suffix.lower() == ".txt":
                rows += read_txt_manifest(p)
        except Exception as e:
            logger.error("Manifest error for %s: %s", p, e)
            console.print(f"[red]Manifest error for {p}:[/red] {e}")
    return rows

# ---------- download ----------
def download_one(row: Dict[str,str], out_root: Path, timeout: Tuple[int,int], retries: int, resume: bool) -> Tuple[str,str]:
    category = (row.get("category") or "MISC").strip()
    name     = (row.get("name") or "").strip()
    url      = (row.get("url") or "").strip()
    subdir   = (row.get("out_subdir") or category).strip()
    mode     = (row.get("mode") or "direct").strip().lower()

    if not url.startswith(("http://","https://")):
        return "skip", f"Non-HTTP url: {url}"
    if mode == "portal":
        logger.info("PORTAL (skip): %s | %s", name or url, url)
        return "portal", f"Portal page (skipped): {url}"

    dst_dir = out_root / subdir
    ensure_dir(dst_dir)
    dst = dst_dir / guess_filename(url, name)
    part = dst.with_suffix(dst.suffix + ".part")

    expected = head_content_length(url, timeout)
    if dst.exists() and expected is not None and dst.stat().st_size == expected:
        logger.info("Exists OK: %s", dst)
        return "ok", f"Exists (size match): {dst}"

    attempt = 0
    while attempt <= retries:
        try:
            headers = {}
            if resume and part.exists():
                headers["Range"] = f"bytes={part.stat().st_size}-"
            with requests.get(url, stream=True, headers=headers, timeout=timeout, allow_redirects=True) as r:
                if r.status_code in (401,403):
                    logger.warning("Auth required/forbidden: %s", url)
                    return "auth", f"Auth required/forbidden: {url}"
                if r.status_code >= 400:
                    raise RuntimeError(f"HTTP {r.status_code}")
                # Header-only portal detection (no stream consumption)
                if looks_like_portal(r.headers):
                    logger.info("Portal/HTML (skip): %s", url)
                    return "portal", f"Portal (HTML) skip: {url}"
                mode_open = "ab" if headers.get("Range") else "wb"
                with open(part, mode_open) as fh:
                    for chunk in r.iter_content(chunk_size=1_048_576):  # 1MB
                        if chunk: fh.write(chunk)
            if expected is not None and part.stat().st_size < expected:
                raise RuntimeError("Truncated transfer")
            if dst.exists():
                try: dst.unlink()
                except Exception: pass
            os.replace(part, dst)
            logger.info("Downloaded: %s  (%s)", dst, url)
            return "ok", f"Downloaded: {dst}"
        except Exception as e:
            attempt += 1
            wait = min(30, 2 ** attempt)
            logger.warning("Retry %d/%d for %s: %s; wait %ss", attempt, retries, url, e, wait)
            time.sleep(wait)
    return "fail", f"Failed: {url}"

# ---------- main ----------
def main():
    ap = argparse.ArgumentParser(description="Fetch from CSV/TXT manifest(s) with progress and logging")
    ap.add_argument("--manifest", required=True, help="Path to CSV/TXT file OR directory of manifests")
    ap.add_argument("--out-root", default=os.path.join("data","01_raw","NANOMEDICINE"), help="Output root folder")
    ap.add_argument("--workers", type=int, default=4, help="Parallel downloads")
    ap.add_argument("--timeout-connect", type=int, default=15, help="Connect timeout (s)")
    ap.add_argument("--timeout-read", type=int, default=120, help="Read timeout (s)")
    ap.add_argument("--retries", type=int, default=3, help="Retries per file")
    ap.add_argument("--resume", action="store_true", help="Resume partial .part files")
    ap.add_argument("--dry", action="store_true", help="Preview only; no downloads")
    args = ap.parse_args()

    mp = Path(args.manifest)
    if not mp.exists():
        console.print(f"[red]Manifest path not found:[/red] {mp}"); sys.exit(2)

    try:
        rows = iter_manifests(mp)
    except Exception as e:
        console.print(f"[red]Manifest parse error:[/red] {e}"); sys.exit(2)

    if not rows:
        console.print("[yellow]No downloadable rows found (only portal pointers or empty files).[/yellow]")
        sys.exit(0)

    total = len(rows)
    out_root = Path(args.out_root); ensure_dir(out_root)
    timeout = (args.timeout_connect, args.timeout_read)

    scanned = 0
    ok = fail = skipped = portal = auth = 0

    with Progress(
        TextColumn("[bold blue]Downloads"),
        BarColumn(),
        TextColumn("[green]{task.percentage:>3.0f}%"),
        TextColumn("• {task.completed}/{task.total} done"),
        TextColumn("• scanned {scanned}"),
        TextColumn("• remaining {remaining}"),
        TimeRemainingColumn(),
        console=console,
        transient=False,
        refresh_per_second=4,
    ) as progress:
        task = progress.add_task("Downloads", total=total)
        progress.update(task, scanned=scanned, remaining=total-scanned)

        def work(row: Dict[str,str]) -> Tuple[str,str]:
            nonlocal ok, fail, skipped, portal, auth
            if args.dry:
                logger.info("DRY: %s | %s", row.get("name"), row.get("url"))
                return "dry", "Dry run"
            status, msg = download_one(row, out_root, timeout, args.retries, args.resume)
            if status == "ok": ok += 1
            elif status == "fail": fail += 1
            elif status == "portal": portal += 1
            elif status == "auth": auth += 1
            else: skipped += 1
            return status, msg

        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            futs = [ex.submit(work, r) for r in rows]
            for fut in as_completed(futs):
                try: _ = fut.result()
                except Exception as e:
                    logger.error("Worker error: %s", e); fail += 1
                scanned += 1
                progress.update(task, advance=1, scanned=scanned, remaining=total-scanned)

    console.print(f"[bold green]Done.[/bold green] ok={ok} portal={portal} auth={auth} skipped={skipped} fail={fail}")
    logger.info("Summary ok=%d portal=%d auth=%d skipped=%d fail=%d", ok, portal, auth, skipped, fail)

if __name__ == "__main__":
    main()
