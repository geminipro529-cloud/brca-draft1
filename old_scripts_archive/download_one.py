# ===============================
# File: scripts/download_one.py
# ===============================
# Purpose: Single-file, resumable downloader for HTTP/HTTPS/FTP with retries, backoff, atomic writes, and progress callback.
import os, sys, time, argparse, traceback, random
from urllib.parse import urlsplit, unquote
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError, ContentTooShortError
from ftplib import FTP, error_perm

UA = "bc-project-downloader/1.0 (+win; py3.11)"
CHUNK = 1 << 20  # 1 MiB
MAX_RETRIES = 3
TIMEOUT = 45

def _sanitize_filename(name: str) -> str:
    bad = '<>:"/\\|?*'
    name = "".join("_" if c in bad else c for c in name)
    name = name.strip().rstrip(". ")
    return name or "file"

def _derive_paths(url: str, out_root: str, name_hint: str | None):
    parts = urlsplit(url)
    netloc = _sanitize_filename(parts.netloc) or "host"
    path = unquote(parts.path or "/").replace("\\", "/")
    base = _sanitize_filename(path.rsplit("/", 1)[-1]) or None
    if not base:
        base = _sanitize_filename(name_hint) if name_hint else "download"
    if "." not in base and name_hint and "." in name_hint:
        base = _sanitize_filename(name_hint)
    subdir = path.rsplit("/", 1)[0].lstrip("/")
    safe_subdir = _sanitize_filename(subdir.replace("/", os.sep))
    dest_dir = os.path.join(out_root, netloc, safe_subdir) if safe_subdir else os.path.join(out_root, netloc)
    os.makedirs(dest_dir, exist_ok=True)
    final_path = os.path.join(dest_dir, base)
    part_path  = final_path + ".part"
    return final_path, part_path, parts

def _atomic_replace(src, dst):
    for i in range(6):
        try:
            os.replace(src, dst)
            return
        except PermissionError:
            time.sleep(0.2 * (i + 1))
    os.replace(src, dst)

def _http_https_download(url, final_path, part_path, hdrs, timeout, chunk, progress):
    offset = os.path.getsize(part_path) if os.path.exists(part_path) else 0
    req = Request(url, headers=hdrs)
    if offset > 0:
        req.add_header("Range", f"bytes={offset}-")
    with urlopen(req, timeout=timeout) as resp:
        code = getattr(resp, "status", None) or resp.getcode()
        if offset > 0 and code == 200:
            # Server ignored Range; restart clean to avoid corruption.
            offset = 0
            open(part_path, "wb").close()
        total_str = resp.getheader("Content-Length")
        total = int(total_str) if total_str is not None else -1
        full_size = (total + offset) if (total >= 0 and code == 206 and offset > 0) else total
        mode = "ab" if offset else "wb"
        downloaded = offset
        with open(part_path, mode, buffering=0) as f:
            while True:
                block = resp.read(chunk)
                if not block:
                    break
                f.write(block)
                downloaded += len(block)
                if progress:
                    progress(downloaded, full_size if full_size is not None else -1)
    _atomic_replace(part_path, final_path)
    return final_path

def _ftp_download(parts, final_path, part_path, timeout, chunk, progress):
    host = parts.hostname
    if not host:
        raise URLError("Invalid FTP host")
    port = parts.port or 21
    user = parts.username or "anonymous"
    pwd  = parts.password or "anonymous@"
    remote_path = parts.path or "/"
    offset = os.path.getsize(part_path) if os.path.exists(part_path) else 0
    downloaded = offset
    with open(part_path, "ab", buffering=0) as f:
        def _cb(data):
            nonlocal downloaded
            f.write(data)
            downloaded += len(data)
            if progress:
                progress(downloaded, -1)
        with FTP() as ftp:
            ftp.connect(host, port, timeout=timeout)
            ftp.login(user, pwd)
            dirpath, fname = remote_path.rsplit("/", 1) if "/" in remote_path else ("", remote_path)
            if dirpath:
                for seg in dirpath.split("/"):
                    if seg:
                        ftp.cwd(seg)
            rest_offset = offset
            if offset > 0:
                try:
                    ftp.sendcmd(f"REST {offset}")
                except error_perm:
                    rest_offset = 0
                    downloaded = 0
                    f.seek(0); f.truncate()
            ftp.retrbinary(f"RETR {fname}", _cb, blocksize=chunk, rest=(rest_offset if rest_offset else None))
    _atomic_replace(part_path, final_path)
    return final_path

def download_one(url: str, out_root: str, name_hint: str | None = None,
                 timeout: int = TIMEOUT, max_retries: int = MAX_RETRIES,
                 chunk_size: int = CHUNK, progress=None) -> str:
    if not url or not isinstance(url, str):
        raise URLError("Empty URL")
    scheme = urlsplit(url).scheme.lower()
    if scheme not in ("http", "https", "ftp"):
        raise URLError(f"Unsupported URL scheme: {scheme}")
    final_path, part_path, parts = _derive_paths(url, out_root, name_hint)
    if os.path.exists(final_path) and os.path.getsize(final_path) > 0:
        return final_path
    hdrs = {"User-Agent": UA, "Accept-Encoding": "identity"}
    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            if scheme in ("http", "https"):
                return _http_https_download(url, final_path, part_path, hdrs, timeout, chunk_size, progress)
            else:
                return _ftp_download(parts, final_path, part_path, timeout, chunk_size, progress)
        except (HTTPError, URLError, ContentTooShortError, TimeoutError, OSError) as e:
            last_err = e
            if attempt < max_retries:
                time.sleep(min(10, 1.5 ** attempt) + random.uniform(0, 0.5))
            else:
                break
    try:
        if os.path.exists(part_path) and os.path.getsize(part_path) == 0:
            os.remove(part_path)
    except OSError:
        pass
    raise URLError(f"Failed after {max_retries} attempts: {url} :: {last_err}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Resumable single-file downloader (HTTP/HTTPS/FTP).")
    ap.add_argument("--url", required=True)
    ap.add_argument("--out-root", required=True)
    ap.add_argument("--name-hint", default=None)
    ap.add_argument("--timeout", type=int, default=TIMEOUT)
    ap.add_argument("--retries", type=int, default=MAX_RETRIES)
    args = ap.parse_args()
    try:
        path = download_one(args.url, args.out_root, args.name_hint, args.timeout, args.retries)
        print(f"[OK] {args.url} -> {path}")
    except Exception as e:
        print(f"[ERR] {args.url} -> {e}")
        traceback.print_exc()
        sys.exit(1)


# =============================
# File: scripts/fetch_data.py
# =============================
# Purpose: Concurrent, resumable multi-file downloader for nested manifests with overall + per-file progress.
import os, sys, argparse, time, traceback
from concurrent.futures import ThreadPoolExecutor, as_completed

# Optional pretty progress (falls back to plain prints if not installed)
try:
    from tqdm import tqdm
except Exception:
    tqdm = None

# Ensure we can import download_one from scripts/
sys.path.append(os.path.dirname(__file__))
from download_one import download_one

def _iter_txt_files(root_or_file: str):
    if os.path.isdir(root_or_file):
        for r, _, files in os.walk(root_or_file):
            for f in files:
                if f.lower().endswith(".txt"):
                    yield os.path.join(r, f)
    elif os.path.isfile(root_or_file) and root_or_file.lower().endswith(".txt"):
        yield root_or_file

def _load_urls(path: str):
    urls = []
    for f in _iter_txt_files(path):
        with open(f, encoding="utf-8") as fh:
            urls.extend(line.strip() for line in fh if line.strip())
    # de-dupe while preserving order
    seen = set(); deduped = []
    for u in urls:
        if u not in seen:
            seen.add(u); deduped.append(u)
    return deduped

def main(urls_path: str, out_root: str, workers: int, log_file: str):
    os.makedirs(out_root, exist_ok=True)
    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    urls = _load_urls(urls_path)
    if not urls:
        sys.exit(f"[ERROR] No URLs found under: {urls_path}")

    done = set()
    if os.path.exists(log_file):
        with open(log_file, encoding="utf-8") as lf:
            for line in lf:
                if line.startswith("OK\t"):
                    try:
                        done.add(line.split("\t",2)[1])
                    except Exception:
                        continue

    todo = [u for u in urls if u not in done]
    total, already = len(urls), len(done)
    if not todo:
        print(f"[INFO] All URLs already fetched ({already}/{total}).")
        return

    print(f"[INFO] {len(todo)} new files; {already} already done; total {total}")

    # Overall progress
    if tqdm:
        overall = tqdm(total=len(todo), desc="Files", unit="file", position=0)
    else:
        overall = None
        print("[INFO] Progress: 0/%d" % len(todo))

    # Position pool for per-file bars
    def make_file_bar(name: str, pos: int):
        if tqdm:
            return tqdm(total=0, unit="B", unit_scale=True, desc=name[:40], position=pos, leave=False)
        return None

    def task(url: str, pos: int):
        bar = make_file_bar(os.path.basename(url) or "download", pos)
        last_tick = time.time()

        def progress(downloaded: int, total_size: int):
            nonlocal last_tick
            if bar:
                if total_size > 0:
                    bar.total = total_size
                bar.n = downloaded
                # throttle refresh to ~20fps
                now = time.time()
                if now - last_tick >= 0.05:
                    bar.refresh(); last_tick = now
            else:
                # plain print fallback every ~2s
                now = time.time()
                if now - last_tick >= 2:
                    if total_size > 0:
                        pct = (downloaded / total_size) * 100
                        print(f"[{pos}] {os.path.basename(url)[:40]} - {pct:5.1f}% ({downloaded//(1<<20)} MiB)")
                    else:
                        print(f"[{pos}] {os.path.basename(url)[:40]} - {downloaded//(1<<20)} MiB")
                    last_tick = now

        try:
            path = download_one(url, out_root, progress=progress)
            return ("OK", url, path)
        except Exception as e:
            return ("ERR", url, str(e))
        finally:
            if bar:
                bar.close()

    with ThreadPoolExecutor(max_workers=workers) as ex, open(log_file, "a", encoding="utf-8") as lf:
        futures = {}
        # map positions 1..workers for per-file bars (overall is position 0)
        for i, u in enumerate(todo):
            pos = (i % workers) + 1
            futures[ex.submit(task, u, pos)] = u

        ok = err = 0
        for fut in as_completed(futures):
            status, url, msg = fut.result()
            lf.write(f"{status}\t{url}\t{msg}\n"); lf.flush()
            if status == "OK": ok += 1
            else: err += 1
            if overall:
                overall.update(1)
            else:
                done_n = ok + err
                print(f"[INFO] Progress: {done_n}/{len(todo)}")

    if overall: overall.close()
    print(f"[SUMMARY] OK: {ok}  ERR: {err}  (new: {len(todo)}, total: {total})")

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Concurrent, resumable fetcher (nested manifests; per-file + overall progress).")
    ap.add_argument("--urls", required=True, help="Folder of .txt manifests (recurses) or a single .txt")
    ap.add_argument("--out-root", required=True, help="Output root")
    ap.add_argument("--workers", type=int, default=5, help="Parallel downloads (default=5)")
    ap.add_argument("--log", required=True, help="Log file path (append-only)")
    a = ap.parse_args()
    try:
        main(a.urls, a.out_root, a.workers, a.log)
    except Exception as e:
        print(f"[FATAL] {e}")
        traceback.print_exc()
        sys.exit(1)
