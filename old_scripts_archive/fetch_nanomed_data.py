# File: scripts/download_one.py
# Purpose: Single-file, resumable downloader for HTTP/HTTPS/FTP with retries and atomic writes.
# Usage (CLI test):  python scripts\download_one.py --url <URL> --out-root data\01_raw --name-hint optional_name
import os, sys, time, math, argparse, traceback, random
from urllib.parse import urlsplit, unquote
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError, ContentTooShortError
from ftplib import FTP, error_perm

UA = "bc-project-downloader/1.0 (+win; py3.11)"
CHUNK = 1 << 20  # 1 MiB
MAX_RETRIES = 3
TIMEOUT = 45

def _sanitize_filename(name: str) -> str:
    # Windows-illegal:  \ / : * ? " < > | and trailing dots/spaces
    bad = '<>:"/\\|?*'
    name = "".join("_" if c in bad else c for c in name)
    name = name.strip().rstrip(". ")
    return name or "file"

def _derive_paths(url: str, out_root: str, name_hint: str | None):
    parts = urlsplit(url)
    netloc = _sanitize_filename(parts.netloc) or "host"
    path = unquote(parts.path or "/").replace("\\", "/")
    base = _sanitize_filename(path.rsplit("/", 1)[-1]) or None
    # If URL ends with '/', use hint; else use URL basename.
    if not base or base == "":
        base = _sanitize_filename(name_hint) if name_hint else "download"
    # Avoid absurd names from query-only links
    if "." not in base and name_hint and "." in name_hint:
        base = _sanitize_filename(name_hint)
    subdir = path.rsplit("/", 1)[0].lstrip("/")
    safe_subdir = _sanitize_filename(subdir.replace("/", os.sep))
    dest_dir = os.path.join(out_root, netloc, safe_subdir) if safe_subdir else os.path.join(out_root, netloc)
    os.makedirs(dest_dir, exist_ok=True)
    final_path = os.path.join(dest_dir, base)
    part_path = final_path + ".part"
    return final_path, part_path, parts

def _atomic_replace(src, dst):
    # Retry replace in case AV/Indexers momentarily lock the file
    for i in range(6):
        try:
            os.replace(src, dst)
            return
        except PermissionError:
            time.sleep(0.2 * (i + 1))
    # Last try
    os.replace(src, dst)

def _http_https_download(url, final_path, part_path, hdrs, timeout, chunk, progress):
    # Determine resume offset
    offset = os.path.getsize(part_path) if os.path.exists(part_path) else 0
    req = Request(url, headers=hdrs)
    if offset > 0:
        req.add_header("Range", f"bytes={offset}-")
    with urlopen(req, timeout=timeout) as resp:
        # Content length may be remaining bytes when 206
        total = resp.getheader("Content-Length")
        try:
            total = int(total) if total is not None else None
        except ValueError:
            total = None
        mode = "ab" if offset else "wb"
        downloaded = offset
        with open(part_path, mode, buffering=0) as f:
            while True:
                chunk_bytes = resp.read(chunk)
                if not chunk_bytes:
                    break
                f.write(chunk_bytes)
                downloaded += len(chunk_bytes)
                if progress:
                    progress(downloaded, total + offset if (total is not None and offset) else (total or -1))
    # Finalize
    _atomic_replace(part_path, final_path)
    return final_path

def _ftp_download(parts, final_path, part_path, timeout, chunk, progress):
    # Parse path and connect
    host = parts.hostname
    if not host:
        raise URLError("Invalid FTP host")
    port = parts.port or 21
    user = parts.username or "anonymous"
    pwd = parts.password or "anonymous@"
    remote_path = parts.path or "/"
    # Resume offset
    offset = os.path.getsize(part_path) if os.path.exists(part_path) else 0
    downloaded = offset

    def _cb(data):
        nonlocal downloaded
        with open(part_path, "ab", buffering=0) as f:
            f.write(data)
        downloaded += len(data)
        if progress:
            progress(downloaded, -1)

    with FTP() as ftp:
        ftp.connect(host, port, timeout=timeout)
        ftp.login(user, pwd)
        # Navigate to directory
        dirpath, fname = remote_path.rsplit("/", 1) if "/" in remote_path else ("", remote_path)
        if dirpath:
            for seg in dirpath.split("/"):
                if seg:
                    try:
                        ftp.cwd(seg)
                    except error_perm as e:
                        raise URLError(f"FTP CWD failed: {e}")
        # Try resume via REST
        try:
            if offset:
                ftp.sendcmd(f"REST {offset}")
        except error_perm:
            # Some servers disallow REST; restart from zero
            offset = 0
            downloaded = 0
            if os.path.exists(part_path):
                os.remove(part_path)
        # Retrieve
        cmd = f"RETR {fname}"
        # Ensure .part exists and opened once before callbacks
        if not os.path.exists(part_path):
            open(part_path, "wb").close()
        ftp.retrbinary(cmd, _cb, blocksize=chunk)
    _atomic_replace(part_path, final_path)
    return final_path

def download_one(url: str, out_root: str, name_hint: str | None = None,
                 timeout: int = TIMEOUT, max_retries: int = MAX_RETRIES,
                 chunk_size: int = CHUNK, progress=None) -> str:
    """
    Download a single URL into out_root with resumable .part and atomic replace.
    Returns the absolute path to the final saved file. Skips if final exists.
    progress: optional callable(bytes_done:int, total_bytes:int| -1)
    """
    if not url or not isinstance(url, str):
        raise URLError("Empty URL")
    scheme = urlsplit(url).scheme.lower()
    if scheme not in ("http", "https", "ftp"):
        raise URLError(f"Unsupported URL scheme: {scheme}")

    final_path, part_path, parts = _derive_paths(url, out_root, name_hint)
    # Idempotence: if final exists and non-zero, skip
    if os.path.exists(final_path) and os.path.getsize(final_path) > 0:
        return final_path

    hdrs = {"User-Agent": UA, "Accept-Encoding": "identity"}
    # Ensure .part file exists if resuming
    if not os.path.exists(part_path):
        open(part_path, "wb").close()

    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            if scheme in ("http", "https"):
                return _http_https_download(url, final_path, part_path, hdrs, timeout, chunk_size, progress)
            else:
                return _ftp_download(parts, final_path, part_path, timeout, chunk_size, progress)
        except (HTTPError, URLError, ContentTooShortError, TimeoutError, OSError) as e:
            last_err = e
            # Backoff with jitter
            sleep_s = min(10, 1.5 ** attempt) + random.uniform(0, 0.5)
            if attempt < max_retries:
                # If server doesn't support resume, start clean next attempt
                try:
                    if os.path.exists(part_path) and os.path.getsize(part_path) == 0:
                        os.remove(part_path)
                except OSError:
                    pass
                time.sleep(sleep_s)
            else:
                break
    # Clean up empty .part on final failure
    try:
        if os.path.exists(part_path) and os.path.getsize(part_path) == 0:
            os.remove(part_path)
    except OSError:
        pass
    raise URLError(f"Failed to download after {max_retries} attempts: {url} :: {last_err}")

# ---------- Minimal CLI for testing ----------
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
        sys.exit(1)
