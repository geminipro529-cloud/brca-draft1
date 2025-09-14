# compass_brca/step01_intelligent_fetch_and_process.py
# V4.1: DEFINITIVE COMPONENTIZED DOWNLOADER

import asyncio
import sys
from pathlib import Path
import shutil
from urllib.parse import urlparse
import httpx
import polars as pl
import pyarrow.parquet as pq
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, BarColumn, DownloadColumn, TextColumn, TimeRemainingColumn, TransferSpeedColumn, TaskID
from rich.live import Live

try:
    from compass_brca.utils.pipeline_config import MANIFESTS_DIR, RAW_DATA_DIR, QUARANTINE_DIR, CHUNKED_DIR
except ImportError:
    PROJECT_ROOT = Path(__file__).resolve().parents[1]
    MANIFESTS_DIR, RAW_DATA_DIR, MASTER_MANIFEST_FINAL, QUARANTINE_DIR, CHUNKED_DIR = \
        PROJECT_ROOT / "00_manifests", PROJECT_ROOT / "01_data_raw", "master_manifest_FINAL.csv", \
        PROJECT_ROOT / "02b_data_quarantined", PROJECT_ROOT / "02c_data_chunked_safe"

MAX_CONCURRENT_TASKS = 8
HTTP_TIMEOUT = 45.0
CHUNK_SIZE_DOWNLOAD = 8192
MAX_UNCOMPRESSED_SIZE_MB, MIN_EXPANSION_RATIO = 750, 10.0
CHUNK_ROWS_DISSECT = 100_000

def inspect_file_safety(file_path: Path) -> tuple[bool, str]:
    try:
        if not file_path.name.endswith(('.parquet', '.parq')): return False, "Not a Parquet file"
        on_disk_size = file_path.stat().st_size
        if on_disk_size < 10240: return False, "File too small"
        parquet_file = pq.ParquetFile(file_path)
        uncompressed_size = sum(parquet_file.metadata.row_group(i).total_byte_size for i in range(parquet_file.metadata.num_row_groups))
        uncompressed_size_mb, expansion_ratio = uncompressed_size / (1024*1024), uncompressed_size / on_disk_size if on_disk_size > 0 else 0
        if uncompressed_size_mb > MAX_UNCOMPRESSED_SIZE_MB: return True, f"Uncompressed size ({uncompressed_size_mb:.1f}MB) exceeds threshold"
        if expansion_ratio > MIN_EXPANSION_RATIO: return True, f"Expansion ratio ({expansion_ratio:.1f}x) exceeds threshold"
        return False, "Safe"
    except Exception as e: return False, f"Inspection error: {e}"

def dissect_poison_pill(source_path: Path, progress: Progress, task_id: TaskID):
    try:
        stem = source_path.stem
        chunk_dest_dir = CHUNKED_DIR / stem; chunk_dest_dir.mkdir(parents=True, exist_ok=True)
        progress.update(task_id, description=f"[magenta]DISSECTING[/magenta] {source_path.name}")
        lazy_frame = pl.scan_parquet(source_path)
        total_rows = lazy_frame.select(pl.count()).collect().item()
        if total_rows == 0: return
        num_chunks = (total_rows // CHUNK_ROWS_DISSECT) + (1 if total_rows % CHUNK_ROWS_DISSECT > 0 else 0)
        progress.update(task_id, total=num_chunks, completed=0)
        for i, offset in enumerate(range(0, total_rows, CHUNK_ROWS_DISSECT)):
            chunk_df = lazy_frame.slice(offset, CHUNK_ROWS_DISSECT).collect(streaming=True)
            chunk_df.write_parquet(chunk_dest_dir / f"{stem}_chunk_{i+1:04d}.parquet")
            progress.update(task_id, advance=1)
    except Exception as e: progress.update(task_id, description=f"[bold red]DISSECTION FAILED[/bold red] {source_path.name}")

async def process_manifest_entry(client: httpx.AsyncClient, entry: dict, progress: Progress, overall_files_task: TaskID, overall_bytes_task: TaskID, semaphore: asyncio.Semaphore):
    async with semaphore:
        url, dest_path_str = entry["url"], entry["output_path"]
        dest_path = Path(dest_path_str); filename = dest_path.name
        expected_size = entry.get("filesize", 0)
        quarantined_path, chunked_dir = QUARANTINE_DIR / filename, CHUNKED_DIR / dest_path.stem
        if (dest_path.exists() and dest_path.stat().st_size == expected_size and expected_size > 0) or \
           (quarantined_path.exists() and chunked_dir.exists() and any(chunked_dir.iterdir())):
            progress.update(overall_files_task, advance=1); progress.update(overall_bytes_task, advance=expected_size)
            return
        task_id = progress.add_task(f"[cyan]QUEUED[/cyan] {filename}", total=expected_size, start=False, visible=True)
        is_local_file = urlparse(url).scheme == 'file'
        try:
            progress.start_task(task_id)
            if is_local_file:
                progress.update(task_id, description=f"[blue]COPYING[/blue] {filename}")
                source_path = Path(urlparse(url).path.lstrip('/'))
                if not source_path.exists(): raise FileNotFoundError(f"Local source file not found: {source_path}")
                dest_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy(source_path, dest_path)
                progress.update(task_id, completed=expected_size); progress.update(overall_bytes_task, advance=expected_size)
            else:
                progress.update(task_id, description=f"[green]DOWNLOADING[/green] {filename}")
                async with client.stream("GET", url, timeout=HTTP_TIMEOUT, follow_redirects=True) as response:
                    response.raise_for_status()
                    dest_path.parent.mkdir(parents=True, exist_ok=True)
                    with open(dest_path, "wb") as f:
                        async for chunk in response.aiter_bytes(chunk_size=CHUNK_SIZE_DOWNLOAD):
                            f.write(chunk); progress.update(task_id, advance=len(chunk)); progress.update(overall_bytes_task, advance=len(chunk))
        except Exception as e:
            verb = "Copy" if is_local_file else "Download"
            progress.update(task_id, description=f"[bold red]{verb.upper()} FAILED[/bold red] {filename}"); return
        progress.update(task_id, description=f"[yellow]INSPECTING[/yellow] {filename}", completed=expected_size, total=expected_size)
        is_poison, reason = inspect_file_safety(dest_path)
        if is_poison:
            quarantined_path.parent.mkdir(parents=True, exist_ok=True); shutil.move(dest_path, quarantined_path)
            dissect_poison_pill(quarantined_path, progress, task_id)
            progress.update(task_id, description=f"[bold red]QUARANTINED[/bold red] {filename}")
        else:
            progress.update(task_id, description=f"[bold green]VERIFIED SAFE[/bold green] {filename}")
        progress.update(overall_files_task, advance=1)

async def main(progress_bar: Progress | None = None) -> int:
    console = Console(); is_standalone = progress_bar is None
    # Define MASTER_MANIFEST_FINAL locally as it's not in the global config
    MASTER_MANIFEST_FINAL = "master_manifest_FINAL.csv"
    RAW_DATA_DIR.mkdir(exist_ok=True); QUARANTINE_DIR.mkdir(exist_ok=True); CHUNKED_DIR.mkdir(exist_ok=True)
    manifest_path = MANIFESTS_DIR / MASTER_MANIFEST_FINAL
    if not manifest_path.exists(): console.print(f"[bold red]CRITICAL: Manifest file not found.[/bold red]"); return 1
    tasks_to_process = pl.read_csv(manifest_path).to_dicts()
    total_files, total_size_bytes = len(tasks_to_process), sum(t.get('filesize', 0) for t in tasks_to_process)
    
    progress = progress_bar if not is_standalone else Progress(TextColumn("{task.description}"), BarColumn(), TextColumn("{task.completed}/{task.total}"), DownloadColumn(), TransferSpeedColumn(), TimeRemainingColumn())
    display_context = Live(Panel(progress), console=console) if is_standalone else nullcontext()

    with display_context:
        files_task = progress.add_task("[bold]Overall Files[/]", total=total_files, title="Overall File Progress")
        bytes_task = progress.add_task("[bold]Overall Size[/]", total=total_size_bytes, title="Overall Size Progress")
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)
        async with httpx.AsyncClient(follow_redirects=True) as client:
            coroutines = [process_manifest_entry(client, entry, progress, files_task, bytes_task, semaphore) for entry in tasks_to_process]
            await asyncio.gather(*coroutines)
    if is_standalone: console.rule("[bold green]Step 01 Standalone Run Complete[/bold green]")
    return 0

class nullcontext:
    def __enter__(self): pass
    def __exit__(self, exc_type, exc_val, exc_tb): pass

if __name__ == "__main__":
    sys.exit(asyncio.run(main()))