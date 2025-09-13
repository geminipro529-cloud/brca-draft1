# scripts/utils/rich_downloader.py
# V3.0: A professional, rich-featured concurrent downloader module.
# This version adds support for the FTP protocol by using the appropriate
# library (urllib) for FTP links, while still using 'requests' for HTTP/S.

import os
import time
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import Manager
from pathlib import Path
import requests
import urllib.request # <-- Import the library for FTP
from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.progress import (
    BarColumn, DownloadColumn, Progress, TaskProgressColumn, TextColumn,
    TimeRemainingColumn, TransferSpeedColumn, MofNCompleteColumn,
)
from rich.table import Table

console = Console()

def download_file_http(url: str, output_path: Path, task_id: int, progress_manager: dict):
    """Handles HTTP/S downloads using the 'requests' library."""
    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        total_size = int(r.headers.get('content-length', 0))
        progress_manager[task_id] = {"progress": 0, "total": total_size, "status": "downloading"}
        with open(output_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
                progress_manager[task_id]["progress"] += len(chunk)

def download_file_ftp(url: str, output_path: Path, task_id: int, progress_manager: dict):
    """Handles FTP downloads using the 'urllib' library."""
    with urllib.request.urlopen(url, timeout=60) as r:
        total_size = int(r.info().get('Content-Length', 0))
        progress_manager[task_id] = {"progress": 0, "total": total_size, "status": "downloading"}
        with open(output_path, 'wb') as f:
            while chunk := r.read(8192):
                f.write(chunk)
                progress_manager[task_id]["progress"] += len(chunk)

# --- The Main Worker Function (Dispatcher) ---
def download_dispatcher(url: str, output_path_str: str, task_id: int, progress_manager: dict):
    """
    Acts as a dispatcher, choosing the correct download function based on the URL scheme.
    """
    output_path = Path(output_path_str)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Initialize the progress manager entry immediately to prevent KeyError
    progress_manager[task_id] = {"progress": 0, "total": 0, "status": "initializing"}

    try:
        if url.lower().startswith('ftp'):
            download_file_ftp(url, output_path, task_id, progress_manager)
        else: # Default to HTTP/S
            download_file_http(url, output_path, task_id, progress_manager)
        
        progress_manager[task_id]["status"] = "complete"
        return True, url, None
    except Exception as e:
        progress_manager[task_id]["status"] = "error"
        if output_path.exists():
            try: os.remove(output_path)
            except OSError: pass
        return False, url, str(e)

# --- Main Downloader Orchestrator (Largely Unchanged) ---
def run_concurrent_downloader(tasks: list[tuple[str, str]], max_workers: int):
    # (The UI setup code from the previous version remains the same)
    individual_progress = Progress(TextColumn("[bold blue]{task.fields[filename]}", justify="right"), BarColumn(bar_width=None), "[progress.percentage]{task.percentage:>3.1f}%", "•", DownloadColumn(), "•", TransferSpeedColumn(), "•", TimeRemainingColumn(), expand=True)
    overall_progress = Progress(TextColumn("[bold]Overall Progress[/bold]"), BarColumn(), TaskProgressColumn(), MofNCompleteColumn())
    progress_table = Table.grid(expand=True)
    progress_table.add_row(Panel.fit(overall_progress, title="[b]Total[/b]", border_style="green"))
    progress_table.add_row(Panel.fit(individual_progress, title="[b]Concurrent Downloads[/b]", border_style="yellow"))

    with Manager() as manager:
        progress_manager = manager.dict()
        with Live(progress_table, refresh_per_second=10, console=console) as live:
            overall_task = overall_progress.add_task("All Files", total=len(tasks))
            
            with ProcessPoolExecutor(max_workers=max_workers) as executor:
                # We now call the dispatcher, not the old download_file function
                futures = [executor.submit(download_dispatcher, url, path, i, progress_manager) for i, (url, path) in enumerate(tasks)]
                rich_task_ids = [individual_progress.add_task("download", filename=Path(path).name, total=1, start=False, visible=False) for _, path in tasks]

                while not all(f.done() for f in futures):
                    for i in range(len(tasks)):
                        # This check is now safe because the entry is created immediately
                        if i in progress_manager:
                            details = progress_manager[i]
                            if details["status"] == "downloading":
                                individual_progress.update(rich_task_ids[i], total=details["total"], completed=details["progress"], visible=True)
                                if not individual_progress.tasks[rich_task_ids[i]].started:
                                    individual_progress.start_task(rich_task_ids[i])
                            
                    completed_count = sum(1 for f in futures if f.done())
                    overall_progress.update(overall_task, completed=completed_count)
                    time.sleep(0.1)

                overall_progress.update(overall_task, completed=len(tasks))

                for f in futures:
                    success, url, error = f.result()
                    if not success:
                        console.print(f"[bold red]Download Failed:[/bold red] {Path(url).name} - Reason: {error}")

    console.print("\n[bold green]All download tasks complete.[/bold green]")