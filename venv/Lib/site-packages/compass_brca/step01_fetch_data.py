# src/compass_brca/step01_fetch_data.py
# DEFINITIVE VERSION V2 (Intelligent Dispatcher)
import sys
import pandas as pd
from pathlib import Path
import urllib.request
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    from compass_brca.pipeline_config import MANIFESTS_DIR, RAW_DATA_DIR
except ImportError:
    sys.path.append(str(Path(__file__).resolve().parents[2]))
    from src.compass_brca.pipeline_config import MANIFESTS_DIR, RAW_DATA_DIR

from rich.console import Console
from rich.progress import Progress, BarColumn, TextColumn, TransferSpeedColumn

console = Console()
MANIFEST_FILE = MANIFESTS_DIR / "master_manifest_FINAL.csv"
MAX_WORKERS = 4

def process_item(item_tuple, progress, overall_task):
    """Worker function to handle one item from the manifest (download or copy)."""
    index, total, url, dest_path = item_tuple
    
    if url.startswith("file:///"):
        source_path = Path(url[8:])
        task_id = progress.add_task(f"[yellow]Copying {dest_path.name}", total=1)
        try:
            if not source_path.exists():
                progress.update(task_id, completed=1, description="[red]Failed (Not Found)")
                return
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(source_path, dest_path)
            progress.update(task_id, completed=1, description=f"[green]✓ Copied {dest_path.name}")
        except Exception as e:
            progress.update(task_id, completed=1, description="[red]Failed (Copy Error)")
    elif url.startswith(('http://', 'https://', 'ftp://')):
        try:
            with urllib.request.urlopen(url) as response:
                total_size = int(response.info().get('Content-Length', 0))
                task_id = progress.add_task(f"[cyan]Downloading {dest_path.name}", total=total_size)
                dest_path.parent.mkdir(parents=True, exist_ok=True)
                with open(dest_path, 'wb') as f:
                    while True:
                        chunk = response.read(16 * 1024)
                        if not chunk: break
                        f.write(chunk)
                        progress.update(task_id, advance=len(chunk))
                progress.update(task_id, description=f"[green]✓ Downloaded {dest_path.name}")
        except Exception as e:
            task_id = progress.add_task(f"[red]Failed {dest_path.name}", total=1)
            progress.update(task_id, completed=1, description="[red]Failed (Download Error)")
    
    progress.update(overall_task, advance=1)

def main():
    console.rule("[bold magenta]Step 01: Intelligent Data Dispatcher[/]")
    if not MANIFEST_FILE.exists():
        console.print(f"[bold red]Fatal Error: Master manifest not found at {MANIFEST_FILE}[/]"); sys.exit(1)

    manifest_df = pd.read_csv(MANIFEST_FILE)
    tasks = []
    
    for index, row in manifest_df.iterrows():
        url, dest_folder_name = row['source_url'], row['destination_folder']
        filename = Path(url.split('/')[-1].split('?')[0])
        destination_path = RAW_DATA_DIR / dest_folder_name / filename
        if not (destination_path.exists() and destination_path.stat().st_size > 0):
            tasks.append((index + 1, len(manifest_df), url, destination_path))

    if not tasks:
        console.print("[green]All manifest files already exist. Nothing to fetch.[/]"); return

    console.print(f"Found [cyan]{len(tasks)}[/] new items to fetch.")
    
    with Progress(TextColumn("[bold blue]Overall"), BarColumn(), TextColumn("{task.completed}/{task.total}"), TextColumn("•"), TransferSpeedColumn(), console=console) as progress:
        overall_task = progress.add_task("Fetching...", total=len(tasks))
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(process_item, task, progress, overall_task) for task in tasks]
            for future in as_completed(futures):
                try: future.result()
                except Exception as e: console.print(f"[bold red]A worker thread raised an error: {e}[/]")
    
    console.rule("[bold green]Step 01: Fetch Raw Data - COMPLETED[/]")

if __name__ == "__main__":
    main()