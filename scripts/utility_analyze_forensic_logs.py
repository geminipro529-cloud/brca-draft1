# scripts/utility_analyze_thrashing_logs.py
# V3.1: DEFINITIVE - Simple and Direct
# This version is hardcoded to look for the logs inside the project root's
# "logs" directory. It has no complex pathing or arguments. It uses multiple
# workers to read log files concurrently for speed.

import sys
from pathlib import Path
from collections import Counter
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.prompt import Confirm
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from rich.progress import track

# --- V3.1: Simplified, Foolproof Path Configuration ---
# This robustly finds the project root from this script's location.
PROJECT_ROOT = Path(__file__).resolve().parents[1]

# This is the location where the Dask worker logs are being written by the scheduler.
# NOTE: The user must move the logs from D:\ to this location first.
LOGS_DIR = PROJECT_ROOT / "logs"

# This is where identified culprits will be moved.
QUARANTINE_DIR = PROJECT_ROOT / "02d_data_culprits_quarantined"
NUM_WORKERS = 4

def read_log_file(log_file_path: Path) -> list[str]:
    """Worker function to read a single log file and return its lines."""
    try:
        with open(log_file_path, "r", encoding="utf-8") as f:
            # We are only interested in lines that are file paths, not tracebacks.
            # A simple heuristic is to filter for lines that contain ".parquet".
            return [line.strip() for line in f if ".parquet" in line and "Traceback" not in line and line.strip()]
    except Exception:
        return []

def main():
    """Analyzes thrashing logs from the project root."""
    console = Console()
    console.rule("[bold magenta]Dask Thrashing Log Analyzer (V3.1)[/bold magenta]")
    console.print(f"Analyzing logs from: [bold cyan]{LOGS_DIR.resolve()}[/bold cyan]")

    if not LOGS_DIR.exists():
        console.print(f"[bold red]Error:[/bold red] Log directory not found at '{LOGS_DIR.resolve()}'")
        console.print("Please ensure you have moved the 'logs' folder from 'D:\\' into your project root.")
        return

    # Find all files that look like dask worker logs
    log_files = [p for p in LOGS_DIR.glob("worker-*.log")]
    if not log_files:
        console.print("[yellow]No Dask worker log files (e.g., worker-....log) found to analyze.[/]")
        return
        
    all_logged_files = []
    
    console.print(f"Reading {len(log_files)} worker log(s) with {NUM_WORKERS} workers...")
    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        future_to_file = {executor.submit(read_log_file, log_file): log_file for log_file in log_files}
        for future in track(as_completed(future_to_file), description="Analyzing logs...", total=len(log_files)):
            try:
                all_logged_files.extend(future.result())
            except Exception as exc:
                console.print(f"[red]Error processing log file {future_to_file[future].name}: {exc}[/red]")

    if not all_logged_files:
        console.print("[green]Logs were found but contained no valid file path entries.[/]")
        return
        
    culprit_counts = Counter(all_logged_files)
    
    table = Table(title="[bold red]Thrashing Culprit Analysis[/bold red]",
                  caption="Files that appear most frequently in worker logs are causing crash loops.",
                  header_style="bold red")
    table.add_column("Culprit File Path", style="cyan")
    table.add_column("Crash Loop Count", style="red", justify="right")
    
    culprits_to_quarantine = []
    for filename, count in culprit_counts.most_common(50):
        # A high count is a strong signal of a thrashing file.
        if count > 5:
            table.add_row(filename, str(count))
            culprits_to_quarantine.append(Path(filename))
        
    console.print(table)

    if not culprits_to_quarantine:
        console.print("[green]Analysis complete. No significant thrashing detected.[/]")
        return
        
    console.print(Panel(f"Found [bold yellow]{len(culprits_to_quarantine)}[/] files causing Dask workers to thrash.", title="[yellow]Analysis Complete[/]"))

    if Confirm.ask("\n[bold yellow]Move these culprit files to the quarantine directory?[/bold yellow]"):
        QUARANTINE_DIR.mkdir(exist_ok=True)
        console.print(f"Moving {len(culprits_to_quarantine)} files to [cyan]{QUARANTINE_DIR}[/]...")
        quarantined_count = 0
        for culprit_path in culprits_to_quarantine:
            if culprit_path.exists():
                try:
                    shutil.move(str(culprit_path), str(QUARANTINE_DIR))
                    quarantined_count += 1
                except Exception as e:
                    console.print(f"[red]Failed to move {culprit_path.name}: {e}[/red]")
            else:
                console.print(f"[yellow]Warning: Culprit file not found (path may be incorrect in logs): {culprit_path}[/yellow]")
        console.print(f"\n[green]Successfully quarantined {quarantined_count} culprit file(s).[/]")
        console.print(Panel("The pipeline is now ready to be re-run on the sanitized data.", title="[bold green]Next Steps[/]"))

if __name__ == "__main__":
    main()