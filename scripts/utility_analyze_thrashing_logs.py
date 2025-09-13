# scripts/utility_analyze_thrashing_logs.py
# V2.3: BRUTALLY SIMPLE ARGUMENT PARSING
# Purpose: This version removes ALL complex default pathing to fix the ignored
# command-line argument bug. It now takes a REQUIRED argument for the log path.

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
import argparse

# --- Hardcoded Project Structure (Relative to this script's location) ---
# This is a more robust way to find the root for the quarantine directory
PROJECT_ROOT = Path(__file__).resolve().parents[1]
QUARANTINE_DIR = PROJECT_ROOT / "02d_data_culprits_quarantined"

NUM_WORKERS = 4

def read_log_file(log_file_path: Path) -> list[str]:
    """Worker function to read a single log file."""
    try:
        with open(log_file_path, "r", encoding="utf-8") as f:
            return [line.strip() for line in f if line.strip()]
    except Exception:
        return []

def main():
    """Analyzes thrashing logs to find and quarantine culprit files."""
    # --- V2.3 FIX: Make the argument required and remove all defaults ---
    parser = argparse.ArgumentParser(description="Analyze Dask thrashing logs to find culprits.")
    parser.add_argument(
        "log_dir",  # No "--", this makes it a required positional argument
        type=Path,
        help="Path to the forensic logs directory (e.g., D:\\forensic_logs)"
    )
    args = parser.parse_args()
    
    # Use the REQUIRED path from the command line. This cannot fail.
    forensic_logs_dir = args.log_dir
    
    console = Console()
    console.rule("[bold magenta]Dask Thrashing Log Analyzer (V2.3)[/bold magenta]")
    console.print(f"Analyzing logs from: [bold cyan]{forensic_logs_dir.resolve()}[/bold cyan]")

    if not forensic_logs_dir.exists():
        console.print(f"[bold red]Error:[/bold red] Specified logs directory not found at '{forensic_logs_dir.resolve()}'")
        return

    # --- The rest of the script is unchanged ---
    log_files = list(forensic_logs_dir.glob("*.log"))
    if not log_files:
        console.print(f"[yellow]No forensic log files found in '{forensic_logs_dir.resolve()}' to analyze.[/]")
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
        console.print("[green]Logs were found but contained no file entries.[/]")
        return
        
    culprit_counts = Counter(all_logged_files)
    
    table = Table(title="[bold red]Thrashing Culprit Analysis[/bold red]", header_style="bold red")
    table.add_column("Culprit File Path", style="cyan")
    table.add_column("Crash Loop Count", style="red", justify="right")

    culprits_to_quarantine = []
    for filename, count in culprit_counts.most_common(50):
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
                console.print(f"[yellow]Warning: Culprit file not found (path might be incorrect in logs): {culprit_path}[/yellow]")

        console.print(f"\n[green]Successfully quarantined {quarantined_count} culprit file(s).[/]")
        console.print(Panel("The pipeline is now ready to be re-run on the sanitized data.", title="[bold green]Next Steps[/]"))

if __name__ == "__main__":
    main()