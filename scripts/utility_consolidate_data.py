# scripts/utility_consolidate_data.py
# V-Final (Corrected): A robust, intelligent utility to consolidate a messy, multi-folder data
# directory into the canonical COMPASS-BRCA pipeline structure.
# This definitive version features a stable, multi-stage rich progress UI,
# concurrent workers, and clean, separate logging for a professional user experience.

import argparse
from pathlib import Path
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import logging
import pandas as pd  # <-- VERIFIED: Import is at the top-level
from rich.console import Console
from rich.panel import Panel
from rich.progress import (
    Progress,
    BarColumn,
    TextColumn,
    TimeElapsedColumn,
    MofNCompleteColumn,
    SpinnerColumn,
    TaskProgressColumn,
)
import compass_brca.utils.pipeline_config as cfg

# --- Setup ---
console = Console()
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='consolidation.log',
    filemode='w'
)

# --- File Categorization Rules ---
RAW_EXTENSIONS = ['.csv', '.tsv', '.txt', '.cel', '.fastq', '.gz', '.tar', '.zip', '.json']
INTERIM_EXTENSIONS = ['.parquet']

def process_file(file_path: Path, source_root: Path, dry_run: bool) -> tuple[str, str, str]:
    original_path_str = str(file_path.resolve())
    ext = file_path.suffix.lower()

    destination_dir = None
    if ext in RAW_EXTENSIONS:
        destination_dir = cfg.RAW_DATA_DIR
    elif ext in INTERIM_EXTENSIONS:
        destination_dir = cfg.INTERIM_DATA_DIR
    else:
        return "SKIPPED", original_path_str, f"Unknown extension: {ext}"

    try:
        parent_folder_name = file_path.parent.name
        new_filename = f"{parent_folder_name}_{file_path.name}"
    except IndexError:
        new_filename = file_path.name
        
    new_path = destination_dir / new_filename

    if not dry_run:
        try:
            destination_dir.mkdir(parents=True, exist_ok=True)
            shutil.move(file_path, new_path)
            return "MOVED", original_path_str, str(new_path.resolve())
        except Exception as e:
            logging.error(f"Failed to move {original_path_str}: {e}")
            return "ERROR", original_path_str, str(e)
    else:
        return "PLAN_MOVE", original_path_str, str(new_path.resolve())

def main():
    parser = argparse.ArgumentParser(description="Consolidate scattered data files into the canonical pipeline structure.")
    parser.add_argument("source_directory", type=str, help="The path to the messy parent data directory (e.g., 'data').")
    parser.add_argument("--workers", type=int, default=os.cpu_count(), help="Number of concurrent file processing threads.")
    parser.add_argument("--dry-run", action="store_true", help="Show a plan of all file moves without executing them.")
    args = parser.parse_args()

    source_dir = Path(args.source_directory)
    if not source_dir.exists():
        console.print(f"[bold red]Error:[/bold red] Source directory `{source_dir}` not found.")
        return

    console.print(Panel(
        f"Source: `{source_dir}`\nMode:   {'[bold yellow]Dry Run (Preview Only)[/]' if args.dry_run else '[bold red]Live Run (Files Will Be Moved)[/]'}\nWorkers: {args.workers}",
        title="[bold cyan]Concurrent Data Consolidator[/bold cyan]"
    ))

    if not args.dry_run:
        console.print("[bold red]!!! WARNING !!! This script will physically move files on your disk.[/]")
        if input("Type 'YES' to proceed with the live run: ") != 'YES':
            console.print("[yellow]Aborted by user.[/]")
            return
            
    progress = Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(bar_width=None),
        MofNCompleteColumn(),
        TaskProgressColumn(),
        TimeElapsedColumn(),
        TextColumn("[bold blue]{task.fields[status]}"),
        console=console,
        transient=False
    )
    
    with progress:
        discovery_task = progress.add_task("[green]Discovering files...", total=None, status="Scanning...")
        all_files = [f for f in source_dir.rglob("*") if f.is_file()]
        progress.update(discovery_task, total=len(all_files), completed=len(all_files), description=f"[green]Discovered {len(all_files)} files[/green]", status="[bold green]Done![/bold green]")
    
        if not all_files:
            console.print("[yellow]No files found in the source directory to consolidate.[/yellow]")
            return

        consolidation_task = progress.add_task(f"[cyan]Consolidating files...", total=len(all_files), status="Initializing workers...")
        consolidation_log = []
        
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            futures = {executor.submit(process_file, f, source_dir, args.dry_run): f for f in all_files}
            
            for future in as_completed(futures):
                file_being_processed = futures[future].name
                progress.update(consolidation_task, advance=1, status=f"Processing: {file_being_processed[:35]}...")
                
                result = future.result()
                consolidation_log.append(result)
                
                if result[0] == "ERROR":
                    console.print(f"[bold red]ERROR:[/bold red] Failed to move '{result[1]}'. Check `consolidation.log` for details.")

    report_path = Path(f"consolidation_report_{source_dir.name}.csv")
    
    # This line will now work because 'pd' is guaranteed to be defined.
    report_df = pd.DataFrame(consolidation_log, columns=["status", "original_path", "new_path_or_message"])
    report_df.to_csv(report_path, index=False)
    
    console.print(f"\n[bold green]Consolidation process complete![/bold green]")
    console.print(f"  - A detailed CSV report has been saved to: `{report_path}`")
    console.print(f"  - A detailed error log has been saved to: `consolation.log`")
    if args.dry_run:
        console.print("[yellow]This was a dry run. No files were actually moved.[/yellow]")
    else:
        console.print(f"  - Your data has been organized into `{cfg.RAW_DATA_DIR}` and `{cfg.INTERIM_DATA_DIR}`.")
        console.print(f"[bold]Next Step:[/bold] It is highly recommended to now delete the old, now-empty subfolders inside `{source_dir}`.")

if __name__ == "__main__":
    main()