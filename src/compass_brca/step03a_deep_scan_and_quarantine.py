# src/compass_brca/step03a_deep_scan_and_quarantine.py
# DEFINITIVE "BOMB DETECTOR"
# This script proactively finds and quarantines pathologically wide Parquet files.
import sys
import shutil
from pathlib import Path
import pyarrow.parquet as pq

try:
    from compass_brca.pipeline_config import (
        INTERIM_DATA_DIR, CULPRIT_QUARANTINE_DIR, LOGS_DIR
    )
except ImportError:
    project_root = Path(__file__).resolve().parents[2]
    sys.path.append(str(project_root))
    from src.compass_brca.pipeline_config import (
        INTERIM_DATA_DIR, CULPRIT_QUARANTINE_DIR, LOGS_DIR
    )

from rich.console import Console
from rich.progress import track

console = Console()

# --- BOMB DEFINITION ---
# A file is considered a "bomb" if it has more columns than this threshold.
COLUMN_THRESHOLD = 50000 
# --- END BOMB DEFINITION ---

def main():
    """Finds and quarantines Parquet files that are too 'wide' to process safely."""
    console.rule("[bold magenta]Step 03a: Deep Scan & Quarantine (Bomb Detector)[/]")
    
    if not INTERIM_DATA_DIR.exists():
        console.print(f"[bold red]Error: Interim data directory not found at {INTERIM_DATA_DIR}[/]"); sys.exit(1)
        
    CULPRIT_QUARANTINE_DIR.mkdir(exist_ok=True)
    LOGS_DIR.mkdir(exist_ok=True)
    quarantine_log_path = LOGS_DIR / "step03a_quarantine_log.txt"

    all_parquet_files = list(INTERIM_DATA_DIR.rglob("*.parquet"))
    if not all_parquet_files:
        console.print("[yellow]No Parquet files found to scan.[/]"); return

    console.print(f"Scanning [cyan]{len(all_parquet_files):,}[/] files with a column threshold of {COLUMN_THRESHOLD:,}...")
    
    quarantined_count = 0
    with open(quarantine_log_path, "a", encoding="utf-8") as log_file:
        for file_path in track(all_parquet_files, description="Scanning for bombs..."):
            try:
                # Low-level metadata read. This is memory-safe.
                parquet_file = pq.ParquetFile(file_path)
                num_columns = parquet_file.metadata.num_columns
                
                if num_columns > COLUMN_THRESHOLD:
                    quarantined_count += 1
                    destination = CULPRIT_QUARANTINE_DIR / file_path.name
                    console.print(f"\n[bold yellow]BOMB DETECTED:[/]\n  - [dim]File:[/dim] {file_path.name}\n  - [dim]Columns:[/dim] {num_columns:,}\n  - [bold]Action: Quarantining.[/]")
                    shutil.move(file_path, destination)
                    log_file.write(f"QUARANTINED: {file_path} (Columns: {num_columns})\n")

            except Exception as e:
                console.print(f"[bold red]Error scanning metadata for {file_path.name}: {e}[/]")
                log_file.write(f"ERROR_SCANNING: {file_path} (Reason: {e})\n")

    console.rule("[bold green]Deep Scan Complete[/]")
    if quarantined_count > 0:
        console.print(f"  - [bold red]Found and quarantined {quarantined_count} bomb(s).[/]")
        console.print(f"  - See log for details: {quarantine_log_path}")
    else:
        console.print("  - [green]No bombs detected. The dataset is safe for Step 04.[/]")

if __name__ == "__main__":
    main()