# scripts/step02_audit_raw_data.py
# V2.0: Multi-source audit.
# Purpose: Scans all raw and chunked data directories to provide a complete summary of contents.
# This verifies the intelligent ingestion step and gives a full look at the data composition.
# Input: Files in 01_data_raw/ AND 02c_data_chunked_safe/
# Output: A summary table printed to the console.

from pathlib import Path
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
import sys

# --- Configuration ---
# Attempt to import from a central pipeline_config.py
try:
    sys.path.append(str(Path(__file__).resolve().parents[1]))
    # This revamped script now needs to know about the CHUNKED_DIR
    from compass_brca.utils.pipeline_config import RAW_DATA_DIR, CHUNKED_DIR 
except ImportError:
    print("Could not import from pipeline_config.py, using script defaults.")
    RAW_DATA_DIR = Path("01_data_raw")
    CHUNKED_DIR = Path("02c_data_chunked_safe")


def main():
    """Scans all raw data directories and prints a formatted, unified audit report."""
    console = Console()
    
    # --- V2.0 CHANGE: Define all directories to be audited ---
    audit_dirs = [RAW_DATA_DIR, CHUNKED_DIR]
    audit_dir_str = " & ".join([f"'{d}'" for d in audit_dirs])
    
    console.print(Panel(f"[bold cyan]Auditing All Raw Data in -> {audit_dir_str}[/]", expand=False))

    # --- V2.0 CHANGE: Recursively find all files from all specified directories ---
    all_files = []
    for data_dir in audit_dirs:
        if data_dir.exists():
            all_files.extend(data_dir.rglob('*'))
        else:
            console.print(f"[yellow]Warning: Audit directory not found: {data_dir}[/]")
            
    files = [f for f in all_files if f.is_file()]

    if not files:
        console.print("[bold yellow]Warning: No files found in any audit directory.[/]")
        # Create the completion checkpoint to allow the pipeline to proceed
        (RAW_DATA_DIR / ".audit_complete").touch()
        return

    table = Table(
        title="Unified Raw Data Audit Report",
        caption=f"Total files found across all sources: {len(files):,}",
        header_style="bold magenta",
        show_header=True
    )
    table.add_column("File Extension", justify="left", style="cyan", no_wrap=True)
    table.add_column("File Count", justify="right", style="magenta")
    table.add_column("Total Size (MB)", justify="right", style="green")
    table.add_column("Smallest File (KB)", justify="right", style="yellow")
    table.add_column("Largest File (MB)", justify="right", style="red")

    extensions = {}
    for file in files:
        ext = file.suffix.lower() if file.suffix else ".no_extension"
        size = file.stat().st_size
        
        if ext not in extensions:
            extensions[ext] = {'count': 0, 'total_size': 0, 'sizes': []}
        
        extensions[ext]['count'] += 1
        extensions[ext]['total_size'] += size
        extensions[ext]['sizes'].append(size)

    for ext, data in sorted(extensions.items()):
        total_size_mb = data['total_size'] / (1024 * 1024)
        smallest_kb = min(data['sizes']) / 1024 if data['sizes'] else 0
        largest_mb = max(data['sizes']) / (1024 * 1024) if data['sizes'] else 0
        table.add_row(
            ext,
            f"{data['count']:,}",
            f"{total_size_mb:,.2f}",
            f"{smallest_kb:,.2f}",
            f"{largest_mb:,.2f}"
        )

    console.print(table)
    console.print(Panel("[bold green]Unified audit complete.[/]", expand=False))
    
    # Create the completion checkpoint for the orchestrator
    (RAW_DATA_DIR / ".audit_complete").touch()

if __name__ == "__main__":
    main()