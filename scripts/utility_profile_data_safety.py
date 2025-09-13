# scripts/utility_profile_data_safety.py
# Purpose: A read-only diagnostic tool to analyze the safety metrics of raw data.
# It scans all downloaded Parquet files, calculates their potential in-memory size
# and expansion ratio, and reports the "top offenders". This helps determine
# the correct thresholds for the step01 ingestion script.
# Input: Files in 01_data_raw/
# Output: A summary table of the most dangerous files printed to the console.

import sys
from pathlib import Path
import pyarrow.parquet as pq
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
import pandas as pd

# --- Configuration ---
try:
    sys.path.append(str(Path(__file__).resolve().parents[1]))
    from compass_brca.pipeline_config import RAW_DATA_DIR
except ImportError:
    print("Could not import from pipeline_config.py, using script defaults.")
    RAW_DATA_DIR = Path("01_data_raw")

# --- Parameters ---
# How many of the "worst" files to show in the report
TOP_N_REPORT = 20

def get_safety_metrics(file_path: Path) -> dict | None:
    """Safely reads a Parquet file's metadata and returns its safety metrics."""
    try:
        if not file_path.name.endswith(('.parquet', '.parq')):
            return None

        on_disk_size = file_path.stat().st_size
        if on_disk_size == 0:
            return None

        parquet_file = pq.ParquetFile(file_path)
        metadata = parquet_file.metadata
        uncompressed_size = sum(
            metadata.row_group(i).total_byte_size
            for i in range(metadata.num_row_groups)
        )
        expansion_ratio = uncompressed_size / on_disk_size if on_disk_size > 0 else 0
        
        return {
            "filename": file_path.name,
            "disk_size_mb": on_disk_size / (1024*1024),
            "uncompressed_size_mb": uncompressed_size / (1024*1024),
            "expansion_ratio": expansion_ratio,
        }
    except Exception:
        # Silently skip files that can't be read, as they are a different problem
        return None

def main():
    """Main function to scan, analyze, and report on data safety metrics."""
    console = Console()
    console.print(Panel(f"[bold cyan]Profiling Data Safety Metrics in -> '{RAW_DATA_DIR}'[/]", expand=False))
    
    if not RAW_DATA_DIR.exists():
        console.print(f"[bold red]Error: Directory not found: {RAW_DATA_DIR}[/]")
        return
        
    all_files = list(RAW_DATA_DIR.rglob("*"))
    parquet_files = [f for f in all_files if f.is_file()]
    
    if not parquet_files:
        console.print("[yellow]No Parquet files found to analyze.[/]")
        return
        
    console.print(f"Analyzing {len(parquet_files):,} files. This may take a moment...")
    
    # --- Analysis ---
    all_metrics = [get_safety_metrics(f) for f in parquet_files]
    # Filter out None values from files that were skipped or failed
    valid_metrics = [m for m in all_metrics if m is not None]
    
    if not valid_metrics:
        console.print("[bold red]Error: Could not extract safety metrics from any files.[/]")
        return
        
    df = pd.DataFrame(valid_metrics)

    # --- Reporting ---
    # Report 1: Top offenders by UNCOMPRESSED SIZE
    top_by_size = df.sort_values(by="uncompressed_size_mb", ascending=False).head(TOP_N_REPORT)
    
    table_size = Table(title=f"Top {TOP_N_REPORT} Files by Potential In-Memory Size", header_style="bold magenta")
    table_size.add_column("Filename", style="cyan")
    table_size.add_column("Disk Size (MB)", style="yellow", justify="right")
    table_size.add_column("Uncompressed Size (MB)", style="bold red", justify="right")
    table_size.add_column("Expansion Ratio", style="green", justify="right")
    
    for _, row in top_by_size.iterrows():
        table_size.add_row(
            row['filename'],
            f"{row['disk_size_mb']:.2f}",
            f"{row['uncompressed_size_mb']:.2f}",
            f"{row['expansion_ratio']:.1f}x"
        )
    
    # Report 2: Top offenders by EXPANSION RATIO
    top_by_ratio = df.sort_values(by="expansion_ratio", ascending=False).head(TOP_N_REPORT)

    table_ratio = Table(title=f"Top {TOP_N_REPORT} Files by Expansion Ratio", header_style="bold magenta")
    table_ratio.add_column("Filename", style="cyan")
    table_ratio.add_column("Disk Size (MB)", style="yellow", justify="right")
    table_ratio.add_column("Uncompressed Size (MB)", style="red", justify="right")
    table_ratio.add_column("Expansion Ratio", style="bold green", justify="right")

    for _, row in top_by_ratio.iterrows():
        table_ratio.add_row(
            row['filename'],
            f"{row['disk_size_mb']:.2f}",
            f"{row['uncompressed_size_mb']:.2f}",
            f"{row['expansion_ratio']:.1f}x"
        )

    console.print(table_size)
    console.print(table_ratio)
    
    console.print(Panel(
        "[bold]Recommendation:[/bold] Examine the [bold red]'Uncompressed Size (MB)'[/] and [bold green]'Expansion Ratio'[/] of the files listed above.\n"
        "Choose new thresholds for `MAX_UNCOMPRESSED_SIZE_MB` and `MIN_EXPANSION_RATIO` in the `step01` script that are lower than these top offenders.",
        title="[yellow]Action Required[/]"
    ))

if __name__ == "__main__":
    main()