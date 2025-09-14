# scripts/utility_find_large_source_files.py
# V2.0: Visual Fix for Long Paths
# Purpose: A simple diagnostic tool to find the largest files in the raw data
# directory. This version fixes a visual bug by allowing long file paths to wrap
# correctly in the output table instead of being truncated.

import sys
from pathlib import Path
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
import pandas as pd

# --- Configuration ---
try:
    sys.path.append(str(Path(__file__).resolve().parents[1]))
    from compass_brca.utils.pipeline_config import RAW_DATA_DIR
except ImportError:
    print("Could not import from pipeline_config.py, using script defaults.")
    RAW_DATA_DIR = Path("01_data_raw")

# --- Parameters ---
TOP_N_REPORT = 25

def main():
    """Scans the raw data directory for the largest files on disk."""
    console = Console()
    console.print(Panel(f"[bold cyan]Finding Largest Source Files in -> '{RAW_DATA_DIR}'[/]", expand=False))
    
    if not RAW_DATA_DIR.exists():
        console.print(f"[bold red]Error: Directory not found: {RAW_DATA_DIR}[/]")
        return
        
    all_files = list(RAW_DATA_DIR.rglob("*"))
    files_with_sizes = [
        {"path": f, "size_mb": f.stat().st_size / (1024*1024)}
        for f in all_files if f.is_file() and f.stat().st_size > 0
    ]
    
    if not files_with_sizes:
        console.print("[yellow]No files found to analyze.[/]")
        return
        
    df = pd.DataFrame(files_with_sizes)
    top_files = df.sort_values(by="size_mb", ascending=False).head(TOP_N_REPORT)

    table = Table(title=f"Top {TOP_N_REPORT} Largest Files by Disk Size", header_style="bold magenta")
    
    # --- V2.0 FIX: Set `no_wrap=False` to allow long paths to wrap gracefully ---
    table.add_column("File Path", style="cyan", no_wrap=False) 
    table.add_column("Size (MB)", style="bold red", justify="right")

    for _, row in top_files.iterrows():
        try:
            # Display the path relative to the RAW_DATA_DIR for clarity
            relative_path = row['path'].relative_to(RAW_DATA_DIR)
            table.add_row(
                str(relative_path),
                f"{row['size_mb']:.2f}"
            )
        except ValueError:
            # Fallback for paths that might not be relative (should not happen but is safe)
            table.add_row(
                str(row['path']),
                f"{row['size_mb']:.2f}"
            )
        
    console.print(table)
    console.print(Panel(
        "[bold]Hypothesis:[/bold] The files listed above are the most likely candidates to cause memory errors in `step03` (normalization).\n"
        "If you see large [bold yellow].csv, .tsv, .txt[/] files, they need to be processed with a memory-safe streaming/chunking method during normalization.",
        title="[yellow]Analysis[/]"
    ))

if __name__ == "__main__":
    main()