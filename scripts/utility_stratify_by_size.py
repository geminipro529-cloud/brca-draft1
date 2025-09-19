# scripts/utility_stratify_by_size.py
# DEFINITIVE VERSION - Standardized for src layout
import sys
from pathlib import Path
from rich.console import Console
from rich.progress import track

# --- IMPORTS ---
# This is the definitive, robust import block.
# It allows the script to find the config file whether run directly or as a subprocess.
try:
    # This will work when the package is installed and the CWD is the project root.
    from src.compass_brca.pipeline_config import INTERIM_DATA_DIR, PRIMARY_DATA_DIR
except ImportError:
    # This is the crucial fallback. It adds the project root to the Python path.
    # We go up two levels from scripts/utility...py to get to the root.
    project_root = Path(__file__).resolve().parents[1]
    sys.path.append(str(project_root))
    from src.compass_brca.pipeline_config import INTERIM_DATA_DIR, PRIMARY_DATA_DIR
# --- END IMPORTS ---

console = Console()

# --- CONFIGURATION ---
LARGE_FILE_THRESHOLD_MB = 50
SMALL_FILES_LIST = PRIMARY_DATA_DIR / "small_files.txt"
LARGE_FILES_LIST = PRIMARY_DATA_DIR / "large_files.txt"
# --- END CONFIGURATION ---

def main():
    """Stratifies Parquet files in the interim directory by size."""
    console.rule("[cyan]Stratifying Interim Files by Size[/]")

    if not INTERIM_DATA_DIR.exists():
        console.print(f"[bold red]Error: Interim data directory not found at {INTERIM_DATA_DIR}[/]")
        sys.exit(1)

    PRIMARY_DATA_DIR.mkdir(exist_ok=True)
    
    small_files, large_files = [], []
    threshold_bytes = LARGE_FILE_THRESHOLD_MB * 1024 * 1024

    all_parquet_files = list(INTERIM_DATA_DIR.rglob("*.parquet"))
    
    if not all_parquet_files:
        console.print(f"[yellow]No .parquet files found in {INTERIM_DATA_DIR} to stratify.[/]")
        # Create empty files to satisfy Step 04
        SMALL_FILES_LIST.touch()
        LARGE_FILES_LIST.touch()
        console.rule("[bold green]Stratification Complete[/]"); return

    console.print(f"Scanning {len(all_parquet_files):,} files...")

    for file_path in track(all_parquet_files, description="Analyzing file sizes..."):
        try:
            if file_path.stat().st_size >= threshold_bytes:
                large_files.append(str(file_path))
            else:
                small_files.append(str(file_path))
        except FileNotFoundError:
            continue # File may have been deleted during processing

    console.print(f"\n[bold]Scan complete.[/]")
    console.print(f"  - Found [green]{len(small_files):,}[/] small files.")
    console.print(f"  - Found [yellow]{len(large_files):,}[/] large files.")

    with open(SMALL_FILES_LIST, "w", encoding="utf-8") as f:
        f.write("\n".join(small_files))
    with open(LARGE_FILES_LIST, "w", encoding="utf-8") as f:
        f.write("\n".join(large_files))
        
    console.print(f"  - File lists saved to [dim]{PRIMARY_DATA_DIR}[/]")
    console.rule("[bold green]Stratification Complete[/]")

if __name__ == "__main__":
    main()