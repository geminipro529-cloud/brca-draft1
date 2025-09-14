# scripts/utility_stratify_by_size.py
# V3.0: CRASH-PROOF - Removes Rich progress bar to prevent Unicode errors.

import sys
from pathlib import Path
from rich.console import Console
from rich.panel import Panel

console = Console()
try:
    from compass_brca.utils.pipeline_config import INTERIM_DATA_DIR, PRIMARY_DATA_DIR
except ImportError:
    console.print("Failed to import from pipeline_config. Using script defaults.")
    INTERIM_DATA_DIR = Path("02_data_interim")
    PRIMARY_DATA_DIR = Path("03_data_primary")

LARGE_FILE_THRESHOLD_MB = 50 
SMALL_FILES_LIST = PRIMARY_DATA_DIR / "small_files.txt"
LARGE_FILES_LIST = PRIMARY_DATA_DIR / "large_files.txt"

def main():
    console.print(Panel(f"[bold cyan]Stratifying Files by Size in -> '{INTERIM_DATA_DIR}'[/]", expand=False))
    if not INTERIM_DATA_DIR.exists():
        sys.exit(1)
    PRIMARY_DATA_DIR.mkdir(exist_ok=True)
    small_files, large_files = [], []
    threshold_bytes = LARGE_FILE_THRESHOLD_MB * 1024 * 1024

    file_generator = INTERIM_DATA_DIR.rglob("*.parquet")
    
    # --- V3.0 FIX: Use a simple, crash-proof progress indicator ---
    total_files = 0
    for i, file_path in enumerate(file_generator):
        if i % 5000 == 0: # Print a status update every 5000 files
            print(f"  ...scanned {i:,} files", flush=True)
        if not file_path.is_file(): continue
        try:
            if file_path.stat().st_size >= threshold_bytes:
                large_files.append(str(file_path))
            else:
                small_files.append(str(file_path))
            total_files += 1
        except FileNotFoundError:
            continue
            
    print(f"  ...scan complete. Total files found: {total_files:,}")

    console.print(f"Scan complete. Found [green]{len(small_files):,}[/] small files and [yellow]{len(large_files):,}[/] large files.")
    with open(SMALL_FILES_LIST, "w") as f: f.write("\n".join(small_files))
    with open(LARGE_FILES_LIST, "w") as f: f.write("\n".join(large_files))
    console.print(Panel("[bold green]Stratification complete. You may now run step04.[/]", expand=False))

if __name__ == "__main__":
    main()