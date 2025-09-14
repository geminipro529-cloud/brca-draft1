# compass_brca/step03_clean_and_normalize.py
# V2.2: DEFINITIVE - With Filename Sanitization
# This version includes a robust function to clean and sanitize source filenames
# before writing the output, preventing corrupted or invalid paths that crash Dask.

import sys
from pathlib import Path
import polars as pl
from rich.console import Console
from rich.panel import Panel
import re

console = Console()
try:
    from compass_brca.utils.pipeline_config import RAW_DATA_DIR, CHUNKED_DIR, INTERIM_DATA_DIR
except ImportError:
    # ... (fallback)
    sys.exit(1)

STREAMING_CHUNK_SIZE = 500_000
STREAMING_THRESHOLD_MB = 100

# --- V2.2 DEFINITIVE FIX: Filename Sanitization Function ---
def sanitize_filename(filename: str) -> str:
    """Creates a safe, valid filename."""
    # Remove any path components, just keep the filename
    base_name = Path(filename).name
    # Replace multiple dots and invalid characters with a single underscore
    sanitized = re.sub(r'[\s\.\\\/:*?"<>|]+', '_', base_name)
    return f"{sanitized}.parquet"

# (The helper functions `handle_duplicate_columns`, `normalize_large_file`,
# and `normalize_small_file` are unchanged and should be here.)
# ...

def main():
    console.print(Panel("[bold cyan]Step 03: Clean & Normalize (V2.2 - Sanitized)[/]", expand=False))
    INTERIM_DATA_DIR.mkdir(exist_ok=True)
    
    source_dirs = [RAW_DATA_DIR, CHUNKED_DIR]
    files_to_process = [f for s_dir in source_dirs if s_dir.exists() for f in s_dir.rglob("*") if f.is_file()]
    total_files = len(files_to_process)
    
    console.print(f"Found {total_files:,} total files to process.")

    for i, source_path in enumerate(files_to_process):
        if (i + 1) % 100 == 0:
            print(f"\r  ...processing file {i+1:,} of {total_files:,}", end='', flush=True)
        
        # --- V2.2 FIX: Use the sanitization function to create the destination path ---
        sanitized_name = sanitize_filename(source_path.name)
        dest_path = INTERIM_DATA_DIR / sanitized_name
        
        if dest_path.exists(): continue

        file_size_mb = source_path.stat().st_size / (1024 * 1024)

        if file_size_mb > STREAMING_THRESHOLD_MB and source_path.suffix.lower() in ['.csv', '.tsv', '.txt']:
            print("\r" + " " * 80 + "\r", end='')
            console.print(f"\n[yellow]Processing large file '{source_path.name}'...[/]")
            normalize_large_file(source_path, dest_path)
        else:
            normalize_small_file(source_path, dest_path)

    print("\r" + " " * 80 + "\r", end='')
    console.print(Panel("[bold green]Cleaning and normalization complete.[/]", expand=False))

if __name__ == "__main__":
    # You would need to paste the full helper functions here for this to be runnable standalone
    main()
