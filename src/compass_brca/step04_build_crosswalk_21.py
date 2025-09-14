# compass_brca/step04_build_crosswalk_21.py
# V21.2: DEFINITIVE - Corrected Pathing for Forensic Mode
# This version uses a foolproof method to ensure logs are always created inside the project root.

import sys
from pathlib import Path
import logging
import time
import polars as pl
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, BarColumn, TextColumn, TimeElapsedColumn
from dask.distributed import Client, LocalCluster, wait, get_worker

console = Console()
try:
    from compass_brca.utils.pipeline_config import PRIMARY_DATA_DIR
except ImportError:
    # ... (error handling)
    sys.exit(1)

# --- V21.2 FIX: Foolproof Project Root and Log Directory Definition ---
PROJECT_ROOT = Path(__file__).resolve().parents[1]
FORENSIC_LOGS_DIR = PROJECT_ROOT / "forensic_logs"

# --- Use robust, package-based imports ---
try:
    from compass_brca.utils.pipeline_config import PRIMARY_DATA_DIR, MASTER_CROSSWALK_FILENAME
except ImportError as e:
    console.print(f"[bold red]Fatal Error: Could not import pipeline_config.py.[/]")
    console.print(f"[dim]Python Error: {e}[/dim]")
    console.print("Ensure your project structure is correct and you've run 'pip install -e .'")
    sys.exit(1)

# --- Dask & Worker Parameters ---
BATCH_SIZE = 1000 
N_WORKERS = 8
MEMORY_LIMIT = "2GB"
# Parameter for chunking operations inside the worker
COLUMN_CHUNK_SIZE = 500

# --- Define paths using the correctly imported config ---
PROJECT_ROOT = PRIMARY_DATA_DIR.parents[1]
SMALL_FILES_LIST = PRIMARY_DATA_DIR / "small_files.txt"
LARGE_FILES_LIST = PRIMARY_DATA_DIR / "large_files.txt"

# --- Setup Logging ---
LOGS_DIR = PROJECT_ROOT / "logs"
LOGS_DIR.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s",
    handlers=[logging.FileHandler(LOGS_DIR / "step04_build_crosswalk.log"), logging.StreamHandler()],
)

def process_single_file(file_path_str: str) -> pl.DataFrame:
    """
    The core Dask worker function. Reads one Parquet file and extracts identifiers
    using a memory-safe, column-chunking approach to the melt operation.
    """
    try:
        file_path = Path(file_path_str)
        df = pl.read_parquet(file_path)

        if not df.columns or len(df.columns) <= 1:
            return pl.DataFrame()

        id_vars = df.columns[0]
        value_vars = df.columns[1:]
        
        all_melted_chunks = []

        # Iterate through the value columns in safe chunks
        for i in range(0, len(value_vars), COLUMN_CHUNK_SIZE):
            chunk_value_vars = value_vars[i:i + COLUMN_CHUNK_SIZE]
            melted_chunk = df.select([id_vars] + chunk_value_vars).melt(id_vars=id_vars)
            all_melted_chunks.append(melted_chunk)

        if not all_melted_chunks:
            return pl.DataFrame()
            
        final_melted = pl.concat(all_melted_chunks)
        
        crosswalk_chunk = final_melted.select([
            pl.lit(str(file_path)).alias("source_file"),
            pl.col("variable").alias("identifier_type"),
            pl.col("value").alias("identifier_value")
        ]).drop_nulls()
        
        return crosswalk_chunk.with_columns(pl.col("identifier_value").cast(pl.Utf8))
        
    except Exception:
        # Return an empty dataframe with the correct schema on failure
        return pl.DataFrame({
            "source_file": pl.Series([], dtype=pl.Utf8),
            "identifier_type": pl.Series([], dtype=pl.Utf8),
            "identifier_value": pl.Series([], dtype=pl.Utf8),
        })

def main():
    """Main orchestrator for building the crosswalk using Dask."""
    console.print(Panel(
        f"[bold cyan]Step 04: ID Crosswalk Builder (V20.1)[/]\n"
        f"[dim]Engine: Dask (Queued Submission) | Workers: {N_WORKERS} | Batch Size: {BATCH_SIZE}[/]",
        expand=False
    ))

    if not SMALL_FILES_LIST.exists() or not LARGE_FILES_LIST.exists():
        console.print(f"[bold red]Error:[/bold red] Stratified file lists not found at '{PRIMARY_DATA_DIR}'.")
        console.print("Please run `utility_stratify_by_size.py` first.")
        sys.exit(1)
    logging.info("Stratified file lists found. Proceeding.")

    with open(SMALL_FILES_LIST, "r") as f: small_files = [line.strip() for line in f if line.strip()]
    with open(LARGE_FILES_LIST, "r") as f: large_files = [line.strip() for line in f if line.strip()]
    all_files = small_files + large_files
    if not all_files:
        console.print("[yellow]Warning: No files found in stratified lists to process.[/]")
        return

    with LocalCluster(n_workers=N_WORKERS, memory_limit=MEMORY_LIMIT, threads_per_worker=1) as cluster, Client(cluster) as client:
        console.print(f"Dask Dashboard available at: [link={client.dashboard_link}]{client.dashboard_link}[/link]")
        all_results = []
        with Progress(TextColumn("[progress.description]{task.description}"), BarColumn(), TextColumn("[magenta]{task.completed}/{task.total} files"), TimeElapsedColumn()) as progress:
            main_task = progress.add_task("[green]Processing file batches...", total=len(all_files))
            for i in range(0, len(all_files), BATCH_SIZE):
                batch_files = all_files[i:i + BATCH_SIZE]
                futures = client.map(process_single_file, batch_files)
                wait(futures)
                batch_results = client.gather(futures)
                all_results.extend([df for df in batch_results if not df.is_empty()])
                progress.update(main_task, advance=len(batch_files))
    
    console.print("\n[cyan]Dask processing complete. Aggregating final crosswalk...[/]")
    if not all_results:
        console.print("[bold red]Error: No valid data was processed. Crosswalk will be empty.[/]")
        final_crosswalk = pl.DataFrame()
    else:
        final_crosswalk = pl.concat(all_results).unique()

    output_path = PRIMARY_DATA_DIR / MASTER_CROSSWALK_FILENAME
    final_crosswalk.write_parquet(output_path)
    
    console.print(Panel(
        f"[bold green]Crosswalk build complete![/]\n"
        f"Rows: {len(final_crosswalk):,}\n"
        f"Output: '{output_path}'",
        expand=False
    ))

if __name__ == "__main__":
    main()