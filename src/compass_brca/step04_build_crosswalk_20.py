# compass_brca/step04_build_crosswalk_20.py
# V25.0: DEFINITIVE - Selective Extraction Logic
# Purpose: This version abandons the memory-intensive 'unpivot' strategy.
# It intelligently inspects the dataframe's schema and only extracts
# identifiers from non-numeric (string/object) columns, which is faster,
# safer, and scientifically more relevant.

import sys
from pathlib import Path
import logging
import polars as pl
from dask.distributed import Client, LocalCluster, wait
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, BarColumn, TextColumn, TimeElapsedColumn

console = Console()

try:
    from compass_brca.pipeline_config import PRIMARY_DATA_DIR, MASTER_CROSSWALK_FILENAME
except ImportError:
    console.print("[bold red]FATAL ERROR: Could not import pipeline_config.py.[/]")
    sys.exit(1)

# --- Definitive Dask Parameters (Simple & Stable) ---
N_WORKERS = 2
MEMORY_LIMIT = "6GB"
BATCH_SIZE = 500

PROJECT_ROOT = PRIMARY_DATA_DIR.parents[1]
SMALL_FILES_LIST = PRIMARY_DATA_DIR / "small_files.txt"
LARGE_FILES_LIST = PRIMARY_DATA_DIR / "large_files.txt"
LOGS_DIR = PROJECT_ROOT / "logs"
LOGS_DIR.mkdir(exist_ok=True)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s",
    handlers=[logging.FileHandler(LOGS_DIR / "step04_definitive.log"), logging.StreamHandler(sys.stdout)])

# --- V25.0: NEW, INTELLIGENT WORKER FUNCTION ---
def process_single_file(file_path_str: str) -> pl.DataFrame:
    """
    Reads a Parquet file and extracts identifiers ONLY from non-numeric columns.
    This avoids the memory-intensive unpivot on wide numerical data.
    """
    try:
        df = pl.read_parquet(file_path_str)
        
        # Identify columns that are likely to contain categorical identifiers (not numbers)
        string_columns = [col for col in df.columns if df[col].dtype in [pl.Utf8, pl.Categorical, pl.Object]]

        if not string_columns:
            return pl.DataFrame() # No string columns to extract from

        # Create a small dataframe containing only the string columns and their values
        # This is memory-efficient as we are dropping all numeric data.
        crosswalk_chunk = df.select(string_columns).melt().select(
            pl.lit(file_path_str).alias("source_file"),
            pl.col("variable").alias("identifier_type"),
            pl.col("value").alias("identifier_value")
        ).drop_nulls()
        
        return crosswalk_chunk.with_columns(pl.col("identifier_value").cast(pl.Utf8))
        
    except Exception:
        # Return an empty dataframe with the correct schema on any failure
        return pl.DataFrame({
            "source_file": pl.Series([], dtype=pl.Utf8),
            "identifier_type": pl.Series([], dtype=pl.Utf8),
            "identifier_value": pl.Series([], dtype=pl.Utf8),
        })

def main():
    """Main orchestrator for building the crosswalk with the new selective logic."""
    console.print(Panel(
        f"[bold cyan]Step 04: ID Crosswalk Builder (V25.0 - Selective Extraction)[/]\n"
        f"[dim]Engine: Dask | Workers: {N_WORKERS} ({MEMORY_LIMIT} each)[/]",
        expand=False
    ))

    if not SMALL_FILES_LIST.exists():
        console.print("[bold red]Error: Stratified file lists not found.[/]"); sys.exit(1)
    
    with open(SMALL_FILES_LIST, "r") as f: all_files = [line.strip() for line in f if line.strip()]
    if not all_files:
        console.print("[yellow]Warning: No files found to process.[/]"); return

    with LocalCluster(n_workers=N_WORKERS, memory_limit=MEMORY_LIMIT, threads_per_worker=1) as cluster, Client(cluster) as client:
        console.print(f"Dask Dashboard available at: [link={client.dashboard_link}]{client.dashboard_link}[/link]")
        all_results = []
        with Progress(TextColumn("[green]Processing Batches...[/]"), BarColumn(), TextColumn("[magenta]{task.completed}/{task.total} files"), TimeElapsedColumn(), console=console) as progress:
            main_task = progress.add_task("Crosswalk Progress", total=len(all_files))
            for i in range(0, len(all_files), BATCH_SIZE):
                batch_files = all_files[i:i + BATCH_SIZE]
                futures = client.map(process_single_file, batch_files)
                wait(futures)
                try:
                    batch_results = client.gather(futures)
                    all_results.extend([df for df in batch_results if not df.is_empty()])
                except Exception as e:
                    logging.warning(f"A batch failed but the process will continue. Error: {e}")
                progress.update(main_task, advance=len(batch_files))

    console.print("\n[cyan]Dask processing complete. Aggregating final crosswalk...[/]")
    if not all_results:
        final_crosswalk = pl.DataFrame()
    else:
        final_crosswalk = pl.concat(all_results).unique()
    
    output_dir = PRIMARY_DATA_DIR / "master_crosswalk.dataset"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "crosswalk.parquet"
    final_crosswalk.write_parquet(output_path)
    console.print(Panel(f"[bold green]Crosswalk build complete![/]\nRows: {len(final_crosswalk):,}\nOutput: '{output_path}'", expand=False))

if __name__ == "__main__":
    main()