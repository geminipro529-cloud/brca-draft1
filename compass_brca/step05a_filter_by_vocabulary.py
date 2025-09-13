# scripts/step05a_filter_by_vocabulary.py
# A crucial, domain-specific filtering step that uses the master vocabulary
# to remove "off-target noise" from the data. This prepares a smaller,
# high-signal dataset for the subsequent steps.

import pandas as pd
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import logging
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, BarColumn, TextColumn, TimeElapsedColumn, MofNCompleteColumn, SpinnerColumn
import compass_brca.pipeline_config as cfg

# --- Setup ---
console = Console()
LOG_FILE_PATH = cfg.LOGS_DIR / "step05a_filter_by_vocabulary.log"
cfg.LOGS_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', filename=LOG_FILE_PATH, filemode='w')

# --- Heuristics: Define which columns to filter on ---
# These should be columns containing key biological entities.
FILTERABLE_COLUMNS = ['gene', 'hugo_symbol', 'gene_symbol', 'drug_name', 'treatment', 'therapy', 'agent', 'cell_line']

def filter_file(file_path: Path, vocab_set: set, output_dir: Path) -> tuple[str, str, int, int]:
    """
    Filters a single data file, keeping only rows relevant to the master vocabulary.
    """
    try:
        output_file = output_dir / file_path.name
        if output_file.exists():
            # If we want to get the row counts, we need to read the file anyway.
            # For speed, we can just skip if it exists.
            return "skipped", file_path.name, 0, 0

        df = pd.read_parquet(file_path)
        original_rows = len(df)
        if original_rows == 0:
            return "empty", file_path.name, 0, 0

        # Find which of the filterable columns actually exist in this dataframe
        cols_to_filter = [col for col in FILTERABLE_COLUMNS if col in df.columns]
        if not cols_to_filter:
            # If no filterable columns, we can't filter, so we just copy the file
            df.to_parquet(output_file)
            return "no_filterable_cols", file_path.name, original_rows, original_rows

        # --- The Core Filtering Logic ---
        # Create a boolean mask. A row is kept if its value in ANY of the
        # filterable columns is present in our master vocabulary set.
        mask = df[cols_to_filter].isin(vocab_set).any(axis=1)
        df_filtered = df[mask]
        
        filtered_rows = len(df_filtered)
        
        if filtered_rows > 0:
            df_filtered.to_parquet(output_file)
        
        return "success", file_path.name, original_rows, filtered_rows

    except Exception as e:
        logging.error(f"Failed to filter {file_path.name}: {e}")
        return "error", file_path.name, 0, 0

def main():
    # --- New Directory for Filtered Data ---
    FILTERED_DATA_DIR = cfg.PROJECT_ROOT / "02a_data_filtered"
    FILTERED_DATA_DIR.mkdir(exist_ok=True)

    console.print(Panel(f"Filtering interim data using master vocabulary.\n  -> Output will be saved to: `{FILTERED_DATA_DIR}`", title="[bold cyan]Domain-Specific Noise Reduction[/bold cyan]"))

    # --- Load Inputs ---
    if not cfg.VOCABULARY_FILE.exists():
        console.print(f"[bold red]Error:[/bold red] Master vocabulary not found at `{cfg.VOCABULARY_FILE}`. Run step05 first."); return
    
    interim_files = list(cfg.INTERIM_DATA_DIR.glob("*.parquet"))
    if not interim_files:
        console.print("[yellow]No interim files found to filter.[/yellow]"); return

    # Load the vocabulary and convert to a set for extremely fast lookups
    vocab_df = pd.read_parquet(cfg.VOCABULARY_FILE)
    # We use the 'alias' as it represents the original terms in the data
    vocab_set = set(vocab_df['alias'].str.strip().unique())
    console.print(f"Loaded {len(vocab_set)} unique terms into the filter vocabulary.")

    total_original_rows = 0
    total_filtered_rows = 0
    
    with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}"), BarColumn(), MofNCompleteColumn(), TimeElapsedColumn()) as progress:
        task = progress.add_task(f"[cyan]Filtering files...", total=len(interim_files))
        with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
            futures = {executor.submit(filter_file, f, vocab_set, FILTERED_DATA_DIR): f for f in interim_files}
            
            for future in as_completed(futures):
                status, filename, original, filtered = future.result()
                total_original_rows += original
                total_filtered_rows += filtered
                if status == "error":
                    progress.console.print(f"[red]Error processing {filename}. Check log.[/red]")
                progress.update(task, advance=1)

    # --- Final Report ---
    reduction_percent = 0
    if total_original_rows > 0:
        reduction_percent = (1 - (total_filtered_rows / total_original_rows)) * 100

    console.print(Panel(
        f"Filtering complete!\n\n"
        f"  - [bold]Original Total Rows:[/bold] {total_original_rows:,}\n"
        f"  - [bold]Total Rows After Filtering:[/bold] {total_filtered_rows:,}\n"
        f"  - [bold red]Noise Reduction:[/bold] {reduction_percent:.2f}%\n\n"
        f"The filtered, high-signal data is now in `02a_data_filtered/`.\nThe pipeline will now proceed using this cleaner dataset.",
        title="[bold green]Noise Reduction Successful[/bold green]"
    ))

    # Create a checkpoint for this new step
    (FILTERED_DATA_DIR / ".filter_complete").touch()

if __name__ == "__main__":
    main()