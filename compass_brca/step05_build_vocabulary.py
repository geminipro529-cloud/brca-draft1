# compass_brca/step05_build_vocabulary.py
# V1.1: MULTI-FILE INPUT - Reads multi-file Parquet datasets

import sys
import polars as pl
from rich.console import Console
from rich.panel import Panel

try:
    from compass_brca.pipeline_config import PRIMARY_DATA_DIR, MASTER_VOCABULARY_PATH
except ImportError:
    # ... (error handling is the same)
    sys.exit(1)
    
# --- V1.1 FIX: Point to the dataset directory, not a single file ---
CROSSWALK_DATASET_DIR = PRIMARY_DATA_DIR / "master_crosswalk.dataset"

VOCABULARY_SOURCE_COLUMNS = ["identifier_value"]
EXCLUSION_LIST = ["na", "NA", "nan", "NaN", "null", "NULL", ""]

def main():
    console = Console()
    console.print(Panel("[bold cyan]Step 05: Build Master Vocabulary (V1.1)[/]", expand=False))

    if not CROSSWALK_DATASET_DIR.exists():
        console.print(f"[bold red]Error:[/bold red] Crosswalk dataset directory not found at:")
        console.print(f"[cyan]{CROSSWALK_DATASET_DIR}[/]")
        sys.exit(1)

    # --- V1.1 FIX: Use a glob pattern to read all Parquet files in the directory ---
    crosswalk_path_glob = str(CROSSWALK_DATASET_DIR / "*.parquet")
    console.print(f"Reading crosswalk dataset from: [cyan]{crosswalk_path_glob}[/]")
    
    try:
        # Polars can read a multi-file dataset directly by passing a glob pattern.
        crosswalk_lazy_df = pl.scan_parquet(crosswalk_path_glob)

        console.print(f"Extracting unique terms from column(s): [yellow]{', '.join(VOCABULARY_SOURCE_COLUMNS)}[/]")
        
        vocabulary_df = (
            crosswalk_lazy_df
            .select(VOCABULARY_SOURCE_COLUMNS)
            .unique()
            .filter(~pl.col("identifier_value").is_in(EXCLUSION_LIST))
            .sort("identifier_value")
            .collect()
        )
        vocabulary_df = vocabulary_df.rename({"identifier_value": "term"})

        PRIMARY_DATA_DIR.mkdir(parents=True, exist_ok=True)
        vocabulary_df.write_parquet(MASTER_VOCABULARY_PATH)

        console.print(Panel(
            f"[bold green]Vocabulary build complete![/]\n"
            f"Total unique terms found: {len(vocabulary_df):,}\n"
            f"Output saved to: '[cyan]{MASTER_VOCABULARY_PATH}[/]'",
            expand=False
        ))
    except Exception as e:
        console.print(f"[bold red]An unexpected error occurred:[/bold red] {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()