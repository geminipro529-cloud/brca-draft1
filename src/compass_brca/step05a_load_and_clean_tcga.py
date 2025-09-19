# src/compass_brca/step05a_load_and_clean_tcga.py
# DEFINITIVE V5 - "Content-Aware" Analyst
import sys
import pandas as pd
from pathlib import Path

# --- ROBUST IMPORTS ---
try:
    project_root = Path(__file__).resolve().parents[2]
    sys.path.append(str(project_root / "src"))
    from compass_brca.pipeline_config import RAW_DATA_DIR, PRIMARY_DATA_DIR, OUTPUT_TCGA_FEATURES
except ImportError as e:
    sys.exit(f"Fatal Error: Could not import config from 'src/compass_brca/pipeline_config.py'. Details: {e}")
# --- END IMPORTS ---

from rich.console import Console
from rich.progress import track
console = Console()

# --- CONFIGURATION (UPDATED) ---
# The script will now search the ENTIRE raw data directory.
TCGA_SEARCH_DIR = RAW_DATA_DIR
# The critical column headers that uniquely identify a TCGA mutation file.
REQUIRED_COLUMNS = {"Hugo_Symbol", "Tumor_Sample_Barcode"}
NUM_TOP_GENES = 150
# --- END CONFIGURATION ---

def find_mutation_file_by_content(search_dir: Path) -> Path:
    """
    Finds the correct mutation file by reading the headers of all text files,
    not by relying on the filename. This is the definitive method.
    """
    console.print(f"Scanning all text files within [dim]{search_dir}[/] for required columns...")
    text_files = [p for p in search_dir.rglob("*") if p.is_file() and p.suffix.lower() in ['.csv', '.tsv', '.txt']]
    
    for file_path in track(text_files, description="Analyzing file headers..."):
        try:
            separator = '\t' if file_path.suffix.lower() in ['.tsv', '.txt'] else ','
            # Read only the header row to be fast and memory-safe
            headers = pd.read_csv(file_path, sep=separator, comment='#', nrows=0, low_memory=False).columns
            if REQUIRED_COLUMNS.issubset(headers):
                console.print(f"\n[green]  ✓ Found definitive mutation data in file:[/][cyan] {file_path.name}[/]")
                return file_path
        except Exception:
            # Silently ignore files that are unreadable or not tabular
            continue
    return None

def main():
    console.rule("[bold magenta]Step 05a: Load & Clean TCGA Mutation Data (Content-Aware V5)[/]")
    
    if not TCGA_SEARCH_DIR.exists():
        console.print(f"[bold red]Error: Raw data directory not found at {TCGA_SEARCH_DIR}[/]"); sys.exit(1)
        
    PRIMARY_DATA_DIR.mkdir(parents=True, exist_ok=True)

    tcga_mutation_file_path = find_mutation_file_by_content(TCGA_SEARCH_DIR)

    if not tcga_mutation_file_path:
        console.print(f"\n[bold red]Error: Could not find any file in your raw data containing the required columns: {REQUIRED_COLUMNS}[/]"); sys.exit(1)
        
    console.print(f"Loading raw TCGA mutation data from: [cyan]{tcga_mutation_file_path.name}[/]")
    separator = '\t' if tcga_mutation_file_path.suffix.lower() in ['.tsv', '.txt'] else ','
    df_long = pd.read_csv(tcga_mutation_file_path, sep=separator, comment='#', low_memory=False)

    console.print(f"Loaded {len(df_long):,} total mutation records.")
    
    console.print("\n[yellow]Engineering Features (Pivoting Data)...[/]")
    # We must check if the required columns are still in the dataframe after full load
    if not REQUIRED_COLUMNS.issubset(df_long.columns):
        console.print(f"[bold red]CRITICAL ERROR: The identified file '{tcga_mutation_file_path.name}' is missing required columns after full load. This may indicate a malformed file.[/]"); sys.exit(1)

    df_subset = df_long[['Tumor_Sample_Barcode', 'Hugo_Symbol']].copy()
    df_subset['mutated'] = 1
    top_genes = df_subset['Hugo_Symbol'].value_counts().nlargest(NUM_TOP_GENES).index.tolist()
    console.print(f"Identified the {len(top_genes)} most frequently mutated genes as features.")
    df_filtered = df_subset[df_subset['Hugo_Symbol'].isin(top_genes)]

    console.print("Pivoting data to create the 'chessboard' feature table...")
    df_features = df_filtered.pivot_table(index='Tumor_Sample_Barcode', columns='Hugo_Symbol', values='mutated', fill_value=0)
    df_features.reset_index(inplace=True)
    df_features.rename(columns={'Tumor_Sample_Barcode': 'sample_id'}, inplace=True)

    df_features.to_parquet(OUTPUT_TCGA_FEATURES, index=False)
    
    console.print(f"\n[bold green]SUCCESS![/]")
    console.print(f"  - Created a TCGA feature table with [cyan]{len(df_features)}[/] samples and [cyan]{len(df_features.columns) - 1}[/] gene features.")
    console.print(f"  - Saved to [green]{OUTPUT_TCGA_FEATURES}[/]")
    
    console.rule("[bold green]Step 05a: Load & Clean TCGA Mutation Data - COMPLETED[/]")

if __name__ == "__main__":
    main()