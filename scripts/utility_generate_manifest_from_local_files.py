# scripts/utility_generate_manifest_from_local_files.py
# DEFINITIVE V4 - Unified Catalog and Manifest Generator
import sys
import pandas as pd
from pathlib import Path

try:
    project_root = Path(__file__).resolve().parents[1]
    sys.path.append(str(project_root / "src"))
    from compass_brca.pipeline_config import RAW_DATA_DIR, MANIFESTS_DIR
except ImportError:
    sys.exit("Fatal Error: Could not import pipeline_config.")

from rich.console import Console
from rich.progress import track
console = Console()

# --- CONFIGURATION ---
VALID_EXTENSIONS = {'.csv', '.tsv', '.xlsx', '.txt', '.cel'}
OUTPUT_FILE = MANIFESTS_DIR / "master_manifest_FINAL.csv"
METADATA_RULES = {
    'tcga': {'destination_folder': 'tcga', 'data_type': 'genomic_clinical', 'priority': 1},
    'gse': {'destination_folder': 'geo_gse', 'data_type': 'gene_expression_series', 'priority': 2},
    'ncl-': {'destination_folder': 'cananolab', 'data_type': 'nanoparticle_characterization', 'priority': 1},
    'zenodo': {'destination_folder': 'zenodo', 'data_type': 'supplementary_data', 'priority': 2},
    'depmap': {'destination_folder': 'depmap_ccle', 'data_type': 'drug_sensitivity', 'priority': 1},
    'gdsc': {'destination_folder': 'gdsc', 'data_type': 'drug_sensitivity', 'priority': 1},
    'clinical': {'destination_folder': 'misc_clinical', 'data_type': 'clinical_data', 'priority': 4},
}
# --- END CONFIGURATION ---

def apply_expert_rules(file_path: Path) -> dict:
    path_str_lower = str(file_path).lower()
    for keyword, metadata in METADATA_RULES.items():
        if keyword in path_str_lower: return metadata
    return {'destination_folder': 'uncategorized', 'data_type': 'unknown', 'priority': 99}

def main():
    console.rule("[bold magenta]Intelligence: Generate Master Manifest from Local Files (V4)[/]")
    if not RAW_DATA_DIR.exists():
        console.print(f"[bold red]Error: Raw data directory not found at {RAW_DATA_DIR}[/]"); sys.exit(1)
        
    MANIFESTS_DIR.mkdir(parents=True, exist_ok=True)
    
    files_to_process = [p for p in RAW_DATA_DIR.rglob("*") if p.is_file() and p.suffix.lower() in VALID_EXTENSIONS]

    if not files_to_process:
        console.print("[yellow]Warning: No valid local data files found to build manifest from.[/]")
        pd.DataFrame(columns=['source_url', 'destination_folder', 'data_type', 'priority']).to_csv(OUTPUT_FILE, index=False); return

    console.print(f"Found [cyan]{len(files_to_process)}[/] local data files. Building manifest...")
    
    manifest_data = []
    for path in track(files_to_process, description="Generating manifest..."):
        metadata = apply_expert_rules(path)
        # Convert the local path to a file URI for the manifest
        row = {'source_url': path.resolve().as_uri(), **metadata}
        manifest_data.append(row)
        
    manifest_df = pd.DataFrame(manifest_data)
    manifest_df.sort_values(by=['priority', 'destination_folder'], inplace=True)
    manifest_df.to_csv(OUTPUT_FILE, index=False)
    
    console.print(f"\n[bold green]SUCCESS![/]")
    console.print(f"  - Created manifest with [cyan]{len(manifest_df)}[/] files.")
    console.print(f"  - Final manifest saved to [green]{OUTPUT_FILE}[/]")
    console.rule("[bold green]Master Manifest Generation Complete[/]")

if __name__ == "__main__":
    main()