# src/compass_brca/step05_build_ccle_features.py
# DEFINITIVE V5 - Rich Feature & Key Extractor
import sys, pandas as pd
from pathlib import Path

try:
    from compass_brca.pipeline_config import RAW_DATA_DIR, PRIMARY_DATA_DIR, OUTPUT_NANO_FILE, OUTPUT_GENOMIC_FILE
except ImportError as e: sys.exit(f"Fatal Error: {e}")
from rich.console import Console
console = Console()

GDSC_PATH = RAW_DATA_DIR / "targeted_hunt_cycle2/GDSC2_public_raw_data_27Oct23.csv/GDSC2_public_raw_data_27Oct23.csv"
CCLE_MODEL_PATH = RAW_DATA_DIR / "targeted_hunt_cycle2/Model.csv"

def process_gdsc(file_path: Path) -> pd.DataFrame:
    # This function is correct and does not need changes.
    console.print(f"  -> Processing GDSC data from: [dim]{file_path.name}[/dim]")
    use_cols = ['CELL_LINE_NAME', 'DRUG_ID', 'CONC', 'INTENSITY', 'COSMIC_ID']
    df = pd.read_csv(file_path, usecols=use_cols, low_memory=False)
    df.rename(columns={'CELL_LINE_NAME': 'cell_line_name', 'DRUG_ID': 'drug_id', 'CONC': 'concentration_molar', 'INTENSITY': 'outcome_viability_intensity', 'COSMIC_ID': 'COSMIC_ID'}, inplace=True)
    return df

def process_ccle_model(file_path: Path) -> pd.DataFrame:
    console.print(f"  -> Processing CCLE Model info from: [dim]{file_path.name}[/dim]")
    df = pd.read_csv(file_path, low_memory=False)
    mutation_cols = [col for col in df.columns if '_mutation' in col]
    # We now add COSMICID to our list of essential columns
    use_cols = ['CellLineName', 'OncotreeLineage', 'OncotreeSubtype', 'COSMICID'] + mutation_cols
    df_subset = df[[col for col in use_cols if col in df.columns]].copy()
    df_subset.rename(columns={'CellLineName': 'cell_line_name', 'OncotreeLineage': 'lineage', 'OncotreeSubtype': 'subtype', 'COSMICID': 'COSMIC_ID'}, inplace=True)
    for col in mutation_cols:
        if col in df_subset.columns:
            df_subset[col] = (df_subset[col] != 0).astype(int)
    return df_subset

def main():
    console.rule("[bold magenta]Step 05: Build Rich Feature & Key Tables (V5)[/]")
    PRIMARY_DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    console.print("\n[yellow]Processing Drug Sensitivity Datasets...[/]")
    if GDSC_PATH.exists():
        df_gdsc = process_gdsc(GDSC_PATH)
        df_gdsc.to_parquet(OUTPUT_NANO_FILE, index=False)
        console.print(f"[green]✓ Clean drug data saved.[/] ({len(df_gdsc)} rows)")
    
    console.print("\n[yellow]Processing Rich Genomic & Key Datasets...[/]")
    if CCLE_MODEL_PATH.exists():
        df_ccle_model = process_ccle_model(CCLE_MODEL_PATH)
        df_breast = df_ccle_model[df_ccle_model['lineage'] == 'Breast'].copy()
        df_breast.to_parquet(OUTPUT_GENOMIC_FILE, index=False)
        console.print(f"[green]✓ Clean, rich genomic & key data saved.[/] ({len(df_breast)} breast cell lines, {len(df_breast.columns)} features)")
        
    console.rule("[bold green]Step 05: Build Feature & Key Tables - COMPLETED[/]")

if __name__ == "__main__":
    main()