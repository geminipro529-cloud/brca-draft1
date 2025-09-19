# src/compass_brca/step06_link_and_fuse_data.py
# DEFINITIVE V6 - The Final Fusion Engine
import sys, pandas as pd
from pathlib import Path
try:
    from compass_brca.pipeline_config import *
except ImportError as e: sys.exit(f"Fatal Error: {e}")
from rich.console import Console
console = Console()

def main():
    console.rule("[bold magenta]Step 06: Final Data Fusion (V6)[/]")
    if not OUTPUT_NANO_FILE.exists() or not OUTPUT_GENOMIC_FILE.exists():
        console.print("[bold red]Error: Clean input files from Step 05 not found![/]"); sys.exit(1)
    FEATURES_FINAL_DIR.mkdir(parents=True, exist_ok=True)
    
    console.print("Loading clean tables from Step 05...")
    df_drug = pd.read_parquet(OUTPUT_NANO_FILE)
    df_genomic = pd.read_parquet(OUTPUT_GENOMIC_FILE)
    
    console.print("Performing final fusion using COSMIC_ID as the bridge...")
    # The COSMIC_ID is the definitive key present in both GDSC and CCLE Model data.
    fused_df = pd.merge(df_drug, df_genomic, on='COSMIC_ID', how='inner')

    if fused_df.empty:
        console.print("[bold red]FUSION FAILED: No common COSMIC_IDs found.[/]"); sys.exit(1)

    fused_df.to_parquet(OUTPUT_FUSED_TABLE, index=False)
    console.print(f"\n[bold green]FUSION SUCCESSFUL![/]")
    console.print(f"  - Created unified feature table with [cyan]{len(fused_df):,}[/] experiments and [cyan]{len(fused_df.columns)}[/] total features.")
    console.print(f"  - Final ML-ready table saved to [green]{OUTPUT_FUSED_TABLE}[/]")
    console.rule("[bold green]Step 06: Final Data Fusion - COMPLETED[/]")

if __name__ == "__main__":
    main()