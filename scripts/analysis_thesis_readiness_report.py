# scripts/analysis_thesis_readiness_report.py
# DEFINITIVE VERSION V2 - Robust Pathing
import sys
import pandas as pd
from pathlib import Path

# --- ROBUST PATHING ---
# This block ensures the script can find the config file.
# It assumes the script is in the 'scripts' directory of the project.
try:
    project_root = Path(__file__).resolve().parents[1]
    config_path = project_root / "src" / "compass_brca"
    sys.path.append(str(config_path))
    
    from pipeline_config import (
        PRIMARY_DATA_DIR, REPORTS_DIR,
        MASTER_CROSSWALK_PATH, MASTER_VOCABULARY_PATH
    )
except ImportError as e:
    print(f"--- [bold red]Fatal Error[/] ---")
    print(f"Could not import the pipeline configuration.")
    print(f"Please ensure this script is located in the 'scripts' directory of your project.")
    print(f"Error details: {e}")
    sys.exit(1)
# --- END ROBUST PATHING ---

from rich.console import Console
from rich.table import Table
from rich.panel import Panel

console = Console()

# --- KEYWORD DEFINITIONS ---
GENOMIC_KEYWORDS = ['gene', 'rna', 'mutation', 'gse', 'gsm', 'tcga', 'expression', 'tumor', 'somatic', 'omics']
NANOMED_KEYWORDS = ['nano', 'particle', 'drug', 'liposome', 'delivery', 'chemistry', 'payload']
# --- END DEFINITIONS ---

def main():
    """Analyzes the processed data to generate a readiness report."""
    console.rule("[bold magenta]Analysis: Thesis Readiness Report[/]")
    
    if not MASTER_CROSSWALK_PATH.exists() or not MASTER_VOCABULARY_PATH.exists():
        console.print("[bold red]Error: Master crosswalk or vocabulary not found.[/]")
        sys.exit(1)
        
    crosswalk_df = pd.read_parquet(MASTER_CROSSWALK_PATH)
    vocab_df = pd.read_parquet(MASTER_VOCABULARY_PATH)
    console.print(f"Loaded {len(crosswalk_df):,} crosswalk entries and {len(vocab_df):,} vocabulary terms.")

    def classify_term(term):
        term_lower = str(term).lower()
        is_genomic = any(kw in term_lower for kw in GENOMIC_KEYWORDS)
        is_nanomed = any(kw in term_lower for kw in NANOMED_KEYWORDS)
        if is_genomic and is_nanomed: return "Genomic & Nanomedicine"
        if is_genomic: return "Genomic"
        if is_nanomed: return "Nanomedicine"
        return "Other"
    
    vocab_df['domain'] = vocab_df['term'].apply(classify_term)
    report_df = pd.merge(crosswalk_df, vocab_df[['term', 'domain']], left_on='column_name', right_on='term', how='left')
    file_summary = report_df.groupby('file_path')['domain'].apply(lambda x: set(x)).reset_index()

    REPORTS_DIR.mkdir(exist_ok=True)
    
    total_files = file_summary['file_path'].nunique()
    genomic_files = file_summary[file_summary['domain'].apply(lambda s: 'Genomic' in s)]['file_path'].nunique()
    nanomed_files = file_summary[file_summary['domain'].apply(lambda s: 'Nanomedicine' in s)]['file_path'].nunique()
    
    intersection_files = file_summary[
        file_summary['domain'].apply(lambda s: 'Genomic' in s and 'Nanomedicine' in s)
    ]['file_path'].nunique()
    
    console.print(Panel(
        f"Total Files Analyzed in Crosswalk: [bold cyan]{total_files:,}[/]\n\n"
        f"Files with Genomic terms: [bold green]{genomic_files:,}[/] ({genomic_files/total_files:.1%})\n"
        f"Files with Nanomedicine terms: [bold yellow]{nanomed_files:,}[/] ({nanomed_files/total_files:.1%})\n\n"
        f"Files with BOTH Genomic AND Nanomedicine terms: [bold magenta]{intersection_files:,}[/]",
        title="[bold]Thesis Readiness Scorecard[/]",
        expand=False
    ))

    if total_files > 0 and intersection_files == 0:
        console.print("\n[bold red]CRITICAL FINDING:[/]\n[red]No single data source in the crosswalk contains both genomic and nanomedicine data. "
                      "A primary goal must be to find integrated datasets or link these sources.[/]")
    elif intersection_files > 0:
        console.print("\n[bold green]POSITIVE FINDING:[/]\n[green]Integrated datasets found! The next step is a deep dive into these specific files.[/]")
    else:
        console.print("\n[yellow]NOTE:[/]\n[yellow]The crosswalk appears to be empty or contains no classifiable files. Ensure Step 04 ran correctly.[/]")

if __name__ == "__main__":
    main()