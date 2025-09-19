# scripts/analysis_deep_dive_on_intersections.py
# Phase 3, Step 2: Investigate the "gold nugget" files.

import sys
import pandas as pd
from pathlib import Path

try:
    project_root = Path(__file__).resolve().parents[1]
    config_path = project_root / "src" / "compass_brca"
    sys.path.append(str(config_path))
    from pipeline_config import PRIMARY_DATA_DIR, INTERIM_DATA_DIR
except ImportError as e:
    print(f"--- [bold red]Fatal Error[/] ---")
    print(f"Could not import the pipeline configuration. Error details: {e}")
    sys.exit(1)

from rich.console import Console
from rich.panel import Panel

console = Console()

# Use the same keywords as the readiness report for consistency
GENOMIC_KEYWORDS = ['gene', 'rna', 'mutation', 'gse', 'gsm', 'tcga', 'expression', 'tumor', 'somatic', 'omics']
NANOMED_KEYWORDS = ['nano', 'particle', 'drug', 'liposome', 'delivery', 'chemistry', 'payload']

def main():
    console.rule("[bold magenta]Analysis: Deep Dive on Intersecting Files[/]")
    
    MASTER_CROSSWALK_PATH = PRIMARY_DATA_DIR / "master_crosswalk.parquet"
    MASTER_VOCABULARY_PATH = PRIMARY_DATA_DIR / "master_vocabulary.parquet"

    if not MASTER_CROSSWALK_PATH.exists() or not MASTER_VOCABULARY_PATH.exists():
        console.print("[bold red]Error: Master crosswalk or vocabulary not found.[/]"); sys.exit(1)
        
    crosswalk_df = pd.read_parquet(MASTER_CROSSWALK_PATH)
    vocab_df = pd.read_parquet(MASTER_VOCABULARY_PATH)

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
    file_summary = report_df.groupby('file_path')['domain'].apply(lambda s: set(x for x in s if x != "Other")).reset_index()

    intersection_files_set = set(file_summary[
        file_summary['domain'].apply(lambda s: 'Genomic' in s and 'Nanomedicine' in s)
    ]['file_path'])

    if not intersection_files_set:
        console.print("[yellow]No intersecting files found to analyze.[/]"); return
        
    console.print(f"Found [bold magenta]{len(intersection_files_set)}[/] prime candidate files. Generating detailed report...")

    for file_path_str in sorted(list(intersection_files_set)):
        # --- For each "gold nugget" file, give a full report ---
        file_path = Path(file_path_str)
        
        # Get all columns for this file from the crosswalk
        columns_in_file = report_df[report_df['file_path'] == file_path_str]
        
        genomic_cols = columns_in_file[columns_in_file['domain'].str.contains("Genomic", na=False)]['column_name'].tolist()
        nanomed_cols = columns_in_file[columns_in_file['domain'].str.contains("Nanomedicine", na=False)]['column_name'].tolist()
        
        report_text = (
            f"Genomic Columns ({len(genomic_cols)}): [green]{', '.join(genomic_cols[:5])}{'...' if len(genomic_cols) > 5 else ''}[/]\n"
            f"Nanomedicine Columns ({len(nanomed_cols)}): [yellow]{', '.join(nanomed_cols[:5])}{'...' if len(nanomed_cols) > 5 else ''}[/]"
        )

        console.print(Panel(
            report_text,
            title=f"[bold]File:[/][cyan] {file_path.name}",
            subtitle=f"[dim]Full Path: {file_path}[/dim]",
            expand=False
        ))

if __name__ == "__main__":
    main()