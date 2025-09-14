# compass_brca/analysis/thesis_readiness_report.py
#
# DEFINITIVE VERSION
# Purpose: Performs a scientific gap analysis on the processed data to determine
# what questions can realistically be answered. It checks the coverage of key
# terms and the linkage between different data domains (e.g., nanoparticles
# and genes).
#
# Input:  master_crosswalk.dataset/ and vocabularies/ from 03_data_primary/
# Output: A report printed to the console and saved to the reports/ directory.

import sys
from pathlib import Path
import polars as pl
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from datetime import datetime

console = Console()

try:
    from compass_brca.pipeline_config import PRIMARY_DATA_DIR, REPORTS_DIR
except ImportError as e:
    console.print(f"[bold red]Fatal Error: Could not import pipeline_config.py.[/]")
    sys.exit(1)

# --- Define Paths ---
CROSSWALK_DATASET_DIR = PRIMARY_DATA_DIR / "master_crosswalk.dataset"
VOCABULARY_DIR = PRIMARY_DATA_DIR / "vocabularies"

# --- Analysis Parameters ---
# Define the key vocabularies we want to check for coverage and linkage.
# These filenames must match the output from the step05 script.
KEY_VOCABULARIES = {
    "gene_symbol": "gene_symbol_vocabulary.parquet",
    "drug_name": "drug_name_vocabulary.parquet",
    "nanoparticle": "nanoparticle_vocabulary.parquet",
}

def main():
    """Main function to generate the thesis readiness report."""
    console.rule("[bold magenta]Analysis: Thesis Readiness Report[/bold magenta]")
    
    # --- Pre-flight Checks ---
    if not CROSSWALK_DATASET_DIR.exists() or not VOCABULARY_DIR.exists():
        console.print("[bold red]Error:[/bold red] Required data directories not found in '03_data_primary'.")
        console.print("Please run the full processing pipeline (steps 04 and 05) first.")
        sys.exit(1)

    # Prepare for report generation
    REPORTS_DIR.mkdir(exist_ok=True)
    report_timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
    report_path = REPORTS_DIR / f"thesis_readiness_report_{report_timestamp}.md"
    report_lines = ["# Thesis Readiness Report", f"Generated: {report_timestamp}\n"]

    # --- Load Data ---
    console.print("Loading crosswalk data...")
    crosswalk_df = pl.read_parquet(str(CROSSWALK_DATASET_DIR / "*.parquet"))
    
    vocab_data = {}
    for domain, filename in KEY_VOCABULARIES.items():
        vocab_path = VOCABULARY_DIR / filename
        if vocab_path.exists():
            vocab_data[domain] = pl.read_parquet(vocab_path)["term"].to_list()
        else:
            console.print(f"[yellow]Warning: Vocabulary file not found, skipping: {filename}[/yellow]")

    # --- Analysis 1: Vocabulary Coverage ---
    console.print("Analyzing vocabulary coverage...")
    report_lines.append("## 1. Vocabulary Coverage\n")
    table = Table(title="Coverage of Key Scientific Domains")
    table.add_column("Domain", style="cyan"); table.add_column("Total Terms", justify="right"); table.add_column("Terms Found in Data", justify="right"); table.add_column("Coverage %", justify="right")

    for domain, terms in vocab_data.items():
        found_terms = crosswalk_df.filter(pl.col("identifier_value").is_in(terms))["identifier_value"].n_unique()
        coverage = (found_terms / len(terms)) * 100 if terms else 0
        table.add_row(domain, f"{len(terms):,}", f"{found_terms:,}", f"{coverage:.1f}%")
        report_lines.append(f"- **{domain.replace('_', ' ').title()}:** {found_terms:,} of {len(terms):,} expert terms found in the dataset ({coverage:.1f}% coverage).")

    console.print(table)
    report_lines.append("\n")
    
    # --- Analysis 2: Data Linkage ---
    console.print("Analyzing data linkage between domains...")
    report_lines.append("## 2. Data Linkage Analysis\n")
    report_lines.append("This analysis checks for files that contain terms from multiple domains, which is essential for finding correlations.\n")
    
    # Check if nanoparticle vocabulary is loaded before proceeding
    if "nanoparticle" not in vocab_data:
        console.print("[yellow]Warning:[/yellow] Nanoparticle vocabulary not found. Skipping linkage analysis.")
        report_lines.append("- **SKIPPED:** Nanoparticle vocabulary not found. Linkage analysis could not be performed.")
        nano_files = []
    else:
        # Find files that contain at least one nanoparticle term
        nano_files = crosswalk_df.filter(pl.col("identifier_value").is_in(vocab_data.get("nanoparticle", [])))["source_file"].unique().to_list()
    if not nano_files:
        console.print("[red]Critical Finding:[/red] No files containing nanoparticle terms were found. Linkage analysis cannot proceed.")
        report_lines.append("- **CRITICAL:** No files with nanoparticle terms found.")
    else:
        # From within those nanoparticle-containing files, how many ALSO contain genes or drugs?
        linked_df = crosswalk_df.filter(pl.col("source_file").is_in(nano_files))
        
        linked_gene_files = linked_df.filter(pl.col("identifier_value").is_in(vocab_data.get("gene_symbol", [])))["source_file"].n_unique()
        linked_drug_files = linked_df.filter(pl.col("identifier_value").is_in(vocab_data.get("drug_name", [])))["source_file"].n_unique()
        
        linkage_table = Table(title="Linkage Between Nanoparticles and Other Domains")
        linkage_table.add_column("Linkage", style="cyan"); linkage_table.add_column("File Count", justify="right")
        linkage_table.add_row("Files containing any Nanoparticle term", f"{len(nano_files):,}")
        linkage_table.add_row("...of which, ALSO contain a Gene Symbol", f"{linked_gene_files:,}")
        linkage_table.add_row("...of which, ALSO contain a Drug Name", f"{linked_drug_files:,}")
        
        console.print(linkage_table)
        report_lines.append(f"- Found **{len(nano_files):,}** source files containing at least one Nanoparticle term.")
        report_lines.append(f"- Of these, **{linked_gene_files:,}** files also contain a Gene Symbol (potential for nanoparticle-gene correlation analysis).")
        report_lines.append(f"- Of these, **{linked_drug_files:,}** files also contain a Drug Name (potential for nanoparticle-drug correlation analysis).")

    # --- Save and Conclude ---
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("\n".join(report_lines))
        
    console.print(Panel(f"[bold green]Thesis Readiness Report complete![/]\nFull report saved to: '[cyan]{report_path}[/]'", expand=False))

if __name__ == "__main__":
    main()