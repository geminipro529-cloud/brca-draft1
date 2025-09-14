# scripts/utility_create_final_manifest.py
# V2.0: An expert system script that automates the "human-in-the-loop" curation step.
# Now includes intelligent guessing for data_source and file_type for web URLs.

import pandas as pd
import argparse
from pathlib import Path
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
import compass_brca.utils.pipeline_config as cfg

console = Console()

# --- Master Bioinformatician Heuristics ---
CONFIDENCE_THRESHOLD = 3
HIGH_CONFIDENCE_PATTERNS = ['_series_matrix.txt.gz']
MEDIUM_CONFIDENCE_KEYWORDS = ['clinical', 'expression', 'patient', 'sample']

# --- Intelligent Guessing Logic (copied from other utilities) ---
def guess_source_and_type(url: str, filename: str) -> tuple[str, str]:
    """Makes an educated guess about a file's data_source and file_type from its URL."""
    url_lower = url.lower()
    name_lower = filename.lower()
    
    # Guess data_source
    if "brca" in url_lower or "tcga" in url_lower: data_source = "TCGA-BRCA-Web"
    elif "nano" in url_lower: data_source = "Nanomedicine-Web"
    elif "geo" in url_lower or "gse" in url_lower: data_source = "GEO-Web"
    else: data_source = "Unknown-Web"

    # Guess file_type
    if "clinic" in name_lower: file_type = "clinical"
    elif "expression" in name_lower or ".cel" in name_lower: file_type = "expression"
    elif "mutation" in name_lower or ".vcf" in name_lower or ".maf" in name_lower: file_type = "mutation"
    elif "manifest" in name_lower: file_type = "metadata_manifest"
    else: file_type = "experimental_raw"
        
    return data_source, file_type

def apply_expert_judgment(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    df['final_score'] = df['relevance_score']
    df['selection_reason'] = ''

    for pattern in HIGH_CONFIDENCE_PATTERNS:
        df.loc[df['potential_filename'].str.contains(pattern, na=False), 'final_score'] += 5
    for keyword in MEDIUM_CONFIDENCE_KEYWORDS:
        df.loc[df['potential_filename'].str.contains(keyword, na=False), 'final_score'] += 1

    selected_mask = df['final_score'] > CONFIDENCE_THRESHOLD
    df.loc[selected_mask, 'selection_reason'] = f"Score > {CONFIDENCE_THRESHOLD}"
    
    return df[selected_mask], df[~selected_mask]

def main():
    legacy_manifest_path = Path("00_manifests/generated_legacy_manifest.csv")
    curated_urls_path = Path("curated_potential_downloads.csv")
    final_manifest_path = Path("00_manifests/master_manifest_FINAL.csv")

    if not legacy_manifest_path.exists() or not curated_urls_path.exists():
        console.print(f"[bold red]Error:[/bold red] Required input files not found!")
        return

    console.print(Panel(f"Automating Final Manifest Creation", title="[bold cyan]Master Curation Engine V2.0[/bold cyan]"))

    df_legacy = pd.read_csv(legacy_manifest_path)
    df_curated = pd.read_csv(curated_urls_path)
    console.print(f"Loaded {len(df_legacy)} existing local file entries.")
    console.print(f"Loaded {len(df_curated)} potential new web URLs to evaluate.")

    df_selected, df_discarded = apply_expert_judgment(df_curated)
    console.print(f"Applying expert heuristics...")
    console.print(f"  -> Auto-selected [bold green]{len(df_selected)}[/bold green] high-confidence web URLs for download.")
    console.print(f"  -> Auto-discarded [yellow]{len(df_discarded)}[/yellow] lower-confidence web URLs.")

    if not df_selected.empty:
        # --- CRITICAL FIX IS HERE ---
        # Apply the guessing function to create the missing columns
        guesses = df_selected.apply(
            lambda row: guess_source_and_type(row['url'], row['potential_filename']),
            axis=1,
            result_type='expand'
        )
        guesses.columns = ['data_source', 'file_type']
        
        # Join the new guessed columns back to the selected dataframe
        selected_web_urls = df_selected.join(guesses)

        # Re-create the output_path for the selected web URLs
        selected_web_urls['output_path'] = selected_web_urls['potential_filename'].apply(
            lambda name: (cfg.RAW_DATA_DIR / "recovered_web" / name).as_posix().replace('\\', '/')
        )
        
        core_columns = ['url', 'output_path', 'data_source', 'file_type']
        
        # Combine the existing local files with the newly selected and annotated web files
        final_manifest_df = pd.concat([
            df_legacy[core_columns],
            selected_web_urls[core_columns]
        ], ignore_index=True)
    else:
        # If no web URLs were selected, the final manifest is just the legacy one
        final_manifest_df = df_legacy

    final_manifest_df.to_csv(final_manifest_path, index=False)

    report_table = Table(title="Auto-Curation Decision Report (Top 10 Selected)")
    report_table.add_column("URL", style="cyan", no_wrap=True)
    report_table.add_column("Filename", style="magenta")
    report_table.add_column("Final Score", style="green", justify="right")

    if not df_selected.empty:
        for _, row in df_selected.head(10).iterrows():
            report_table.add_row(row['url'], row['potential_filename'], f"{row['final_score']:.0f}")

    console.print(report_table)

    console.print(Panel(
        f"A final, merged manifest has been automatically created.\n"
        f"  - [bold]Total Entries:[/bold] {len(final_manifest_df)}\n"
        f"  - [bold]Saved to:[/bold] `{final_manifest_path}`\n\n"
        f"[bold]Next Step:[/bold] You are ready to run the main workflow. This manifest is your final plan.",
        title="[bold green]Final Manifest Created Successfully![/bold green]"
    ))

if __name__ == "__main__":
    main()