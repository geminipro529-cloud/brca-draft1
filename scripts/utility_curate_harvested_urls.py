# scripts/utility_curate_harvested_urls.py
# An intelligent script to automate the curation of a large, noisy list of harvested URLs.
# It applies a series of rule-based filters to separate high-value data links from junk,
# preparing a high-quality list for the final Curation Engine.

import pandas as pd
import argparse
from pathlib import Path
from urllib.parse import urlparse
from rich.console import Console
from rich.panel import Panel
import compass_brca.utils.pipeline_config as cfg

console = Console()

# --- Curation Heuristics (The filtering rules) ---
# 1. File extensions that indicate a direct data download
DATA_EXTENSIONS = ['.gz', '.tar', '.zip', '.csv', '.tsv', '.txt', '.cel', '.json', '.idf', '.sdrf', '.maf', '.vcf']

# 2. Keywords that increase the relevance score of a URL
POSITIVE_KEYWORDS = ['brca', 'tcga', 'gse', 'geo', 'nanoparticle', 'biosample', 'biospecimen', 'clinical', 'expression', 'mutation', 'radiology']

# 3. Keywords that identify URLs as likely non-data (e.g., documentation, APIs, web pages)
NEGATIVE_KEYWORDS = ['documentation', 'api', 'login', 'about', 'faq', 'help', 'docs', 'search', '.html', '.php', '.aspx', 'schema']

# 4. Trusted scientific domains that get a higher relevance score
TRUSTED_DOMAINS = ['nih.gov', 'ncbi.nlm.nih.gov', 'ebi.ac.uk', 'zenodo.org', 'gdc.cancer.gov', 'cbioportal.org']

def main():
    parser = argparse.ArgumentParser(description="Intelligently curate a raw list of harvested URLs.")
    parser.add_argument("harvested_file", type=str, help="Path to the raw CSV file from the URL harvester (e.g., 'harvested_urls.csv').")
    args = parser.parse_args()

    harvested_path = Path(args.harvested_file)
    if not harvested_path.exists():
        console.print(f"[bold red]Error:[/bold red] Harvested URL file not found at `{harvested_path}`.")
        return

    console.print(Panel(f"Intelligently Curating URLs from `{harvested_path}`", title="[bold cyan]Automated Curation Filter[/bold cyan]"))
    df = pd.read_csv(harvested_path)
    
    # --- Initial State ---
    initial_count = len(df)
    df = df.dropna(subset=['url']).drop_duplicates(subset=['url']).copy()
    normalized_count = len(df)
    console.print(f"Starting with {initial_count} raw URLs. Found {normalized_count} unique, non-empty URLs.")
    
    # --- Prepare for Filtering ---
    df['rejection_reason'] = 'Kept'
    df_original = df.copy() # Keep a copy for the final rejected log

    # --- Step 1: Filter by Direct Data File Extensions ---
    url_lower = df['url'].str.lower()
    data_ext_mask = url_lower.str.endswith(tuple(DATA_EXTENSIONS), na=False)
    df.loc[~data_ext_mask, 'rejection_reason'] = 'Not a direct data file extension'
    df = df[data_ext_mask]
    console.print(f"  -> Filter 1 (File Type): Kept [green]{len(df)}[/green] URLs pointing to data files.")
    
    # --- Step 2: Filter by Negative Keywords ---
    negative_keyword_mask = url_lower.str.contains('|'.join(NEGATIVE_KEYWORDS), na=False)
    df.loc[negative_keyword_mask, 'rejection_reason'] = 'Contained negative/junk keyword'
    df = df[~negative_keyword_mask] # Keep rows that DO NOT contain negative keywords
    console.print(f"  -> Filter 2 (Junk Words): Kept [green]{len(df)}[/green] URLs after removing junk links.")
    
    # --- Step 3: Score by Relevancy (Positive Keywords & Trusted Domains) ---
    final_df = df.copy()
    final_df['relevance_score'] = 0
    final_df['domain'] = final_df['url'].apply(lambda u: urlparse(u).netloc)

    url_lower_filtered = final_df['url'].str.lower()
    for keyword in POSITIVE_KEYWORDS:
        final_df.loc[url_lower_filtered.str.contains(keyword, na=False), 'relevance_score'] += 1
    
    final_df.loc[final_df['domain'].str.contains('|'.join(TRUSTED_DOMAINS), na=False), 'relevance_score'] += 2 # Trusted domains are more important

    # --- Step 4: Cross-reference with existing raw data ---
    if cfg.RAW_DATA_DIR.exists():
        existing_filenames = {f.name for f in cfg.RAW_DATA_DIR.rglob("*")}
        final_df['already_exists'] = final_df['potential_filename'].isin(existing_filenames)
        # Mark this in the original dataframe for the rejected log
        df_original.loc[final_df[final_df['already_exists']].index, 'rejection_reason'] = 'File already exists in 01_data_raw'
    else:
        final_df['already_exists'] = False

    # --- Final Output Generation ---
    curated_df = final_df # We keep all columns for the next script
    rejected_df = df_original[~df_original.index.isin(curated_df.index)][['url', 'rejection_reason']]

    curated_output_path = Path("curated_potential_downloads.csv")
    rejected_output_path = Path("rejected_urls.csv")
    
    curated_df.to_csv(curated_output_path, index=False)
    rejected_df.to_csv(rejected_output_path, index=False)
    
    console.print(Panel(
        f"Curation complete!\n\n"
        f"  - [bold green]{len(curated_df)}[/bold green] high-value, potential data links saved to: `{curated_output_path}`\n"
        f"  - [bold red]{len(rejected_df)}[/bold red] rejected or existing URLs logged in: `{rejected_output_path}`\n\n"
        f"[bold]Next Step:[/bold] Run the `utility_create_final_manifest.py` script to automatically analyze this curated list and build your final plan.",
        title="[bold green]Automated Curation Filter Successful![/bold green]"
    ))

if __name__ == "__main__":
    main()