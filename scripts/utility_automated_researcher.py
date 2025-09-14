# scripts/utility_automated_researcher.py
# An automated research assistant that programmatically searches PubMed for papers
# relevant to a query, scans them for data accession numbers, and resolves those
# numbers to direct, downloadable data links.

import argparse
import re
import requests
import time
from pathlib import Path
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, BarColumn, TextColumn, TimeElapsedColumn, MofNCompleteColumn, SpinnerColumn
import compass_brca.utils.pipeline_config as cfg

console = Console()

# --- NCBI Entrez API Configuration ---
# It's highly recommended to get a free API key from NCBI for higher rate limits.
# https://www.ncbi.nlm.nih.gov/books/NBK25497/#chapter2.Getting_an_API_Key
NCBI_API_KEY = None 
EMAIL = "your.thesis.email@university.edu"  # Be a good citizen, let NCBI know who you are.
NCBI_API_BASE = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"

# --- Regex Patterns to find data accession numbers in text ---
GEO_ACCESSION_PATTERN = re.compile(r'\b(GSE[0-9]+)\b', re.IGNORECASE)
SRA_ACCESSION_PATTERN = re.compile(r'\b([ES]RP[0-9]+)\b', re.IGNORECASE)
# This can be expanded with more patterns (e.g., for ArrayExpress, ProteomeXchange)

def search_pubmed(query: str, max_results: int) -> list[str]:
    """Searches PubMed and returns a list of PubMed IDs (PMIDs)."""
    console.print(f"Searching PubMed for query: `[cyan]{query}[/]`...")
    search_url = f"{NCBI_API_BASE}esearch.fcgi"
    params = {
        'db': 'pubmed',
        'term': query,
        'retmax': max_results,
        'retmode': 'json',
        'email': EMAIL,
        'api_key': NCBI_API_KEY
    }
    try:
        res = requests.get(search_url, params=params).json()
        pmids = res['esearchresult']['idlist']
        console.print(f"  -> Found {len(pmids)} relevant publications.")
        return pmids
    except (KeyError, IndexError, requests.RequestException) as e:
        console.print(f"[red]Error searching PubMed: {e}[/red]")
        return []

def resolve_single_pmid(pmid: str, progress) -> dict | None:
    """
    Performs the resolution chain for a single PubMed ID.
    PMID -> PMC Link -> Full-Text -> Accession # -> FTP Link
    """
    headers = {'User-Agent': f'COMPASS-BRCA-Researcher ({EMAIL})'}
    
    try:
        # 1. PMID to PubMed Central (PMC) ID
        link_url = f"{NCBI_API_BASE}elink.fcgi?dbfrom=pubmed&db=pmc&id={pmid}&retmode=json&email={EMAIL}&api_key={NCBI_API_KEY}"
        res = requests.get(link_url, headers=headers).json()
        time.sleep(0.34) # Stay within NCBI's 3 requests/second limit
        # Navigate the complex JSON to find the PMC ID
        pmc_id = res['linksets'][0]['linksetdbs'][0]['links'][0]
        pmc_url = f"https://www.ncbi.nlm.nih.gov/pmc/articles/PMC{pmc_id}/"

        # 2. Scrape Full Text page for Accession Numbers
        page_content = requests.get(pmc_url, headers=headers).text
        time.sleep(0.34)
        
        # Find all unique accession numbers in the text
        geo_accessions = set(GEO_ACCESSION_PATTERN.findall(page_content))
        if not geo_accessions:
            return None # No data found in this paper

        # 3. For each GEO Accession, find its FTP link
        resolved_datasets = []
        for gse in geo_accessions:
            # The FTP path format is standardized but can be tricky. This is a common pattern.
            ftp_link = f"https://ftp.ncbi.nlm.nih.gov/geo/series/{gse[:-3]}nnn/{gse}/matrix/{gse}_series_matrix.txt.gz"
            
            # Check if the file actually exists by sending a lightweight HEAD request
            ftp_res = requests.head(ftp_link, headers=headers, timeout=15)
            time.sleep(0.34)
            
            if ftp_res.status_code == 200:
                resolved_datasets.append({
                    "accession": gse,
                    "download_url": ftp_link,
                    "source_pmid": pmid,
                })
                progress.console.print(f"  [green]✓ Success![/green] Found dataset [bold]{gse}[/bold] from PMID {pmid}")
        
        return resolved_datasets if resolved_datasets else None

    except (IndexError, KeyError, requests.RequestException):
        # This is a normal outcome for papers not in PMC or without data links
        return None
    except Exception as e:
        progress.console.print(f"[yellow]Warning:[/yellow] An unexpected error occurred for PMID {pmid}: {e}")
        return None

def main():
    parser = argparse.ArgumentParser(description="Automated researcher to find datasets from PubMed based on a query.")
    parser.add_argument("--query", required=True, type=str, help="The search query for PubMed (e.g., '\"breast cancer\"+AND+\"nanoparticle\"').")
    parser.add_argument("--output-manifest", required=True, help="Path to save the new manifest for discovered files.")
    parser.add_argument("--max-papers", type=int, default=50, help="Maximum number of papers to retrieve from PubMed.")
    parser.add_argument("--workers", type=int, default=3, help="Number of concurrent workers (max 3 without API key for NCBI).")
    args = parser.parse_args()

    # --- Step 1: Search PubMed ---
    pmids = search_pubmed(args.query, args.max_papers)
    if not pmids: return

    # --- Step 2: Concurrently Resolve PMIDs ---
    all_resolved_datasets = []
    with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}"), BarColumn(), MofNCompleteColumn(), TimeElapsedColumn()) as progress:
        task = progress.add_task(f"[cyan]Resolving {len(pmids)} papers...", total=len(pmids))
        
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            futures = {executor.submit(resolve_single_pmid, pmid, progress): pmid for pmid in pmids}
            for future in as_completed(futures):
                result = future.result()
                if result:
                    all_resolved_datasets.extend(result)
                progress.update(task, advance=1)
    
    # --- Step 3: Create the Final Manifest ---
    if not all_resolved_datasets:
        console.print("\n[yellow]Research complete, but no direct data download links could be automatically resolved.[/yellow]")
        return
    
    # Remove duplicate datasets that might be mentioned in multiple papers
    discovered_df = pd.DataFrame(all_resolved_datasets).drop_duplicates(subset=['accession'])

    manifest_data = [{
        "url": row['download_url'],
        "output_path": f"01_data_raw/discovered_from_pubmed/{row['accession']}_series_matrix.txt.gz",
        "data_source": f"GEO_{row['accession']}",
        "file_type": "expression_matrix"
    } for _, row in discovered_df.iterrows()]
    
    manifest_df = pd.DataFrame(manifest_data)
    output_path = Path(args.output_manifest)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_df.to_csv(output_path, index=False)

    console.print(Panel(
        f"Successfully discovered and resolved [bold green]{len(manifest_df)}[/bold green] new datasets.\n"
        f"A new manifest has been created with the direct download links.\n"
        f"  -> [bold]Saved to:[/bold] `{output_path}`\n\n"
        f"[bold]Next Step:[/bold] Append the contents of this file to your master manifest, or use it directly with the `fetch` step.",
        title="[bold green]Automated Research Successful![/bold green]"
    ))

if __name__ == "__main__":
    main()