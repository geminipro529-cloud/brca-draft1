# scripts/utility_resolve_dois_to_data.py
# An advanced, concurrent utility to resolve a list of publication DOIs to
# direct, raw data download links from public repositories like NCBI GEO.

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
NCBI_API_BASE = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"
NCBI_API_KEY = None  # It's better to get an API key for higher rate limits
EMAIL = "your.email@example.com"  # NCBI requests a contact email

# --- Regex Patterns to find data accession numbers in text ---
GEO_ACCESSION_PATTERN = re.compile(r'\b(GSE[0-9]+)\b')
SRA_ACCESSION_PATTERN = re.compile(r'\b([ES]RP[0-9]+)\b')
# Can be expanded with more patterns (e.g., for ArrayExpress)

def resolve_single_doi(doi: str) -> dict | None:
    """
    Performs the entire resolution chain for a single DOI.
    DOI -> PMID -> Full-Text -> Accession -> FTP Link
    """
    headers = {'User-Agent': f'Python-DOI-Resolver ({EMAIL})'}
    
    try:
        # 1. DOI to PubMed ID (PMID)
        idconv_url = f"{NCBI_API_BASE}esearch.fcgi?db=pubmed&term={doi}[DOI]&retmode=json"
        res = requests.get(idconv_url, headers=headers).json()
        pmid = res['esearchresult']['idlist'][0]
        time.sleep(0.4) # Rate limit

        # 2. PMID to PubMed Central (PMC) ID
        link_url = f"{NCBI_API_BASE}elink.fcgi?dbfrom=pubmed&db=pmc&id={pmid}&retmode=json"
        res = requests.get(link_url, headers=headers).json()
        pmc_id = res['linksets'][0]['linksetdbs'][0]['links'][0]
        pmc_url = f"https://www.ncbi.nlm.nih.gov/pmc/articles/PMC{pmc_id}/"
        time.sleep(0.4)

        # 3. Scrape Full Text page for Accession Numbers
        page_content = requests.get(pmc_url, headers=headers).text
        geo_accession = GEO_ACCESSION_PATTERN.search(page_content)
        if not geo_accession:
            return {"doi": doi, "status": "failed", "reason": "No GEO accession found in PMC full text"}
        gse = geo_accession.group(1)

        # 4. GEO Accession (GSE) to FTP link
        # This is a bit of a heuristic. The series matrix file is often what we want.
        # A more robust solution might use GEOparse, but this is fast and often effective.
        ftp_link = f"https://ftp.ncbi.nlm.nih.gov/geo/series/{gse[:-3]}nnn/{gse}/matrix/{gse}_series_matrix.txt.gz"
        
        # Check if the FTP link is valid by sending a HEAD request
        ftp_res = requests.head(ftp_link, headers=headers, timeout=10)
        if ftp_res.status_code == 200:
            return {"doi": doi, "status": "success", "accession": gse, "download_url": ftp_link}
        else:
            return {"doi": doi, "status": "failed", "reason": f"Found {gse} but FTP link was invalid (Status: {ftp_res.status_code})"}

    except (IndexError, KeyError):
        return {"doi": doi, "status": "failed", "reason": "Could not resolve DOI to PMID/PMC or no PMC link available"}
    except Exception as e:
        return {"doi": doi, "status": "failed", "reason": str(e)}

def main():
    parser = argparse.ArgumentParser(description="Resolve DOIs to direct data download links from NCBI GEO.")
    parser.add_argument("doi_file", type=str, help="A text file with one DOI per line.")
    parser.add_argument("--output-manifest", default="00_manifests/doi_resolved_manifest.csv", help="Path to save the new manifest.")
    parser.add_argument("--workers", type=int, default=10, help="Number of concurrent resolving threads (max 10 for NCBI API).")
    args = parser.parse_args()

    doi_path = Path(args.doi_file)
    if not doi_path.exists():
        console.print(f"[bold red]Error:[/bold red] DOI file not found at `{doi_path}`.")
        return

    with open(doi_path, 'r') as f:
        dois_to_resolve = [line.strip() for line in f if line.strip()]

    console.print(Panel(f"Attempting to resolve {len(dois_to_resolve)} DOIs from `{doi_path.name}`", title="[bold cyan]Concurrent DOI to Data Resolver[/bold cyan]"))

    resolved_data = []
    with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}"), BarColumn(), MofNCompleteColumn(), TimeElapsedColumn()) as progress:
        task = progress.add_task(f"[cyan]Resolving DOIs with {args.workers} workers...", total=len(dois_to_resolve))
        
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            futures = {executor.submit(resolve_single_doi, doi): doi for doi in dois_to_resolve}
            for future in as_completed(futures):
                result = future.result()
                if result:
                    resolved_data.append(result)
                    if result['status'] == 'failed':
                        progress.console.print(f"[yellow]Failed DOI:[/yellow] {result['doi']} - Reason: {result['reason']}")
                progress.update(task, advance=1)

    # --- Final Report and Output ---
    results_df = pd.DataFrame(resolved_data)
    success_df = results_df[results_df['status'] == 'success']

    if success_df.empty:
        console.print("\n[bold yellow]Resolution complete, but no direct data download links could be found for any of the DOIs.[/bold yellow]")
    else:
        # Create the manifest file
        manifest_data = [{
            "url": row['download_url'],
            "output_path": f"01_data_raw/from_doi/{row['accession']}_series_matrix.txt.gz",
            "data_source": f"GEO_{row['accession']}",
            "file_type": "expression_matrix"
        } for _, row in success_df.iterrows()]
        
        manifest_df = pd.DataFrame(manifest_data)
        output_path = Path(args.output_manifest)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        manifest_df.to_csv(output_path, index=False)
        
        console.print(Panel(
            f"Successfully resolved [bold green]{len(success_df)}[/bold green] of {len(dois_to_resolve)} DOIs to direct data download links.\n"
            f"[bold red]{len(results_df) - len(success_df)}[/bold red] DOIs failed to resolve automatically.\n\n"
            f"A new manifest has been created with the successful links.\n"
            f"  -> [bold]Saved to:[/bold] `{output_path}`\n\n"
            f"[bold]Next Step:[/bold] Use this new manifest with the `fetch` step to download the data:\n"
            f"[white on black] python scripts/step01_fetch_data.py --manifest {output_path} [/]",
            title="[bold green]DOI Resolution Complete![/bold green]"
        ))

if __name__ == "__main__":
    main()