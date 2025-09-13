# scripts/utility_harvest_all_urls.py
# A concurrent forensic tool with a rich UI to deep-scan a project for ANY
# text-based file and extract every single URL ever mentioned. This is used
# to recover a master list of all potential data sources.

import re
from pathlib import Path
import argparse
import logging
import os
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, BarColumn, TextColumn, TimeElapsedColumn, MofNCompleteColumn, SpinnerColumn

console = Console()
logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s', filename='url_harvester.log', filemode='w')

# --- Forensic Patterns ---
URL_PATTERN = re.compile(r'https?://[^\s,"\'\]\[\(\)<>]+|ftp://[^\s,"\'\]\[\(\)<>]+')
# Extensions for files that are likely to contain text and URLs
SCAN_EXTENSIONS = ['.txt', '.log', '.csv', '.tsv', '.html', '.json', '.xml', '.sh', '.py']

def harvest_urls_from_file(file_path: Path) -> set[str]:
    """Reads a single file and extracts all unique URLs found within it."""
    found_urls = set()
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
            # findall is efficient for extracting all non-overlapping matches
            matches = URL_PATTERN.findall(content)
            if matches:
                found_urls.update(matches)
    except Exception as e:
        logging.warning(f"Could not read or scan file {file_path}: {e}")
    return found_urls

def main():
    parser = argparse.ArgumentParser(description="Harvest all unique URLs from all text-based files in a directory.")
    parser.add_argument("scan_directory", type=str, help="The directory to scan for files (e.g., '.').")
    parser.add_argument("--output-file", default="harvested_urls.csv", help="Path to save the CSV of all found URLs.")
    parser.add_argument("--workers", type=int, default=os.cpu_count(), help="Number of concurrent worker threads.")
    args = parser.parse_args()

    scan_dir = Path(args.scan_directory)
    output_file_path = Path(args.output_file)

    console.print(Panel(
        f"Scan Directory: `{scan_dir.resolve()}`\nOutput File: `{output_file_path.resolve()}`\nWorkers: {args.workers}",
        title="[bold cyan]Concurrent URL Harvester[/bold cyan]"
    ))

    with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}"), BarColumn(), MofNCompleteColumn(), TimeElapsedColumn()) as progress:
        # --- Stage 1: Discover relevant files ---
        discovery_task = progress.add_task("[green]Discovering files...", total=None)
        files_to_scan = [f for f in scan_dir.rglob("*") if f.is_file() and f.suffix.lower() in SCAN_EXTENSIONS]
        progress.update(discovery_task, total=1, completed=1, description=f"[green]Discovered {len(files_to_scan)} potential source files.[/green]")
        
        if not files_to_scan:
            console.print("[yellow]No text-based files found to scan.[/yellow]")
            return

        # --- Stage 2: Harvest URLs concurrently ---
        harvesting_task = progress.add_task(f"[cyan]Harvesting URLs with {args.workers} workers...", total=len(files_to_scan))
        all_found_urls = set()
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            futures = {executor.submit(harvest_urls_from_file, f): f for f in files_to_scan}
            for future in as_completed(futures):
                urls = future.result()
                all_found_urls.update(urls)
                progress.update(harvesting_task, advance=1)

    if not all_found_urls:
        console.print("[yellow]Scan complete, but no URLs were found in any files.[/yellow]")
        return

    # --- Final Report and Output ---
    console.print(f"\n[bold green]Harvesting complete! Found {len(all_found_urls)} unique URLs.[/bold green]")
    
    # We will also try to guess the filename from the URL for better context
    def get_filename_from_url(url):
        try:
            return Path(url.split('?')[0]).name
        except Exception:
            return "unknown_filename"

    url_data = [{
        "url": url,
        "potential_filename": get_filename_from_url(url)
    } for url in sorted(list(all_found_urls))]

    report_df = pd.DataFrame(url_data)
    report_df.to_csv(output_file_path, index=False)
    
    console.print(Panel(
        f"A master list of all [bold]{len(all_found_urls)}[/bold] unique URLs found in your project has been created.\n"
        f"  -> [bold]Saved to:[/bold] `{output_file_path}`\n\n"
        f"[bold]Next Step:[/bold] Review this CSV file. You can use it as a source to manually create a new, more complete manifest for any datasets you might have missed.",
        title="[bold green]URL Harvest Successful![/bold green]"
    ))

if __name__ == "__main__":
    try:
        import pandas as pd
        import pipeline_config as cfg
    except ImportError:
        # A fallback for finding the config if run from a different context
        import sys
        sys.path.append(str(Path(__file__).parent.parent))
        import pandas as pd
        import pipeline_config as cfg
    main()