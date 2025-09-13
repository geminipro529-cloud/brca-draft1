# scripts/utility_recover_missing_links.py
# V-Final: A concurrent, forensic utility with a rich progress UI to deep-scan
# a project for log files, extract failed download URLs, and generate a
# new manifest to recover only the genuinely missing data.

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
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', filename='recovery.log', filemode='w')

# --- Forensic Patterns ---
URL_PATTERN = re.compile(r'https?://[^\s,"\'\]\[\(\)]+|ftp://[^\s,"\'\]\[\(\)]+')
FAILURE_KEYWORDS = ['fail', 'error', 'failed', 'skip', 'not found', '404', '503', 'timeout', 'errn']

def parse_log_file(log_file: Path) -> set[str]:
    """Reads a single log file and extracts URLs from lines indicating failure."""
    failed_urls = set()
    try:
        with open(log_file, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                line_lower = line.lower()
                if any(keyword in line_lower for keyword in FAILURE_KEYWORDS):
                    match = URL_PATTERN.search(line)
                    if match:
                        failed_urls.add(match.group(0))
    except Exception as e:
        logging.warning(f"Could not read or parse log file {log_file}: {e}")
    return failed_urls

def main():
    parser = argparse.ArgumentParser(description="Recover missing file URLs from scattered log files concurrently.")
    parser.add_argument("scan_directory", type=str, help="The directory to scan for log files (e.g., '.').")
    parser.add_argument("--output-manifest", default="00_manifests/recovery_manifest.csv", help="Path to save the new manifest.")
    parser.add_argument("--workers", type=int, default=os.cpu_count(), help="Number of concurrent worker threads.")
    args = parser.parse_args()

    scan_dir = Path(args.scan_directory)
    output_manifest_path = Path(args.output_manifest)

    console.print(Panel(
        f"Scan Directory: `{scan_dir.resolve()}`\nOutput Manifest: `{output_manifest_path.resolve()}`\nWorkers: {args.workers}",
        title="[bold cyan]Concurrent Missing Link Recovery[/bold cyan]"
    ))

    if not cfg.RAW_DATA_DIR.exists():
        console.print(f"[bold red]Error:[/bold red] Canonical raw data directory `{cfg.RAW_DATA_DIR}` not found. Run consolidator first.")
        return

    with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}"), BarColumn(), MofNCompleteColumn(), TimeElapsedColumn()) as progress:
        # --- Stage 1: Inventory existing raw files ---
        inventory_task = progress.add_task("[green]Inventoring existing files...", total=None)
        existing_filenames = {f.name for f in cfg.RAW_DATA_DIR.rglob("*")}
        progress.update(inventory_task, total=1, completed=1, description=f"[green]Inventoried {len(existing_filenames)} existing files.[/green]")

        # --- Stage 2: Deep scan for all log files ---
        discovery_task = progress.add_task("[green]Discovering log files...", total=None)
        log_files = [f for f in scan_dir.rglob("*.log") if f.is_file()]
        progress.update(discovery_task, total=1, completed=1, description=f"[green]Discovered {len(log_files)} log files.[/green]")
        
        if not log_files:
            console.print("[yellow]No `.log` files found. Nothing to recover.[/yellow]")
            return

        # --- Stage 3: Parse logs concurrently ---
        parsing_task = progress.add_task(f"[cyan]Analyzing logs with {args.workers} workers...", total=len(log_files))
        all_failed_urls = set()
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            futures = {executor.submit(parse_log_file, f): f for f in log_files}
            for future in as_completed(futures):
                urls = future.result()
                all_failed_urls.update(urls)
                progress.update(parsing_task, advance=1)

        if not all_failed_urls:
            console.print("[green]No failed download URLs were found in the logs.[/green]")
            return
        
        # --- Stage 4: Cross-reference and build the recovery manifest ---
        xref_task = progress.add_task("[green]Finding genuinely missing files...", total=len(all_failed_urls))
        genuinely_missing_urls = []
        for url in all_failed_urls:
            filename = Path(url.split('?')[0]).name
            if filename not in existing_filenames:
                genuinely_missing_urls.append(url)
            progress.update(xref_task, advance=1)
        
        if not genuinely_missing_urls:
            console.print("\n[bold green]Success! All previously failed downloads appear to be present in the raw data folder now.[/bold green]")
            return

    # --- Final Report and Output ---
    console.print(f"\n[bold yellow]Found {len(genuinely_missing_urls)} genuinely missing files![/bold yellow]")
    
    recovery_data = [{
        "url": url,
        "output_path": f"01_data_raw/recovered/{Path(url.split('?')[0]).name}",
        "data_source": "Recovered_From_Log",
        "file_type": "Recovered"
    } for url in genuinely_missing_urls]

    recovery_df = pd.DataFrame(recovery_data)
    output_manifest_path.parent.mkdir(parents=True, exist_ok=True)
    recovery_df.to_csv(output_manifest_path, index=False)
    
    console.print(Panel(
        f"A new manifest has been created with [bold]{len(genuinely_missing_urls)}[/bold] URLs for files that need to be downloaded.\n"
        f"  -> [bold]Saved to:[/bold] `{output_manifest_path}`\n\n"
        f"[bold]Next Step:[/bold] Run the `fetch` step using this new manifest:\n"
        f"[white on black] python scripts/step01_fetch_data.py --manifest {output_manifest_path} [/]",
        title="[bold green]Recovery Manifest Created Successfully![/bold green]"
    ))

if __name__ == "__main__":
    # This check is necessary to prevent circular import issues when running with `python -m`
    try:
        import compass_brca.pipeline_config as cfg
    except ImportError:
        # A fallback for finding the config if run from a different context, not ideal but robust.
        import sys
        sys.path.append(str(Path(__file__).parent.parent))
        import compass_brca.pipeline_config as cfg
    main()