# scripts/utility_build_manifest_from_sources.py
# V2.0: An intelligent, concurrent utility to build a manifest from a legacy
# project structure. It now automatically discovers and scans all subfolders
# within a single specified parent directory.

import argparse
from pathlib import Path
import pandas as pd
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, BarColumn, TextColumn, TimeElapsedColumn, MofNCompleteColumn, SpinnerColumn
import compass_brca.pipeline_config as cfg

console = Console()

# --- Intelligent Categorization Heuristics ---
def categorize_file(file_path: Path, source_folder_name: str) -> tuple[str, str]:
    """Makes an educated guess about a file's data_source and file_type."""
    folder_str = source_folder_name.lower()
    name = file_path.name.lower()

    if "brca" in str(file_path).lower() or "tcga" in str(file_path).lower(): data_source = "TCGA-BRCA"
    elif "nano" in str(file_path).lower(): data_source = "Nanomedicine"
    elif "geo" in str(file_path).lower() or "gse" in str(file_path).lower(): data_source = "GEO"
    elif "source" in folder_str: data_source = "Legacy_Sources"
    elif "url" in folder_str: data_source = "URL_Lists"
    else: data_source = "Unknown_Legacy"

    if "clinic" in name: file_type = "clinical"
    elif "expression" in name or ".cel" in name: file_type = "expression"
    elif "mutation" in name or ".vcf" in name or ".maf" in name: file_type = "mutation"
    elif "manifest" in name or "url" in folder_str: file_type = "metadata_manifest"
    else: file_type = "experimental_raw"
        
    return data_source, file_type

def scan_source_folder(source_dir: Path) -> list[dict]:
    """Scans a single source folder and generates manifest entries for all files."""
    manifest_entries = []
    if not source_dir.exists() or not source_dir.is_dir():
        return manifest_entries

    all_files = [f for f in source_dir.rglob("*") if f.is_file()]
    
    for file in all_files:
        data_source, file_type = categorize_file(file, source_dir.name)
        new_output_path = cfg.RAW_DATA_DIR / f"{source_dir.name}_{file.name}"
        
        entry = {
            "url": f"file:///{file.resolve().as_posix()}",
            "output_path": new_output_path.as_posix().replace('\\', '/'),
            "data_source": data_source,
            "file_type": file_type
        }
        manifest_entries.append(entry)
    return manifest_entries

def main():
    parser = argparse.ArgumentParser(description="Build a manifest by auto-scanning subfolders in a parent legacy directory.")
    # --- The key change is here: one argument instead of many ---
    parser.add_argument("parent_directory", type=str, help="The single, top-level legacy data folder containing all the messy subfolders.")
    parser.add_argument("--output-manifest", default="00_manifests/generated_legacy_manifest.csv", help="Path to save the new manifest.")
    parser.add_argument("--workers", type=int, default=os.cpu_count(), help="Number of concurrent scanning threads.")
    args = parser.parse_args()

    parent_dir = Path(args.parent_directory)
    if not parent_dir.exists() or not parent_dir.is_dir():
        console.print(f"[bold red]Error:[/bold red] The specified parent directory was not found: `{parent_dir}`")
        return

    # --- Automatically discover all immediate subfolders to scan ---
    source_dirs_to_scan = [d for d in parent_dir.iterdir() if d.is_dir()]

    if not source_dirs_to_scan:
        console.print(f"[yellow]Warning:[/yellow] No subdirectories found to scan inside `{parent_dir}`.")
        return

    output_manifest_path = Path(args.output_manifest)
    console.print(Panel(
        f"Parent Directory: `{parent_dir.resolve()}`\nFound {len(source_dirs_to_scan)} subfolders to scan.\nOutput Manifest: `{output_manifest_path.resolve()}`",
        title="[bold cyan]Smart-Scan Manifest Builder[/bold cyan]"
    ))

    all_manifest_entries = []
    
    with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}"), BarColumn(), MofNCompleteColumn(), TimeElapsedColumn()) as progress:
        task = progress.add_task(f"[cyan]Scanning {len(source_dirs_to_scan)} subfolders...", total=len(source_dirs_to_scan))
        
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            futures = {executor.submit(scan_source_folder, d): d for d in source_dirs_to_scan}
            
            for future in as_completed(futures):
                entries = future.result()
                all_manifest_entries.extend(entries)
                source_dir_name = futures[future].name
                progress.console.print(f"  -> Finished scanning `[green]{source_dir_name}[/green]`, found {len(entries)} files.")
                progress.update(task, advance=1)

    if not all_manifest_entries:
        console.print("[yellow]Scan complete, but no files were found in any subdirectories.[/yellow]")
        return

    manifest_df = pd.DataFrame(all_manifest_entries)
    output_manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_df.to_csv(output_manifest_path, index=False)
    
    console.print(Panel(
        f"A new manifest has been generated with [bold]{len(manifest_df)}[/bold] entries.\n"
        f"  -> [bold]Saved to:[/bold] `{output_manifest_path}`\n\n"
        f"[bold]Next Steps:[/bold] Review the new manifest, then run the `utility_consolidate_data.py` script on the same parent directory (`{parent_dir.name}`).",
        title="[bold green]Legacy Manifest Created Successfully![/bold green]"
    ))

if __name__ == "__main__":
    main()