# scripts/utility_extract_archives.py
# V3 - The "Warehouse-Wide" Master Extractor
# Scans the entire 01_data_raw directory for all common archive types.

import gzip
import shutil
import tarfile
import zipfile
import py7zr
import rarfile
import sys
from pathlib import Path

# --- ROBUST PATHING ---
try:
    project_root = Path(__file__).resolve().parents[1]
    sys.path.append(str(project_root / "src"))
    from compass_brca.pipeline_config import RAW_DATA_DIR
except ImportError as e:
    sys.exit(f"Fatal Error: Could not import config. Details: {e}")
# --- END ROBUST PATHING ---

from rich.console import Console
from rich.progress import track

console = Console()

# --- CONFIGURATION (UPDATED) ---
# The script will now search the entire raw data warehouse.
TARGET_DIRECTORY = RAW_DATA_DIR
SUPPORTED_EXTENSIONS = {".zip", ".rar", ".7z", ".tar", ".gz", ".tgz", ".tar.gz", ".tar.bz2"}
# --- END CONFIGURATION ---

def get_output_dir(file_path: Path) -> Path:
    """Creates a clean output directory named after the archive."""
    name = file_path.name
    for ext in ['.gz', '.bz2', '.tar', '.zip', '.rar', '.7z', '.tgz']:
        if name.lower().endswith(ext):
            name = name[:-len(ext)]
            
    output_dir = file_path.parent / name
    output_dir.mkdir(exist_ok=True)
    return output_dir

def extract_archive(file_path: Path):
    """Master dispatcher function. Detects archive type and extracts it."""
    output_dir = get_output_dir(file_path)
    console.print(f"  [dim]Extracting {file_path.name} -> to folder '{output_dir.name}'...[/]")

    try:
        # Use lowercased suffixes for robust matching
        suffix = "".join(file_path.suffixes).lower()

        if suffix.endswith('.zip'):
            with zipfile.ZipFile(file_path, 'r') as zf:
                zf.extractall(output_dir)
        
        elif suffix.endswith('.rar'):
            with rarfile.RarFile(file_path, 'r') as rf:
                rf.extractall(output_dir)

        elif suffix.endswith('.7z'):
            with py7zr.SevenZipFile(file_path, mode='r') as z:
                z.extractall(path=output_dir)
        
        elif '.tar' in suffix:
            with tarfile.open(file_path, 'r:*') as tf:
                tf.extractall(path=output_dir)
        
        elif suffix.endswith('.gz'):
            # Handles single gzipped files that are NOT tarballs
            output_path = output_dir / file_path.stem
            with gzip.open(file_path, 'rb') as f_in:
                with open(output_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
        
        else:
            console.print(f"[yellow]Warning: Unhandled file type for {file_path.name}. Skipping.[/]")
            return

        # Clean up the original archive after successful extraction
        file_path.unlink()
        console.print(f"[green]✓ Successfully extracted and cleaned up {file_path.name}[/]")

    except rarfile.UNRARDLL_NOT_FOUND:
        console.print(f"[bold red]FATAL for {file_path.name}: 'unrar' utility not found.[/]")
        console.print("[yellow]Please install the unrar command-line tool to handle .rar files.[/]")
    except Exception as e:
        console.print(f"[bold red]Error processing {file_path.name}: {e}[/]")

def main():
    """Finds and processes all supported archive files in the target directory."""
    console.rule(f"[bold magenta]Master Archive Extraction Utility (V3)[/]")
    
    if not TARGET_DIRECTORY.exists():
        console.print(f"[bold red]Error: Target directory not found at {TARGET_DIRECTORY}[/]"); return

    archive_files = [p for p in TARGET_DIRECTORY.rglob("*") if p.is_file() and "".join(p.suffixes).lower() in SUPPORTED_EXTENSIONS]

    if not archive_files:
        console.print("[green]No supported archive files found to extract.[/]"); return
        
    console.print(f"Found [cyan]{len(archive_files)}[/] archive files to process in the entire raw data directory.")

    for file_path in track(archive_files, description="Processing archives..."):
        console.print(f"\nProcessing: [cyan]{file_path.name}[/]")
        extract_archive(file_path)

    console.rule("[bold green]Extraction Complete[/]")

if __name__ == "__main__":
    main()