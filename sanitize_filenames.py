# sanitize_filenames.py
# V4.0 - A robust, top-down utility that uses a purely absolute path workflow
# to prevent relative/absolute path conflicts during sanitation.

import os
import hashlib
import csv
import argparse
from pathlib import Path
from rich.console import Console
from rich.panel import Panel

console = Console()

# --- Configuration ---
MAX_COMPONENT_LENGTH = 150
ILLEGAL_CHARS = [',', ':', ';', '"', '*', '|', '<', '>']

def get_safe_name(path_obj: Path, is_dir: bool) -> str:
    original_name = path_obj.name
    prefix = "sanitized_dir_" if is_dir else "sanitized_file_"
    name_hash = hashlib.md5(original_name.encode('utf-8', 'ignore')).hexdigest()[:12]
    suffix = path_obj.suffix if not is_dir else ''
    return f"{prefix}{name_hash}{suffix}"

def sanitize_single_path_component(path_obj: Path, log_writer, dry_run: bool) -> Path:
    is_problematic = (len(path_obj.name) > MAX_COMPONENT_LENGTH or
                      any(char in path_obj.name for char in ILLEGAL_CHARS))
                      
    if not is_problematic:
        return path_obj

    new_name = get_safe_name(path_obj, path_obj.is_dir())
    new_path = path_obj.parent / new_name
    
    counter = 0
    while new_path.exists():
        counter += 1
        new_name = get_safe_name(path_obj, path_obj.is_dir()).replace(path_obj.suffix, f"_{counter}{path_obj.suffix}")
        new_path = path_obj.parent / new_name
        
    console.print(f"\n[yellow]Problem Found:[/yellow] `{path_obj}`")
    console.print(f"  [cyan]Action Plan:[/cyan] Rename to `{new_path}`")
    log_writer.writerow([str(path_obj), str(new_path)])

    if not dry_run:
        try:
            prefixed_old_path = f"\\\\?\\{path_obj.resolve()}"
            os.rename(prefixed_old_path, new_path)
            return new_path
        except Exception as e:
            console.print(f"[bold red]  ERROR:[/bold red] Failed to rename. Reason: {e}")
            log_writer.writerow([str(path_obj), f"ERROR: FAILED TO RENAME - {e}"])
            raise e
    
    return path_obj

def main():
    parser = argparse.ArgumentParser(description="Find and fix corrupted/long filenames and directory names.")
    parser.add_argument("target_directory", type=str, help="The relative or absolute path to the directory to scan (e.g., 'data').")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be renamed without actually renaming.")
    args = parser.parse_args()

    # --- CRITICAL CHANGE: Convert to an absolute path immediately ---
    target_dir = Path(args.target_directory).resolve()
    
    if not target_dir.exists():
        console.print(f"[bold red]Error:[/bold red] Target directory `{target_dir}` not found.")
        return

    log_file_path = Path(f"sanitization_log_v4_{target_dir.name}.csv")
    
    console.print(Panel(
        f"Target: `{target_dir}`\nMode:   {'[bold yellow]Dry Run[/]' if args.dry_run else '[bold red]Live Run[/]'}",
        title="[bold cyan]Filesystem Sanitizer V4 (Absolute Path Safe)[/bold cyan]"
    ))

    if not args.dry_run:
        console.print("[bold red]!!! WARNING !!! This will rename both FILES and DIRECTORIES on your disk.[/]")
        if input("Type 'YES' to proceed: ") != 'YES':
            console.print("[yellow]Aborted by user.[/]")
            return
            
    with open(log_file_path, 'w', newline='', encoding='utf-8') as f:
        log_writer = csv.writer(f)
        log_writer.writerow(["original_corrupted_path", "new_sanitized_path"])
        
        console.print("Starting top-down sanitation walk...")
        try:
            # os.walk works perfectly fine with absolute paths
            for root, dirs, files in os.walk(target_dir, topdown=True):
                current_root = Path(root)
                
                # Sanitize directories in the current level first
                for dir_name in dirs[:]:
                    original_dir_path = current_root / dir_name
                    new_dir_path = sanitize_single_path_component(original_dir_path, log_writer, args.dry_run)
                    if new_dir_path != original_dir_path:
                        dirs[dirs.index(dir_name)] = new_dir_path.name
                
                # Now sanitize files in the (now clean) current directory
                for file_name in files:
                    original_file_path = current_root / file_name
                    sanitize_single_path_component(original_file_path, log_writer, args.dry_run)
        except Exception as e:
            console.print("\n[bold red]FATAL ERROR during sanitation walk.[/]")
            console.print(f"The process was halted. Check the log file for details of what was changed before the error.")
            console.print(f"Error Message: {e}")
            return

    console.print(f"\n[bold green]Sanitization complete.[/bold green]")
    console.print(f"A full log of changes has been saved to `{log_file_path}`.")

if __name__ == "__main__":
    main()