
from pathlib import Path
from rich.console import Console
from rich.table import Table

console = Console()

# --- Configuration: File types to search for ---
RAW_DATA_EXTENSIONS = ['.csv', '.tsv', '.txt', '.cel', '.fastq', '.gz', '.tar', '.zip', '.json']
INTERIM_DATA_EXTENSIONS = ['.parquet']
LOG_FILE_EXTENSIONS = ['.log']

def main():
    """Generates a deep scan report of all relevant files in the project."""
    project_root = Path.cwd()
    console.print(f"# Deep Scan Diagnostics Report: `{project_root.name}`")
    
    all_files = list(project_root.rglob("*"))
    
    # Categorize files
    raw_files = [f for f in all_files if f.suffix.lower() in RAW_DATA_EXTENSIONS and "old_scripts" not in str(f)]
    interim_files = [f for f in all_files if f.suffix.lower() in INTERIM_DATA_EXTENSIONS and "old_scripts" not in str(f)]
    log_files = [f for f in all_files if f.suffix.lower() in LOG_FILE_EXTENSIONS and "old_scripts" not in str(f)]
    
    # --- Print Summary Table ---
    table = Table(title="Project File Inventory (Deep Scan)")
    table.add_column("File Category", style="cyan")
    table.add_column("File Count", justify="right", style="magenta")
    table.add_column("Example Locations (Top 3)", style="yellow")
    
    def get_example_dirs(file_list):
        if not file_list: return "(None found)"
        dirs = sorted(list(set(f.parent for f in file_list)))
        return "\n".join([f"`{d.relative_to(project_root)}/`" for d in dirs[:3]])

    table.add_row("Raw Data Files", str(len(raw_files)), get_example_dirs(raw_files))
    table.add_row("Interim/Processed Files", str(len(interim_files)), get_example_dirs(interim_files))
    table.add_row("Download/Log Files", str(len(log_files)), get_example_dirs(log_files))
    
    console.print(table)
    
    console.print("\n[bold]Recommendation:[/bold] This report shows where your existing data is located.")
    console.print("Paste this entire output so I can provide custom commands to consolidate these files into the new pipeline structure.")

if __name__ == "__main__":
    main()