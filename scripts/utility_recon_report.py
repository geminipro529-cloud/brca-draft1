# scripts/utility_recon_report.py
# The definitive "Master Reconnaissance" script.
# Scans the entire raw data directory and produces a full report.

import sys
import os # <-- BUG FIX: Added the missing 'os' import
from pathlib import Path

# --- ROBUST PATHING ---
try:
    project_root = Path(__file__).resolve().parents[1]
    sys.path.append(str(project_root / "src"))
    from compass_brca.pipeline_config import RAW_DATA_DIR, REPORTS_DIR
except ImportError as e:
    sys.exit(f"Fatal Error: Could not import config. Details: {e}")
# --- END ROBUST PATHING ---

from rich.console import Console
from rich.tree import Tree

console = Console()

# --- CONFIGURATION (UPDATED) ---
# The directory we want to scan is now the entire raw data warehouse.
TARGET_DIRECTORY = RAW_DATA_DIR

# The file where the report will be saved.
REPORT_FILE = REPORTS_DIR / "full_raw_data_recon_report.txt"
# --- END CONFIGURATION ---

def build_tree(directory: Path, tree: Tree, max_depth: int = 4):
    """Recursively build a Rich Tree of the directory structure."""
    if max_depth <= 0:
        tree.add("[dim]... (directory too deep to display fully)[/dim]")
        return
        
    try:
        paths = sorted(list(directory.iterdir()), key=lambda p: (p.is_file(), p.name.lower()))
    except FileNotFoundError:
        # Handle cases where a directory might be deleted during the scan
        return
        
    for path in paths:
        if path.is_dir():
            branch = tree.add(f":file_folder: [bold cyan]{path.name}[/]")
            build_tree(path, branch, max_depth - 1)
        else:
            tree.add(f":page_facing_up: [green]{path.name}[/]")

def main():
    """Generates and saves a reconnaissance report."""
    console.rule(f"[bold magenta]Master Reconnaissance Report Utility[/]")
    
    # --- IMPROVEMENT: Ensure absolute path for clarity ---
    target_abs_path = TARGET_DIRECTORY.resolve()

    if not target_abs_path.exists():
        console.print(f"[bold red]Error: Target directory not found at {target_abs_path}[/]")
        return
        
    REPORTS_DIR.mkdir(exist_ok=True)

    console.print(f"Scanning directory: [cyan]{target_abs_path}[/]")
    
    tree = Tree(f":deciduous_tree: [bold]Directory Structure of '{TARGET_DIRECTORY.name}'[/]")
    build_tree(target_abs_path, tree)
    
    console.print("\n--- Directory Tree (Visual Summary) ---")
    console.print(tree)

    report_lines = []
    report_lines.append(f"--- Reconnaissance Report for: {target_abs_path} ---\n")
    
    for root, dirs, files in sorted(os.walk(target_abs_path)):
        root_path = Path(root)
        # Use relative_to to get a clean, indented structure
        relative_path = root_path.relative_to(target_abs_path)
        level = len(relative_path.parts)
        indent = "    " * level
        report_lines.append(f"{indent}📁 {root_path.name}/\n")
        
        sub_indent = "    " * (level + 1)
        for f in sorted(files):
            report_lines.append(f"{sub_indent}📄 {f}\n")

    try:
        with open(REPORT_FILE, "w", encoding="utf-8") as f:
            f.writelines(report_lines)
        console.print("\n--- Status ---")
        console.print(f"[bold green]✓ Detailed reconnaissance report saved successfully.[/]")
        console.print(f"  [dim]Location: {REPORT_FILE}[/]")
    except Exception as e:
        console.print(f"\n--- Status ---")
        console.print(f"[bold red]✗ Error saving report: {e}[/]")

if __name__ == "__main__":
    main()