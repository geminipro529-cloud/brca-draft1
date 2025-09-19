# scripts/analysis_investigate_golden_goose.py
# V3 - "Dragnet" Edition
# A forensic script to analyze our prime candidate file for a wide range of keywords.
import sys
import pandas as pd
from pathlib import Path
from collections import defaultdict

try:
    project_root = Path(__file__).resolve().parents[1]
    sys.path.append(str(project_root / "src"))
    from compass_brca.pipeline_config import INTERIM_DATA_DIR
except ImportError as e:
    sys.exit(f"Fatal Error: Could not import config. Details: {e}")

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

console = Console()

# --- TARGET FILE & DRAGNET KEYWORDS ---
TARGET_FILENAME = "OmicsCNGeneProfileWGS.parquet"

# A comprehensive list of keywords to search for. Case-insensitive.
DRAGNET_KEYWORDS = [
    # Core Terms
    "nano", "particle", "np",
    # Material Types
    "liposome", "polymer", "gold", "lipid", "plga", "dendrimer",
    # Physical Properties
    "size", "zeta", "potential", "charge", "diameter",
    # Drug & Payload
    "drug", "payload", "load", "encapsulation", "release",
    # Surface & Chemistry
    "surface", "chemistry", "peg", "coating",
]
# --- END TARGET ---

def find_file(start_dir: Path, filename: str) -> Path:
    results = list(start_dir.rglob(filename))
    return results[0] if results else None

def main():
    console.rule(f"[bold magenta]Forensic Investigation: Dragnet on {TARGET_FILENAME}[/]")

    target_file_path = find_file(INTERIM_DATA_DIR, TARGET_FILENAME)
    if not target_file_path:
        console.print(f"[bold red]Error: Could not find '{TARGET_FILENAME}'[/]"); sys.exit(1)
        
    console.print(f"Found target file at: [cyan]{target_file_path}[/]")
    
    try:
        df = pd.read_parquet(target_file_path)
    except Exception as e:
        console.print(f"[bold red]Error: Failed to load Parquet file.[/] Details: {e}"); sys.exit(1)

    num_rows, num_cols = df.shape
    
    # --- Execute the Dragnet Search ---
    found_columns = defaultdict(list)
    all_found_col_names = set()

    for col in df.columns:
        col_lower = str(col).lower()
        for keyword in DRAGNET_KEYWORDS:
            if keyword in col_lower:
                found_columns[keyword].append(col)
                all_found_col_names.add(col)
    
    # --- Generate the Intelligence Report ---
    report_text = (
        f"Shape: [bold cyan]{num_rows:,}[/] rows × [bold cyan]{num_cols:,}[/] columns\n\n"
        f"Found [bold yellow]{len(all_found_col_names)}[/] total columns matching [bold yellow]{len(found_columns)}[/] keywords."
    )
    console.print(Panel(report_text, title="[bold]Dragnet Summary[/]", expand=False))

    if not all_found_col_names:
        console.print("\n[bold red]RESULT: No columns found matching any Dragnet keywords.[/]")
        return
        
    # --- Detailed Keyword Hits Table ---
    keyword_table = Table(title="Keyword Hits")
    keyword_table.add_column("Keyword", style="yellow")
    keyword_table.add_column("Matching Column Count", style="magenta")
    keyword_table.add_column("Sample Column Names", style="cyan")

    for keyword, cols in sorted(found_columns.items()):
        sample_cols = ", ".join(cols[:3]) + ('...' if len(cols) > 3 else '')
        keyword_table.add_row(keyword, str(len(cols)), sample_cols)
    
    console.print(keyword_table)
    
    # --- Data Completeness Report ---
    console.print("\n[bold]Data Completeness for All Found Columns:[/]")
    
    found_cols_sorted = sorted(list(all_found_col_names))
    non_null_counts = df[found_cols_sorted].notna().sum()
    fill_rate = (non_null_counts / num_rows) * 100 if num_rows > 0 else pd.Series([0]*len(found_cols_sorted))

    completeness_table = Table(title="Fill Rate Analysis")
    completeness_table.add_column("Column Name", style="cyan")
    completeness_table.add_column("Fill Rate %", style="green")

    for col_name, rate in fill_rate.items():
        color = "green" if rate > 5 else ("yellow" if rate > 0 else "red")
        completeness_table.add_row(f"[{color}]{col_name}[/{color}]", f"[{color}]{rate:.2f}%[/{color}]")
    
    console.print(completeness_table)
    
    # --- Sample Data Preview ---
    console.print("\n[bold]Sample Data Preview (First 5 rows):[/]")
    console.print(df[found_cols_sorted].head())


if __name__ == "__main__":
    main()