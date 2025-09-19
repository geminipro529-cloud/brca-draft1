# scripts/analysis_data_completeness_report.py
# Analyzes the "fill rate" of all Parquet files to find data-rich sources.
import sys
import pandas as pd
from pathlib import Path

try:
    project_root = Path(__file__).resolve().parents[1]
    sys.path.append(str(project_root / "src"))
    from compass_brca.pipeline_config import INTERIM_DATA_DIR
except ImportError as e:
    sys.exit(f"Fatal Error: Could not import config. Details: {e}")

from rich.console import Console
from rich.table import Table

console = Console()

def analyze_file(file_path: Path) -> dict:
    """Calculates the overall fill rate for a Parquet file."""
    try:
        df = pd.read_parquet(file_path)
        if df.empty:
            return None
        
        non_null_count = df.notna().sum().sum()
        total_cell_count = df.shape[0] * df.shape[1]
        
        return {
            "filename": file_path.name,
            "rows": df.shape[0],
            "columns": df.shape[1],
            "fill_rate_percent": (non_null_count / total_cell_count) * 100 if total_cell_count > 0 else 0
        }
    except Exception:
        return None

def main():
    console.rule("[bold magenta]Analysis: Data Completeness Report (Fill Rate)[/]")

    if not INTERIM_DATA_DIR.exists():
        console.print(f"[bold red]Error: Interim data directory not found.[/]"); sys.exit(1)
        
    all_files = list(INTERIM_DATA_DIR.rglob("*.parquet"))
    if not all_files:
        console.print("[yellow]No Parquet files found to analyze.[/]"); return
        
    console.print(f"Analyzing {len(all_files):,} Parquet files...")
    
    results = [analyze_file(p) for p in all_files]
    results = [r for r in results if r is not None]

    if not results:
        console.print("[bold red]Could not analyze any files.[/]"); return

    report_df = pd.DataFrame(results)
    # Sort by the most data-rich files
    report_df.sort_values(by="fill_rate_percent", ascending=False, inplace=True)
    
    table = Table(title="Top 15 Most Complete Datasets")
    table.add_column("Filename", style="cyan")
    table.add_column("Rows", style="magenta")
    table.add_column("Columns", style="magenta")
    table.add_column("Fill Rate %", style="green")

    for _, row in report_df.head(15).iterrows():
        table.add_row(
            row['filename'],
            f"{row['rows']:,}",
            f"{row['columns']:,}",
            f"{row['fill_rate_percent']:.2f}%"
        )
        
    console.print(table)

if __name__ == "__main__":
    main()