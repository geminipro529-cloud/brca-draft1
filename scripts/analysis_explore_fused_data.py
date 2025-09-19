# scripts/analysis_explore_fused_data.py
# DEFINITIVE V3 - With Simple Pathing
import sys
import pandas as pd
from pathlib import Path

# --- SIMPLE PATHING ---
try:
    project_root = Path(__file__).resolve().parents[1]
    sys.path.append(str(project_root / "src"))
    from compass_brca.pipeline_config import OUTPUT_FUSED_TABLE
except ImportError as e:
    sys.exit(f"Fatal Error: Could not import config. Details: {e}")
# --- END SIMPLE PATHING ---

from rich.console import Console
from rich.panel import Panel
console = Console()

def main():
    console.rule("[bold magenta]Analysis: Final Inspection of Fused Feature Table[/]")
    if not OUTPUT_FUSED_TABLE.exists():
        console.print(f"[bold red]Error: Fused feature table not found at {OUTPUT_FUSED_TABLE}[/]")
        sys.exit(1)

    df = pd.read_parquet(OUTPUT_FUSED_TABLE)
    num_experiments = len(df)
    num_features = len(df.columns)
    drug_cols = ['drug_id', 'concentration_molar']
    genomic_cols = [col for col in df.columns if col not in drug_cols and 'outcome_' not in col and 'cell_line_name' not in col]
    outcome_cols = [col for col in df.columns if 'outcome_' in col]
    report_panel = Panel(
        f"Total Complete Experiments (Rows): [bold cyan]{num_experiments:,}[/]\n"
        f"Total Features (Columns): [bold cyan]{num_features:,}[/]\n\n"
        f"[bold]Drug Features ({len(drug_cols)}):[/]\n[dim]{', '.join(drug_cols)}[/]\n\n"
        f"[bold]Genomic Features ({len(genomic_cols)}):[/]\n[dim]{', '.join(genomic_cols[:5])}{'...' if len(genomic_cols) > 5 else ''}[/]\n\n"
        f"[bold]Outcome Variables ({len(outcome_cols)}):[/]\n[dim]{', '.join(outcome_cols)}[/]",
        title="[bold]Master Feature Table: State of the Union[/]",
        expand=False
    )
    console.print(report_panel)
    if not df.empty:
        console.print("\n[bold]Data Preview (First 5 Rows of Key Columns):[/]")
        preview_cols = drug_cols + genomic_cols[:3] + outcome_cols
        console.print(df[[col for col in preview_cols if col in df.columns]].head())
        console.print("\n[bold]Key Numerical Statistics:[/]")
        console.print(df.describe())

if __name__ == "__main__":
    main()