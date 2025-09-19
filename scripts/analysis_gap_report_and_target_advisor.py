# scripts/analysis_gap_report_and_target_advisor.py
# The definitive diagnostic tool and intelligence agent for our project.
import sys
import pandas as pd
from pathlib import Path
import urllib.parse

try:
    project_root = Path(__file__).resolve().parents[1]
    sys.path.append(str(project_root / "src"))
    from compass_brca.pipeline_config import OUTPUT_FUSED_TABLE
except ImportError as e:
    sys.exit(f"Fatal Error: Could not import config. Details: {e}")

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

console = Console()

# --- THE "IDEAL" DATASET BLUEPRINT ---
# This is our "Bill of Materials". We define what the perfect dataset must contain.
# We can add or remove terms here to refine our search.
REQUIRED_NANOPARTICLE_FEATURES = [
    'size_nm', 'zeta_potential_mv', 'core_material', 'surface_chemistry', 'drug_payload'
]
REQUIRED_GENOMIC_FEATURES = [
    'BRCA1_mutation', 'TP53_mutation', 'PIK3CA_mutation', 'HER2_status' # ERBB2 is the gene for HER2
]
# --- END BLUEPRINT ---

def main():
    console.rule("[bold magenta]Analysis: Gap Report & Target Advisor[/]")

    if not OUTPUT_FUSED_TABLE.exists():
        console.print(f"[bold red]Error: Fused feature table not found at {OUTPUT_FUSED_TABLE}[/]"); sys.exit(1)
        
    df = pd.read_parquet(OUTPUT_FUSED_TABLE)
    
    # --- STEP 1: INVENTORY (WHAT DO WE HAVE?) ---
    existing_columns = [col.lower() for col in df.columns]

    # --- STEP 2: GAP ANALYSIS (WHAT ARE WE MISSING?) ---
    missing_nano_features = [f for f in REQUIRED_NANOPARTICLE_FEATURES if f.lower() not in existing_columns]
    missing_genomic_features = [f for f in REQUIRED_GENOMIC_FEATURES if f.lower() not in existing_columns]

    # --- STEP 3: GENERATE THE GAP REPORT ---
    report_panel = Panel(
        f"This report analyzes your current master dataset (`master_fused_features.parquet`) "
        f"and provides a 'shopping list' of the critical data features you are missing to test your thesis.",
        title="[bold]Mission Briefing[/]"
    )
    console.print(report_panel)

    gap_table = Table(title="Data Gap Analysis")
    gap_table.add_column("Feature Type", style="yellow")
    gap_table.add_column("Missing Critical Features", style="cyan")
    gap_table.add_column("Status", style="magenta")

    if not missing_nano_features:
        gap_table.add_row("Nanoparticle Properties", "None", "[bold green]COMPLETE[/]")
    else:
        gap_table.add_row("Nanoparticle Properties", "\n".join(missing_nano_features), "[bold red]INCOMPLETE[/]")
        
    if not missing_genomic_features:
        gap_table.add_row("Genomic Features", "None", "[bold green]COMPLETE[/]")
    else:
        gap_table.add_row("Genomic Features", "\n".join(missing_genomic_features), "[bold red]INCOMPLETE[/]")
        
    console.print(gap_table)

    # --- STEP 4: GENERATE ACTIONABLE INTELLIGENCE (THE TARGET ADVISOR) ---
    if not missing_nano_features and not missing_genomic_features:
        console.print("\n[bold green]Congratulations! Your dataset appears to be complete. You are ready for advanced modeling.[/]")
        return
        
    console.print("\n[bold]Target Advisor: Recommended Actions[/]")
    advisor_table = Table(title="Suggested Next Steps to Fill Data Gaps")
    advisor_table.add_column("Missing Feature", style="red")
    advisor_table.add_column("Suggested Action / Search Query", style="white")

    for feature in missing_nano_features:
        query = urllib.parse.quote_plus(f'"nanoparticle" "breast cancer" "{feature}" "raw data" supplementary')
        url = f"https://scholar.google.com/scholar?q={query}"
        advisor_table.add_row(feature, f"Search for datasets containing this physical property.\n[link={url}]Click here to launch Google Scholar search[/link]")
    
    for feature in missing_genomic_features:
        query = urllib.parse.quote_plus(f'"TCGA" "BRCA" "cbioportal" "{feature.split("_")[0]}"')
        url = f"https://www.google.com/search?q={query}"
        advisor_table.add_row(feature, f"Search for this gene in foundational genomic datasets like TCGA via cBioPortal.\n[link={url}]Click here to launch Google search[/link]")
        
    console.print(advisor_table)

if __name__ == "__main__":
    main()