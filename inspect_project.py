# inspect_project.py
# A read-only diagnostic tool for the COMPASS-BRCA pipeline.
# Run this script from the project root to get a full status report
# and an intelligent recommendation for the next step.

import sys
from pathlib import Path
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.tree import Tree

try:
    # Check for a key file to ensure the script is run from the project root.
    if not Path("run_pipeline.py").exists():
        raise ImportError

    import compass_brca.utils.pipeline_config as cfg
    from run_pipeline import PIPELINE_STEPS
except ImportError:
    print("\n[bold red]ERROR:[/bold red] This script must be run from the project root directory (the one containing 'run_pipeline.py').")
    sys.exit(1)

console = Console()

def display_data_structure():
    """Scans and displays the current state of the data directories."""
    console.print(Panel("[bold cyan]Current Data Structure[/]", expand=False))
    tree = Tree(f":file_folder: [bold]{cfg.PROJECT_ROOT.name}[/]", guide_style="green")
    for data_dir in cfg.ALL_DIRS:
        if data_dir.exists():
            contents = list(data_dir.iterdir())
            num_files = sum(1 for item in contents if item.is_file())
            style = "bold green" if num_files > 0 else "dim yellow"
            branch = tree.add(f":file_folder: [{style}]{data_dir.name}[/]")
            branch.add(f"[italic]{num_files} files[/]")
        else:
            tree.add(f":file_folder: [dim red]{data_dir.name} (Not Found)[/]")
    console.print(tree)

def analyze_pipeline_status():
    """Checks checkpoints to determine the status and provide recommendations."""
    console.print(Panel("[bold cyan]Pipeline Status & Recommendation[/]", expand=False))
    table = Table()
    table.add_column("Step Name", style="cyan")
    table.add_column("Checkpoint", style="yellow")
    table.add_column("Status", justify="right")

    next_step = None
    for step_name, config in PIPELINE_STEPS.items():
        status = "[bold green]COMPLETE[/]" if config["checkpoint"].exists() else "[bold yellow]PENDING[/]"
        if "PENDING" in status and not next_step:
            next_step = step_name
        table.add_row(step_name, str(config["checkpoint"].relative_to(cfg.PROJECT_ROOT)), status)
    console.print(table)
    
    if not cfg.MASTER_MANIFEST.exists():
        rec_text = f"CRITICAL: The master manifest is missing.\n1. Create: `[cyan]{cfg.MASTER_MANIFEST}[/]`\n2. Add URLs to your data.\n3. Run the pipeline."
    elif next_step:
        rec_text = f"The pipeline is ready to continue at step `[bold]{next_step}[/]`.\nRun this command:\n[white on black] python run_pipeline.py --from {next_step} [/]"
    else:
        rec_text = "The core data processing pipeline is complete!\nYou can now run the advanced analytical scripts, e.g.:\n[white on black] python scripts/analysis_survival.py [/]"
    
    console.print(Panel(rec_text, title="[bold]Next Action[/]", border_style="blue"))

def main():
    display_data_structure()
    analyze_pipeline_status()

if __name__ == "__main__":
    main()