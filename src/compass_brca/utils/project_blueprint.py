# compass_brca/utils/project_blueprint.py
# DEFINITIVE VERSION (V2.0)

import sys
from pathlib import Path
import ast
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.tree import Tree

console = Console()

# This import will now succeed because pipeline_config.py is a sibling module.
try:
    from .pipeline_config import (
        PROJECT_ROOT, MANIFESTS_DIR, RAW_DATA_DIR, INTERIM_DATA_DIR, 
        FILTERED_DATA_DIR, QUARANTINE_DIR, CHUNKED_DIR, CULPRIT_QUARANTARANTINE_DIR, 
        PRIMARY_DATA_DIR, FEATURES_FINAL_DIR, FEATURES_BALANCED_DIR, 
        LOGS_DIR, REPORTS_DIR
    )
    COMPASS_BRCA_DIR = PROJECT_ROOT / "compass_brca"
except ImportError:
    console.print("[bold red]Fatal Error:[/bold red] Could not import from 'compass_brca.pipeline_config'.")
    console.print("Please ensure 'pipeline_config.py' is inside the 'compass_brca' directory and you have run 'pip install -e .'.")
    sys.exit(1)

ALL_DIRS = [
    MANIFESTS_DIR, RAW_DATA_DIR, INTERIM_DATA_DIR, FILTERED_DATA_DIR,
    QUARANTINE_DIR, CHUNKED_DIR, CULPRIT_QUARANTINE_DIR, PRIMARY_DATA_DIR,
    FEATURES_FINAL_DIR, FEATURES_BALANCED_DIR, LOGS_DIR, REPORTS_DIR
]

def get_script_summary(script_path: Path) -> dict:
    summary = {"Purpose": "No docstring found.", "Input": "N/A", "Output": "N/A"}
    try:
        with open(script_path, "r", encoding="utf-8") as f: source = f.read()
        tree = ast.parse(source)
        docstring = ast.get_docstring(tree)
        if not docstring: return summary
        lines = [line.strip() for line in docstring.strip().split('\n')]
        purpose_lines, input_lines, output_lines = [], [], []
        current_section = "Purpose"
        for line in lines:
            if not line: continue
            line_lower = line.lower()
            if line_lower.startswith("input:"):
                current_section = "Input"; input_lines.append(line.split(":", 1)[1].strip())
            elif line_lower.startswith("output:"):
                current_section = "Output"; output_lines.append(line.split(":", 1)[1].strip())
            else:
                if current_section == "Purpose": purpose_lines.append(line)
                elif current_section == "Input": input_lines.append(line)
                elif current_section == "Output": output_lines.append(line)
        summary["Purpose"] = " ".join(purpose_lines) if purpose_lines else "Not specified."
        summary["Input"] = " ".join(input_lines) if input_lines else "Not specified."
        summary["Output"] = " ".join(output_lines) if output_lines else "Not specified."
    except Exception:
        summary["Purpose"] = "Error parsing script."
    return summary

def main():
    console.rule("[bold magenta]COMPASS-BRCA Project Blueprint[/bold magenta]")
    console.print("\n[bold]1. Project Directory Structure:[/bold]\n")
    tree = Tree(f"[cyan]{PROJECT_ROOT.name}[/]")
    for dir_path in sorted(ALL_DIRS, key=lambda p: p.name):
        exists_icon = "✅" if dir_path.exists() else "❌"
        tree.add(f"{exists_icon} [green]{dir_path.name}[/]")
    console.print(tree)
    console.print("\n[bold]2. Core Pipeline Scripts Analysis:[/bold]\n")
    script_table = Table(title="Pipeline Scripts Breakdown", header_style="bold magenta")
    script_table.add_column("Script Name", style="cyan", no_wrap=True)
    script_table.add_column("Purpose", style="white")
    script_table.add_column("Input", style="yellow")
    script_table.add_column("Output Location", style="green")
    if not COMPASS_BRCA_DIR.exists():
        console.print(f"[red]Error: Core package directory not found at {COMPASS_BRCA_DIR}[/red]")
        return
    step_scripts = sorted(list(COMPASS_BRCA_DIR.glob("step*.py")))
    for script in step_scripts:
        summary = get_script_summary(script)
        script_table.add_row(script.name, summary["Purpose"], summary["Input"], summary["Output"])
    console.print(script_table)
    console.print(Panel("This blueprint provides a ground-truth view of the project's structure.", title="[dim]Report Complete[/dim]"))

if __name__ == "__main__":
    main()