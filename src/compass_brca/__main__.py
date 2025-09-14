# compass_brca/__main__.py
# V5.2: DEFINITIVE ORCHESTRATOR

import subprocess
import sys
import shutil
from pathlib import Path
import argparse
import time
from rich.console import Console
from rich.panel import Panel

console = Console()
try:
    from compass_brca.pipeline_config import PRIMARY_DATA_DIR
except ImportError:
    console.print("[bold red]Fatal Error: Could not import pipeline_config.py.[/]")
    sys.exit(1)

PIPELINE_STEPS = [
    "step01_intelligent_fetch_and_process",
    "step02_audit_raw_data",
    "step03_clean_and_normalize",
    "step04_build_crosswalk_20",
    "step05_build_vocabularies_stratified",
]

def run_simple_command(command: list[str]) -> int:
    console.print(f"\n[bold cyan]Executing:[/bold cyan] [yellow]{' '.join(command)}[/]")
    result = subprocess.run(command)
    return result.returncode

def cleanup_for_step(step_name: str):
    console.print(f"\n[dim]Performing cleanup for [cyan]{step_name}[/]...[/]")
    if step_name == "step04_build_crosswalk_20":
        if PRIMARY_DATA_DIR.exists():
            shutil.rmtree(PRIMARY_DATA_DIR)
            console.print(f"  - Deleted old primary data directory.")

def main():
    parser = argparse.ArgumentParser(description="COMPASS-BRCA Master Pipeline Orchestrator")
    parser.add_argument("--from-step", choices=PIPELINE_STEPS, default=PIPELINE_STEPS[0], help="The step to start from.")
    args = parser.parse_args()
    start_index = PIPELINE_STEPS.index(args.from_step)
    steps_to_run = PIPELINE_STEPS[start_index:]
    
    console.rule(f"[bold magenta]Starting COMPASS-BRCA Pipeline Run (V5.2)[/]")

    for i, step in enumerate(steps_to_run):
        console.rule(f"Initiating Step {i+1}/{len(steps_to_run)}: {step}")
        cleanup_for_step(step)
        
        if step == "step04_build_crosswalk_20":
            prereq_cmd = ["python", "scripts/utility_stratify_by_size.py"]
            prereq_exit_code = run_simple_command(prereq_cmd)
            if prereq_exit_code != 0:
                console.print(Panel(f"[bold red]Pipeline HALTED. Prerequisite for '{step}' failed.[/bold red]"))
                sys.exit(1)

        module_path = f"compass_brca.{step}"
        command = ["python", "-m", module_path]
        exit_code = run_simple_command(command)

        if exit_code != 0:
            console.print(Panel(f"[bold red]Pipeline HALTED. Step '{step}' failed with exit code {exit_code}.[/bold red]", expand=False))
            sys.exit(1)
        
        console.print(f"[bold green]✓ Step {step} Completed Successfully![/bold green]")

    console.rule("[bold green]COMPASS-BRCA Pipeline Run Completed Successfully![/]")

if __name__ == "__main__":
    main()