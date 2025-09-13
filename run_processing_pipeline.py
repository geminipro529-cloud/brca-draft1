# run_processing_pipeline.py
# DEFINITIVE VERSION

import subprocess
import sys
import shutil
from pathlib import Path
import argparse
from rich.console import Console
from rich.panel import Panel

try:
    from compass_brca.pipeline_config import PRIMARY_DATA_DIR, MASTER_VOCABULARY_PATH
except ImportError as e:
    console = Console()
    console.print(f"[bold red]Fatal Error: Could not import pipeline_config.py.[/]")
    sys.exit(1)

console = Console()

PIPELINE_STEPS = [
    "step01_intelligent_fetch_and_process",
    "step02_audit_raw_data",
    "step03_clean_and_normalize",
    "step04_build_crosswalk_20",
    "step05_build_vocabularies_stratified",
]

def run_command(command: list[str]) -> int:
    console.print(f"[bold cyan]Executing:[/bold cyan] [yellow]{' '.join(command)}[/]")
    # Use Popen to allow real-time output streaming
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, encoding='utf-8', errors='replace')
    while True:
        output = process.stdout.readline()
        if output == '' and process.poll() is not None:
            break
        if output:
            print(output.strip())
    return process.poll()

def cleanup_for_step(step_name: str):
    console.print(f"\n[bold]Performing cleanup for [cyan]{step_name}[/]...[/]")
    if step_name == "step04_build_crosswalk_20":
        if PRIMARY_DATA_DIR.exists():
            console.print(f"  - Deleting old primary data directory: [magenta]{PRIMARY_DATA_DIR}[/]")
            shutil.rmtree(PRIMARY_DATA_DIR)
    else:
        console.print("  - No specific cleanup required for this step.")
        
def prepare_for_step(step_name: str) -> bool:
    console.print(f"\n[bold]Running prerequisites for [cyan]{step_name}[/]...[/]")
    if step_name == "step04_build_crosswalk_20":
        console.print("  - Prerequisite: Stratifying files by size.")
        cmd = ["python", "scripts/utility_stratify_by_size.py"]
        exit_code = run_command(cmd)
        if exit_code != 0:
            console.print(f"[bold red]  - Prerequisite failed![/bold red]")
            return False
    else:
        console.print("  - No prerequisites required for this step.")
    return True

def main():
    parser = argparse.ArgumentParser(description="COMPASS-BRCA Master Pipeline Orchestrator")
    parser.add_argument("--from-step", choices=PIPELINE_STEPS, default=PIPELINE_STEPS[0], help="The step to start the pipeline from.")
    args = parser.parse_args()
    start_index = PIPELINE_STEPS.index(args.from_step)
    steps_to_run = PIPELINE_STEPS[start_index:]
    console.rule(f"[bold magenta]Starting COMPASS-BRCA Pipeline Run[/]\nFrom step: [cyan]{args.from_step}[/]")
    for step in steps_to_run:
        console.rule(f"Initiating: {step}")
        cleanup_for_step(step)
        if not prepare_for_step(step):
            console.print(Panel("[bold red]Pipeline HALTED due to prerequisite failure.[/]", expand=False))
            sys.exit(1)
        module_path = f"compass_brca.{step}"
        command = ["python", "-m", module_path]
        exit_code = run_command(command)
        if exit_code != 0:
            console.print(Panel(f"[bold red]Pipeline HALTED. Step '{step}' failed with exit code {exit_code}.[/bold red]", expand=False))
            console.print(f"To retry, fix the error and run:\npython run_processing_pipeline.py --from-step {step}")
            sys.exit(1)
    console.rule("[bold green]COMPASS-BRCA Pipeline Run Completed Successfully![/]")

if __name__ == "__main__":
    main()
