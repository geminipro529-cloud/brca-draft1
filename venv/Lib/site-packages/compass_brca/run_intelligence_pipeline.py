# run_intelligence_pipeline.py
# DEFINITIVE V4 - "Operation Unification" Edition
import subprocess
import sys
from pathlib import Path

try:
    from compass_brca.pipeline_config import MANIFESTS_DIR
except ImportError:
    sys.exit("Fatal Error: Could not import from compass_brca.pipeline_config.")

from rich.console import Console
console = Console()

# The entire intelligence pipeline is now a single, powerful step.
INTELLIGENCE_STEPS = [
    {
        "name": "Generate Master Manifest",
        "script": "scripts/utility_generate_manifest_from_local_files.py",
        "key_output": MANIFESTS_DIR / "master_manifest_FINAL.csv"
    },
]

def is_step_complete(step_info: dict) -> bool:
    output_path = step_info.get("key_output")
    if not output_path: return False
    return output_path.exists() and output_path.stat().st_size > 0

def run_step(step_info: dict):
    step_name, script_path = step_info["name"], step_info["script"]
    console.rule(f"[bold green]Executing Intelligence Step: {step_name}[/]")
    if is_step_complete(step_info):
        console.print(f"[bold green]✓ SKIPPING: Output for '{step_name}' already exists.[/]\n")
        return
    console.print(f"[yellow]▶ EXECUTING: {step_name}[/]")
    command = [sys.executable, script_path]
    process = subprocess.run(command)
    if process.returncode != 0:
        console.print(f"[bold red]Error: Step '{step_name}' failed.[/]")
        sys.exit(1)
    console.print(f"[bold green]✓ Step '{step_name}' completed.[/]\n")

def main():
    console.rule("[bold magenta]Intelligence Pipeline Start (Unified V4)[/]")
    for step in INTELLIGENCE_STEPS:
        run_step(step)
    console.rule("[bold green]Master Manifest is ready.[/]")

if __name__ == "__main__":
    main()