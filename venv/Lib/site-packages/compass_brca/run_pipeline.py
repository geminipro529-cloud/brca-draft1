import subprocess, sys
from pathlib import Path
from compass_brca.pipeline_config import *
from rich.console import Console
console = Console()
PIPELINE_STEPS = [
    {"name": "step01_fetch_data", "module": "compass_brca.step01_fetch_data", "key_output": RAW_DATA_DIR},
    {"name": "step02_audit_raw_data", "module": "compass_brca.step02_audit_raw_data", "key_output": None},
    {"name": "step03_clean_and_normalize", "module": "compass_brca.step03_clean_and_normalize", "key_output": INTERIM_DATA_DIR},
    {"name": "step03a_deep_scan_and_quarantine", "module": "compass_brca.step03a_deep_scan_and_quarantine", "key_output": None},
    {"name": "step04_build_crosswalk", "module": "compass_brca.step04_build_crosswalk", "key_output": MASTER_CROSSWALK_PATH, "prerequisite": "scripts/utility_stratify_by_size.py"},
    {
        "name": "step05_build_ccle_features", 
        "module": "compass_brca.step05_build_ccle_features", # <-- THIS MUST MATCH
        "key_output": [OUTPUT_NANO_FILE, OUTPUT_GENOMIC_FILE] 
    },
    {
        "name": "step05a_load_and_clean_tcga", 
        "module": "compass_brca.step05a_load_and_clean_tcga", # <-- THIS MUST MATCH
        "key_output": OUTPUT_TCGA_FEATURES
    },
    {
        "name": "step06_link_and_fuse_data",
        "module": "compass_brca.step06_link_and_fuse_data",
        "key_output": OUTPUT_FUSED_TABLE
    },
]
def is_step_complete(step_info: dict) -> bool:
    key_output = step_info.get("key_output")
    if key_output is None: return False
    outputs = key_output if isinstance(key_output, list) else [key_output]
    for output in outputs:
        output_path = Path(output)
        if output_path.is_dir():
            if not (output_path.exists() and any(output_path.iterdir())): return False
        else:
            if not (output_path.exists() and output_path.stat().st_size > 0): return False
    return True
def run_prerequisite(step_info: dict):
    prereq_script = step_info.get("prerequisite")
    if prereq_script:
        command = [sys.executable, str(PROJECT_ROOT / prereq_script)]
        console.print(f"[dim]  Running prerequisite: {prereq_script}...[/]")
        result = subprocess.run(command)
        if result.returncode != 0:
            console.print(f"[bold red]Prerequisite script {prereq_script} failed! Halting.[/]"); sys.exit(1)
        console.print(f"[dim]  Prerequisite complete.[/]")
def run_step(step_info: dict):
    step_name = step_info["name"]
    console.rule(f"[bold cyan]Checking Step: {step_name}[/]")
    if is_step_complete(step_info):
        console.print(f"[bold green]✓ SKIPPING: Output for '{step_name}' already exists.[/]\n"); return
    run_prerequisite(step_info)
    console.print(f"[yellow]▶ EXECUTING: {step_name}[/]")
    command = [sys.executable, "-m", step_info["module"]]
    process = subprocess.run(command)
    if process.returncode != 0:
        console.print(f"[bold red]Error: Step '{step_name}' failed. Pipeline HALTED.[/]"); sys.exit(1)
    console.print(f"[bold green]✓ Step '{step_name}' completed successfully.[/]\n")
def main():
    console.rule("[bold magenta]COMPASS-BRCA Definitive Pipeline Start (V5.2)[/]")
    for step in PIPELINE_STEPS:
        run_step(step)
    console.rule("[bold green]COMPASS-BRCA Pipeline Completed Successfully![/]")
if __name__ == "__main__":
    main()