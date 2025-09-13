# run_pipeline.py
# The master orchestrator for the COMPASS-BRCA pipeline.
# This script manages the execution of all data processing steps,
# handles checkpointing to allow for resumability, and provides centralized logging.

import subprocess
import argparse
import logging
import time
from pathlib import Path
from rich.console import Console
from rich.panel import Panel
import compass_brca.pipeline_config as cfg

console = Console()# run_pipeline.py
# V4.0: A more powerful orchestrator. It can now pass custom command-line
# arguments to pipeline steps, allowing for more flexible workflows like
# specifying custom input/output directories.

import subprocess, argparse, logging, time
from rich.console import Console
from rich.panel import Panel
import compass_brca.pipeline_config as cfg

console = Console()

# --- NEW DIRECTORY DEFINITION ---
FILTERED_DATA_DIR = cfg.PROJECT_ROOT / "02a_data_filtered"

# --- Pipeline Definition with Arguments ---
PIPELINE_STEPS = {
    "fetch": {"script": "step01_fetch_data.py", "checkpoint": cfg.RAW_DATA_DIR / ".fetch_complete"},
    "audit": {"script": "step02_audit_raw_data.py", "checkpoint": cfg.RAW_DATA_DIR / ".audit_complete"},
    "clean": {"script": "step03_clean_and_normalize.py", "checkpoint": cfg.INTERIM_DATA_DIR / ".clean_complete"},
    "crosswalk": {"script": "step04_build_crosswalk.py", "checkpoint": cfg.ID_CROSSWALK_FILE"},
    "vocabulary": {"script": "step05_build_vocabulary.py", "checkpoint": cfg.VOCABULARY_FILE},
    
    # --- NEW STEP ADDED HERE ---
    "filter_by_vocab": {"script": "step05a_filter_by_vocabulary.py", "checkpoint": FILTERED_DATA_DIR / ".filter_complete"},
    
    # --- ANNOTATE STEP IS MODIFIED TO USE THE FILTERED DATA ---
    "annotate": {
        "script": "step06_annotate_data.py",
        "checkpoint": cfg.FINAL_FEATURES_DIR / ".annotate_complete",
        # Custom arguments are now passed to the script
        "args": ["--input-dir", str(FILTERED_DATA_DIR)] 
    },
    
    "missingness": {"script": "step07_analyze_missingness.py", "checkpoint": cfg.CONSOLIDATED_MISSINGNESS_REPORT},
    "reconcile": {"script": "step08_reconcile_manifests.py", "checkpoint": cfg.MANUAL_DOWNLOAD_LIST},
    "balance_cohorts": {"script": "step09_create_balanced_cohorts.py", "checkpoint": cfg.BALANCED_FEATURES_DIR / ".balance_complete"},
}

def run_step(step_name: str, config: dict, logger: logging.Logger) -> bool:
    script_path = cfg.PROJECT_ROOT / "scripts" / config["script"]
    checkpoint = config["checkpoint"]
    
    console.print(Panel(f"Starting Step: [bold cyan]{step_name}[/]", expand=False, border_style="blue"))
    if checkpoint.exists():
        logger.info(f"CHECKPOINT FOUND for step '{step_name}'. Skipping.")
        console.print(f"[bold green]CHECKPOINT FOUND[/]. Skipping step.")
        return True

    # --- UPGRADED COMMAND BUILDER ---
    command = ["python", str(script_path)]
    # Add any custom arguments defined in the PIPELINE_STEPS dictionary
    if "args" in config:
        command.extend(config["args"])
        
    logger.info(f"Running command: {' '.join(command)}")
    try:
        result = subprocess.run(command, check=True, capture_output=True, text=True, encoding='utf-8')
        logger.info(f"Output from {step_name}:\\n{result.stdout}")
        if not checkpoint.exists(): checkpoint.touch()
        console.print(f"[bold green]Step '{step_name}' completed successfully.[/]")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"ERROR in step {step_name}. Exit code: {e.returncode}\\nSTDERR:\\n{e.stderr}")
        console.print(f"[bold red]ERROR[/] in step [bold]{step_name}[/]. See log for details.")
        return False

# ... (The rest of run_pipeline.py, including main(), is unchanged from the previous correct version) ...
def main():
    # This part remains the same as the V3.0 script
    parser = argparse.ArgumentParser(description="COMPASS-BRCA Pipeline Orchestrator")
    parser.add_argument("--from", dest='start_step', choices=PIPELINE_STEPS.keys(), help="Start from a specific step.")
    parser.add_argument("--steps", nargs='+', choices=PIPELINE_STEPS.keys(), help="Run only specific steps.")
    args = parser.parse_args()

    timestamp = time.strftime("%Y%m%d-%H%M%S")
    log_file = cfg.LOGS_DIR / f"run_pipeline_{timestamp}.log"
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(module)s - %(message)s', filename=log_file)
    logger = logging.getLogger()

    # Create all directories, including the new 02a_data_filtered
    FILTERED_DATA_DIR.mkdir(exist_ok=True)
    # The setup_project_structure function is no longer needed as we create dirs on the fly
    
    steps_to_run = list(PIPELINE_STEPS.keys())
    if args.start_step: steps_to_run = steps_to_run[steps_to_run.index(args.start_step):]
    elif args.steps: steps_to_run = args.steps
            
    console.print(Panel("[bold magenta]COMPASS-BRCA Pipeline Initialized[/]", subtitle=f"Logging to {log_file}"))
    start_time = time.time()
    
    for step_name in steps_to_run:
        if not run_step(step_name, PIPELINE_STEPS[step_name], logger):
            console.print(f"[bold red]Pipeline halted due to error in step '{step_name}'.[/]")
            break
            
    end_time = time.time()
    console.print(f"\\n[bold green]Pipeline execution finished in {end_time - start_time:.2f} seconds.[/]")

if __name__ == "__main__": main()

# --- Pipeline Step Definition ---
# This dictionary defines each step, its corresponding script, and the
# checkpoint file that signifies its successful completion.
PIPELINE_STEPS = {
    "fetch": {"script": "step01_fetch_data.py", "checkpoint": cfg.RAW_DATA_DIR / ".fetch_complete"},
    "audit": {"script": "step02_audit_raw_data.py", "checkpoint": cfg.RAW_DATA_DIR / ".audit_complete"},
    "clean": {"script": "step03_clean_and_normalize.py", "checkpoint": cfg.INTERIM_DATA_DIR / ".clean_complete"},
    "crosswalk": {"script": "step04_build_crosswalk.py", "checkpoint": cfg.ID_CROSSWALK_FILE},
    "vocabulary": {"script": "step05_build_vocabulary.py", "checkpoint": cfg.VOCABULARY_FILE},
    "annotate": {"script": "step06_annotate_data.py", "checkpoint": cfg.FINAL_FEATURES_DIR / ".annotate_complete"},
    "missingness": {"script": "step07_analyze_missingness.py", "checkpoint": cfg.CONSOLIDATED_MISSINGNESS_REPORT},
    "reconcile": {"script": "step08_reconcile_manifests.py", "checkpoint": cfg.MANUAL_DOWNLOAD_LIST},
    "balance_cohorts": {"script": "step09_create_balanced_cohorts.py", "checkpoint": cfg.BALANCED_FEATURES_DIR / ".balance_complete"},
}

def setup_project_structure(logger):
    """Ensures all necessary directories from the config exist."""
    logger.info("Verifying project directory structure...")
    for path in cfg.ALL_DIRS:
        path.mkdir(parents=True, exist_ok=True)
    logger.info("Project structure verified.")

def run_step(step_name: str, config: dict, logger: logging.Logger) -> bool:
    """
    Executes a single step of the pipeline, checking for checkpoints and logging output.
    Returns True on success, False on failure.
    """
    script_path = cfg.PROJECT_ROOT / "scripts" / config["script"]
    checkpoint = config["checkpoint"]
    
    console.print(Panel(f"Starting Step: [bold cyan]{step_name}[/]", expand=False, border_style="blue"))
    
    if checkpoint.exists():
        logger.info(f"CHECKPOINT FOUND for step '{step_name}' at: {checkpoint}. Skipping.")
        console.print(f"[bold green]CHECKPOINT FOUND[/]. Skipping step.")
        return True

    # Use the -m flag for robust module execution
    command = ["python", "-m", f"scripts.{script_path.stem}"]
    logger.info(f"Running command: {' '.join(command)}")

    try:
        # Execute the script and let it handle its own UI.
        result = subprocess.run(command, check=True, capture_output=True, text=True, encoding='utf-8')
        logger.info(f"Output from {step_name}:\n{result.stdout}")
        
        # Verify checkpoint creation
        if not checkpoint.exists():
            checkpoint.parent.mkdir(parents=True, exist_ok=True)
            checkpoint.touch()
        
        console.print(f"[bold green]Step '{step_name}' completed successfully.[/]")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"ERROR in step {step_name}. Process returned non-zero exit code: {e.returncode}")
        logger.error(f"STDOUT:\n{e.stdout}")
        logger.error(f"STDERR:\n{e.stderr}")
        console.print(f"[bold red]ERROR[/] in step [bold]{step_name}[/]. See `run_pipeline.log` for detailed error message.")
        return False

def main():
    parser = argparse.ArgumentParser(description="COMPASS-BRCA Pipeline Orchestrator")
    parser.add_argument("--from", dest='start_step', choices=PIPELINE_STEPS.keys(), help="Start execution from a specific step.")
    parser.add_argument("--steps", nargs='+', choices=PIPELINE_STEPS.keys(), help="Run only specific steps.")
    args = parser.parse_args()

    timestamp = time.strftime("%Y%m%d-%H%M%S")
    log_file = cfg.LOGS_DIR / f"run_pipeline_{timestamp}.log"
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(module)s - %(message)s', filename=log_file)
    logger = logging.getLogger()

    setup_project_structure(logger)
    
    steps_to_run = list(PIPELINE_STEPS.keys())
    if args.start_step:
        steps_to_run = steps_to_run[steps_to_run.index(args.start_step):]
    elif args.steps:
        steps_to_run = args.steps
            
    console.print(Panel("[bold magenta]COMPASS-BRCA Pipeline Initialized[/]", title="Welcome", subtitle=f"Logging to {log_file}"))
    start_time = time.time()
    
    for step_name in steps_to_run:
        if not run_step(step_name, PIPELINE_STEPS[step_name], logger):
            console.print(f"[bold red]Pipeline halted due to error in step '{step_name}'.[/]")
            break
            
    end_time = time.time()
    console.print(f"\n[bold green]Pipeline execution finished in {end_time - start_time:.2f} seconds.[/]")

if __name__ == "__main__":
    main()