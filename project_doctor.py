import os
import ast
import re
from pathlib import Path
from datetime import datetime
import time

# --- Configuration ---
# ANSI escape codes for color output in the terminal
class Colors:
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BLUE = '\033[94m'
    HEADER = '\033[95m'
    ENDC = '\033[0m'

# The expected directory structure from your project context
EXPECTED_DIRS = [
    "00_manifests", "01_data_raw", "02_data_interim", "02a_data_filtered",
    "03_data_primary", "04_features_final", "05_features_balanced",
    "scripts", "logs"
]

# Mapping of pipeline steps to their primary output directory
PIPELINE_STEP_OUTPUTS = {
    "step01_fetch_data": "01_data_raw",
    "step03_clean_and_normalize": "02_data_interim",
    "step05a_filter_by_vocabulary": "02a_data_filtered",
    "step04_build_crosswalk": "03_data_primary", # Crosswalks are primary metadata
    "step06_annotate_data": "04_features_final",
    "step09_create_balanced_cohorts": "05_features_balanced"
}

# --- Helper Functions ---

def print_header(title):
    print(f"\n{Colors.HEADER}--- {title} ---{Colors.ENDC}")

def get_dir_summary(path):
    """Quickly summarizes a directory without reading file contents."""
    if not path.is_dir():
        return 0, 0
    
    # This can be slow on millions of files, so we add a timeout.
    start_time = time.time()
    files = []
    for f in path.rglob('*'):
        if f.is_file():
            files.append(f)
        if time.time() - start_time > 10: # 10-second timeout for a single dir scan
            print(f" {Colors.YELLOW}[WARNING] Directory scan of '{path}' is slow, providing partial results.{Colors.ENDC}")
            break
            
    total_size = sum(f.stat().st_size for f in files)
    return len(files), total_size

def format_size(size_bytes):
    """Formats bytes into a human-readable string."""
    if size_bytes == 0:
        return "0B"
    size_name = ("B", "KB", "MB", "GB", "TB")
    i = 0
    while size_bytes >= 1024 and i < len(size_name) -1:
        size_bytes /= 1024.0
        i += 1
    return f"{size_bytes:.2f}{size_name[i]}"

def analyze_script(script_path):
    """Parses a Python script to find functions and infer I/O."""
    try:
        with open(script_path, 'r', encoding='utf-8') as f:
            content = f.read()
            tree = ast.parse(content)
        
        # Find all function definitions
        functions = [node.name for node in ast.walk(tree) if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))]
        
        # Infer I/O by searching for directory names in the script's text
        inputs = [d for d in EXPECTED_DIRS if re.search(fr"['\"]{d}['\"]", content)]
        outputs = list(set(inputs) - {'01_data_raw'}) # Simple heuristic
        
        return functions, sorted(list(set(inputs)))
    except Exception as e:
        return [f"Error parsing: {e}"], [], []

# --- Main Diagnostic Function ---

def run_diagnostics():
    start_total_time = time.time()
    print(f"{Colors.BLUE}Starting COMPASS-BRCA Project Health Check...{Colors.ENDC}")
    
    # 1. Project Structure Verification
    print_header("PROJECT STRUCTURE & CONFIGURATION")
    root = Path.cwd()
    if not (root / 'pipeline_config.py').exists():
        print(f"{Colors.RED}[CRITICAL] 'pipeline_config.py' not found. Are you in the project root?{Colors.ENDC}")
        return

    print(f"Project Root: {Colors.GREEN}{root}{Colors.ENDC}")
    for dir_name in EXPECTED_DIRS:
        if (root / dir_name).is_dir():
            print(f"  - {dir_name:<25} {Colors.GREEN}[OK]{Colors.ENDC}")
        else:
            print(f"  - {dir_name:<25} {Colors.YELLOW}[MISSING]{Colors.ENDC}")
            
    if (root / 'requirements.txt').exists():
        print(f"  - {'requirements.txt':<25} {Colors.GREEN}[OK]{Colors.ENDC}")
    else:
        print(f"  - {'requirements.txt':<25} {Colors.RED}[MISSING]{Colors.ENDC}")


    # 2. Pipeline Step Status (Inferred from Outputs)
    print_header("PIPELINE STEP STATUS (INFERRED)")
    for step, out_dir in PIPELINE_STEP_OUTPUTS.items():
        dir_path = root / out_dir
        if dir_path.is_dir() and any(dir_path.iterdir()):
             print(f"  - {step:<30} {Colors.GREEN}[COMPLETED]{Colors.ENDC} (Output found in '{out_dir}')")
        else:
             print(f"  - {step:<30} {Colors.YELLOW}[PENDING]{Colors.ENDC} (No output in '{out_dir}')")
             

    # 3. Data State Summary
    print_header("DATA STATE SUMMARY")
    for dir_name in sorted([d for d in EXPECTED_DIRS if d.startswith('0')]):
        dir_path = root / dir_name
        if dir_path.is_dir():
            file_count, total_size = get_dir_summary(dir_path)
            if file_count > 0:
                print(f"  - {dir_name:<25} {Colors.BLUE}{file_count:>8,}{Colors.ENDC} files | Total size: {Colors.BLUE}{format_size(total_size)}{Colors.ENDC}")
            else:
                print(f"  - {dir_name:<25} {Colors.YELLOW}0{Colors.ENDC} files")

    # 4. Scripts & Functions Analysis
    print_header("SCRIPTS ANALYSIS")
    scripts_dir = root / 'scripts'
    if scripts_dir.is_dir():
        py_scripts = sorted(list(scripts_dir.glob('*.py')))
        for script in py_scripts:
            print(f"\n- Script: {Colors.BLUE}{script.name}{Colors.ENDC}")
            functions, io_dirs = analyze_script(script)
            if functions:
                print(f"  - Functions Found: {', '.join(functions)}")
            if io_dirs:
                print(f"  - Inferred I/O Dirs: {', '.join(io_dirs)}")
    else:
        print(f"{Colors.RED}[ERROR] 'scripts' directory not found!{Colors.ENDC}")

    # 5. Log File Check
    print_header("LOGS & RECENT ACTIVITY")
    logs_dir = root / 'logs'
    if logs_dir.is_dir() and any(logs_dir.iterdir()):
        latest_log = max(logs_dir.glob('*'), key=os.path.getmtime)
        mod_time = datetime.fromtimestamp(latest_log.stat().st_mtime)
        print(f"Latest log file: {Colors.GREEN}{latest_log.name}{Colors.ENDC}")
        print(f"Last modified:   {Colors.GREEN}{mod_time.strftime('%Y-%m-%d %H:%M:%S')}{Colors.ENDC}")
    else:
        print(f"{Colors.YELLOW}No log files found in 'logs' directory.{Colors.ENDC}")

    # --- Final Summary ---
    end_total_time = time.time()
    duration = end_total_time - start_total_time
    print_header("DIAGNOSTICS COMPLETE")
    print(f"Report generated in {duration:.2f} seconds.")
    if duration > 120:
        print(f"{Colors.YELLOW}Warning: Scan took longer than the 2-minute target. File system may be slow.{Colors.ENDC}")


if __name__ == "__main__":
    run_diagnostics()