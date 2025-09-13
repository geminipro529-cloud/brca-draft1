# compass_brca/pipeline_config.py
#
# DEFINITIVE VERSION (Corrected Root Path)
# =============================================================================
# COMPASS-BRCA: SINGLE SOURCE OF TRUTH
# =============================================================================
# This file contains all essential configuration variables for the pipeline.
# This version includes the definitive fix to correctly locate the project root.

from pathlib import Path

# --- V.FINAL FIX: Definitive Project Root Calculation ---
# This file is located at: D:/breast_cancer_project_root/compass_brca/pipeline_config.py
# .parents[0] is the directory containing this file (compass_brca)
# .parents[1] is the PARENT of that directory, which is the true project root.
PROJECT_ROOT = Path(__file__).resolve().parents[1]

# --- Core Data Directories ---
# All directories below will now be correctly created INSIDE the project root.
MANIFESTS_DIR = PROJECT_ROOT / "00_manifests"
RAW_DATA_DIR = PROJECT_ROOT / "01_data_raw"
INTERIM_DATA_DIR = PROJECT_ROOT / "02_data_interim"
FILTERED_DATA_DIR = PROJECT_ROOT / "02a_data_filtered"
QUARANTINE_DIR = PROJECT_ROOT / "02b_data_quarantined"
CHUNKED_DIR = PROJECT_ROOT / "02c_data_chunked_safe"
CULPRIT_QUARANTINE_DIR = PROJECT_ROOT / "02d_data_culprits_quarantined"
PRIMARY_DATA_DIR = PROJECT_ROOT / "03_data_primary"
FEATURES_FINAL_DIR = PROJECT_ROOT / "04_features_final"
FEATURES_BALANCED_DIR = PROJECT_ROOT / "05_features_balanced"

# --- Ancillary Directories ---
LOGS_DIR = PROJECT_ROOT / "logs"
REPORTS_DIR = PROJECT_ROOT / "reports"

# --- Key Filenames ---
MASTER_MANIFEST_FINAL = "master_manifest_FINAL.csv"
SMALL_FILES_LIST_FILENAME = "small_files.txt"
LARGE_FILES_LIST_FILENAME = "large_files.txt"
MASTER_CROSSWALK_FILENAME = "master_crosswalk.parquet"
MASTER_VOCABULARY_FILENAME = "master_vocabulary.parquet"

# --- Full File Paths (Convenience variables) ---
MASTER_MANIFEST_PATH = MANIFESTS_DIR / MASTER_MANIFEST_FINAL
SMALL_FILES_LIST_PATH = PRIMARY_DATA_DIR / SMALL_FILES_LIST_FILENAME
LARGE_FILES_LIST_PATH = PRIMARY_DATA_DIR / LARGE_FILES_LIST_FILENAME
MASTER_CROSSWALK_PATH = PRIMARY_DATA_DIR / MASTER_CROSSWALK_FILENAME # This now correctly points to the dataset directory
MASTER_VOCABULARY_PATH = PRIMARY_DATA_DIR / MASTER_VOCABULARY_FILENAME

# (The self-test block and initialize_directories function are unchanged)

def initialize_directories():
    """Creates all necessary pipeline directories if they do not exist."""
    print("Initializing pipeline directories...")
    dirs_to_create = [
        MANIFESTS_DIR, RAW_DATA_DIR, INTERIM_DATA_DIR, FILTERED_DATA_DIR,
        QUARANTINE_DIR, CHUNKED_DIR, CULPRIT_QUARANTINE_DIR, PRIMARY_DATA_DIR,
        FEATURES_FINAL_DIR, FEATURES_BALANCED_DIR, LOGS_DIR, REPORTS_DIR
    ]
    for dir_path in dirs_to_create:
        dir_path.mkdir(parents=True, exist_ok=True)
    print("Directory structure verified.")

if __name__ == "__main__":
    from rich.console import Console
    from rich.table import Table
    console = Console()
    console.rule("[bold cyan]Pipeline Configuration Verification[/bold cyan]")
    table = Table(title="Defined Pipeline Paths")
    table.add_column("Variable Name", style="cyan"); table.add_column("Path Value", style="magenta"); table.add_column("Exists?", style="yellow")
    all_paths = {k: v for k, v in locals().items() if isinstance(v, Path)}
    for name, path_obj in sorted(all_paths.items()):
        exists_str = "✅ [green]Yes[/green]" if path_obj.exists() else "❌ [red]No[/red]"
        table.add_row(name, str(path_obj), exists_str)
    console.print(table)