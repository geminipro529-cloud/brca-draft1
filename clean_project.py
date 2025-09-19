# clean_project.py
# A high-speed, targeted cleanup utility.

import shutil
from pathlib import Path
import os

# --- CONFIGURATION ---
# We only need to search in folders where Python code exists.
# This avoids searching the massive data directories.
FOLDERS_TO_CLEAN = ["src", "scripts"]
# --- END CONFIGURATION ---

def main():
    """Finds and deletes __pycache__ and .egg-info directories."""
    project_root = Path(__file__).resolve().parent
    print("--- Starting High-Speed Project Cleanup ---")

    # --- Clean __pycache__ ---
    pycache_count = 0
    print("\nSearching for __pycache__ directories...")
    for folder_name in FOLDERS_TO_CLEAN:
        search_path = project_root / folder_name
        if not search_path.exists():
            continue
        for root, dirs, files in os.walk(search_path):
            if "__pycache__" in dirs:
                pycache_path = Path(root) / "__pycache__"
                print(f"  - Deleting: {pycache_path}")
                shutil.rmtree(pycache_path)
                pycache_count += 1
    print(f"--> Found and deleted {pycache_count} __pycache__ director(y/ies).")

    # --- Clean .egg-info ---
    egg_info_count = 0
    print("\nSearching for .egg-info directories...")
    for path in project_root.glob("*.egg-info"):
        if path.is_dir():
            print(f"  - Deleting: {path}")
            shutil.rmtree(path)
            egg_info_count += 1
    print(f"--> Found and deleted {egg_info_count} .egg-info director(y/ies).")
    
    print("\n--- Cleanup Complete ---")

if __name__ == "__main__":
    main()