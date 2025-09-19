# scripts/utility_inspect_csv_headers.py
# V2 - Corrected for NameError
# A simple tool to read the first few lines of a CSV and print its headers.
import pandas as pd
from pathlib import Path

def inspect_file(target_path: Path):
    """Reads the headers from a single CSV file."""
    print(f"\n--- Inspecting Headers for: {target_path.name} ---")

    if not target_path.exists():
        print(f"!!! ERROR: File not found at {target_path}")
        return

    try:
        # We only need to read the first 5 rows to get the headers
        df = pd.read_csv(target_path, nrows=5, low_memory=False)
        
        print("\n[ SUCCESS ] File loaded. Column headers are:\n")
        
        # Print each column name on a new line
        for col in df.columns:
            print(f"  - {col}")
            
    except Exception as e:
        print(f"!!! ERROR: Could not read the CSV file. Reason: {e}")

def main():
    """Defines the targets and runs the inspection for each."""
    # --- CONFIGURATION ---
    # Define the two files we need to investigate.
    GDSC_TARGET = Path("01_data_raw/targeted_hunt_cycle2/Model.csv")
    CCLE_MODEL_TARGET = Path("01_data_raw/targeted_hunt_cycle2/data_clinical_patient.txt")
    # --- END CONFIGURATION ---

    # --- EXECUTION ---
    inspect_file(GDSC_TARGET)
    inspect_file(CCLE_MODEL_TARGET)
    # --- END EXECUTION ---


if __name__ == "__main__":
    main()