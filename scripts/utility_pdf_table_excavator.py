# scripts/utility_pdf_table_excavator.py
# A specialized tool for extracting tables from a single, targeted PDF.

import camelot
import pandas as pd
from pathlib import Path

# --- CONFIGURATION ---
# This is a manual, one-at-a-time process.
# You will change this path to point to the specific PDF you want to analyze.
PDF_TARGET_PATH = Path("01_data_raw/targeted_hunt_cycle2/some_supplementary_file.pdf")

# The output will be saved here, named after the PDF.
OUTPUT_DIR = Path("02_data_interim/pdf_extractions")
# --- END CONFIGURATION ---

def main():
    print(f"--- Starting PDF Table Excavation on: {PDF_TARGET_PATH.name} ---")

    if not PDF_TARGET_PATH.exists():
        print(f"!!! ERROR: Target PDF not found at {PDF_TARGET_PATH}")
        return

    OUTPUT_DIR.mkdir(exist_ok=True)
    
    print("\n[1] Reading PDF with Camelot (this may take a moment)...")
    # 'pages="all"' tells Camelot to scan the entire document.
    # 'flavor="lattice"' is best for tables with gridlines. Use 'stream' for others.
    try:
        tables = camelot.read_pdf(str(PDF_TARGET_PATH), pages='all', flavor='lattice')
    except Exception as e:
        print(f"!!! ERROR: Camelot failed to process the PDF. It may be an image-based PDF or corrupted.")
        print(f"    Details: {e}")
        return

    if not tables:
        print("!!! No tables found by Camelot in this PDF.")
        return

    print(f"\n[2] Found {tables.n} potential tables. Now exporting to CSV for review.")

    # --- Export and Report ---
    for i, table in enumerate(tables):
        # The raw extracted DataFrame
        df = table.df
        
        # Save the raw extraction to a CSV file for manual inspection
        output_csv_path = OUTPUT_DIR / f"{PDF_TARGET_PATH.stem}_table_{i+1}.csv"
        df.to_csv(output_csv_path, index=False)

        print(f"\n--- Table {i+1} ---")
        print(f"  - Shape: {df.shape[0]} rows, {df.shape[1]} columns")
        print(f"  - Accuracy Score: {table.parsing_report['accuracy']:.2f}%")
        print(f"  - Saved to: {output_csv_path}")
        print("  - Preview (first 5 rows):")
        print(df.head())

    print("\n[3] --- EXCAVATION COMPLETE ---")
    print(">>> CRITICAL NEXT STEP: Manually open and inspect each generated CSV file.")
    print(">>> Compare them to the original PDF to verify correctness before using this data.")

if __name__ == "__main__":
    main()