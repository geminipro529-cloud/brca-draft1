# scripts/step06_annotate_data.py
# V2.0: Now accepts a command-line argument for the input directory,
# making it flexible enough to work with the new filtering step.

import pandas as pd
import argparse # <-- Added
from pathlib import Path # <-- Added
from rich.progress import track
from compass_brca.utils import pipeline_config as cfg

def main():
    # --- NEW: Argument Parsing ---
    parser = argparse.ArgumentParser(description="Annotate data with the master vocabulary.")
    parser.add_argument("--input-dir", type=str, default=str(cfg.INTERIM_DATA_DIR), 
                        help="Directory of cleaned Parquet files to annotate.")
    args = parser.parse_args()
    input_dir = Path(args.input_dir)
    # -----------------------------

    VOCABULARY_FILE = cfg.PRIMARY_DATA_DIR / cfg.MASTER_VOCABULARY_FILENAME
    if not VOCABULARY_FILE.exists(): print(f"Vocabulary file not found."); return
    vocab_df = pd.read_parquet(VOCABULARY_FILE)
    if vocab_df.empty: print("Vocabulary is empty. Skipping annotation."); (cfg.FEATURES_FINAL_DIR / ".annotate_complete").touch(); return

    # The new vocab has no category, so we create a simple lookup map
    # In a real scenario, you'd need a more sophisticated vocabulary with categories
    lookup_map = {'term': pd.Series(vocab_df.term.values, index=vocab_df.term).to_dict()}
    
    # Use the new input_dir variable
    files_to_process = list(input_dir.glob("*.parquet"))
    if not files_to_process: print(f"No files found in {input_dir} to annotate."); (cfg.FEATURES_FINAL_DIR / ".annotate_complete").touch(); return
    
    cfg.FEATURES_FINAL_DIR.mkdir(exist_ok=True)
    for file in track(files_to_process, description=f"Annotating files from {input_dir.name}..."):
        try:
            df = pd.read_parquet(file)
            # This logic remains the same
            candidates = {"gene": ["gene", "hugo_symbol", "symbol"], "drug": ["drug_name", "treatment"]}
            for category, mapping in lookup_map.items():
                for col in df.columns:
                    if col in candidates.get(category, []):
                        df[f"{col}_standardized"] = df[col].map(mapping)
            
            df.to_parquet(cfg.FEATURES_FINAL_DIR / file.name, index=False)
        except Exception as e: print(f"\\n[yellow]Warning:[/yellow] Error annotating {file.name}: {e}")

    (cfg.FEATURES_FINAL_DIR / ".annotate_complete").touch()

if __name__ == "__main__":
    main()