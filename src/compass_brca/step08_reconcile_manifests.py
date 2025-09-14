# scripts/step08_reconcile_manifests.py
import pandas as pd
from pathlib import Path
import compass_brca.utils.pipeline_config as cfg

def main():
    if not cfg.MASTER_MANIFEST.exists():
        print(f"ERROR: Manifest file not found at {cfg.MASTER_MANIFEST}")
        return

    manifest_df = pd.read_csv(cfg.MASTER_MANIFEST)
    missing_urls = [row['url'] for _, row in manifest_df.iterrows() if not Path(row['output_path']).exists()]
    
    cfg.MANUAL_DOWNLOAD_LIST.parent.mkdir(parents=True, exist_ok=True)
    if missing_urls:
        print(f"Found {len(missing_urls)} files from manifest that are missing from the raw data folder.")
        cfg.MANUAL_DOWNLOAD_LIST.write_text("\n".join(missing_urls))
        print(f"List of missing URLs saved to {cfg.MANUAL_DOWNLOAD_LIST}")
    else:
        print("All files in manifest are accounted for. Reconciliation complete.")
        # Create an empty file as a checkpoint
        cfg.MANUAL_DOWNLOAD_LIST.touch()

if __name__ == "__main__":
    main()