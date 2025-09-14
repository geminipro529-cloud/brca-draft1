# scripts/step08_reconcile_manifests.py
import pandas as pd
from pathlib import Path
from compass_brca.utils import pipeline_config as cfg

def main():
    # Define MASTER_MANIFEST and MANUAL_DOWNLOAD_LIST locally
    MASTER_MANIFEST = cfg.MANIFESTS_DIR / "master_manifest_FINAL.csv"
    MANUAL_DOWNLOAD_LIST = cfg.PRIMARY_DATA_DIR / "manual_download_list.txt"

    if not MASTER_MANIFEST.exists():
        print(f"ERROR: Manifest file not found at {MASTER_MANIFEST}")
        return

    manifest_df = pd.read_csv(MASTER_MANIFEST)
    missing_urls = [row['url'] for _, row in manifest_df.iterrows() if not Path(row['output_path']).exists()]
    
    MANUAL_DOWNLOAD_LIST.parent.mkdir(parents=True, exist_ok=True)
    if missing_urls:
        print(f"Found {len(missing_urls)} files from manifest that are missing from the raw data folder.")
        MANUAL_DOWNLOAD_LIST.write_text("\n".join(missing_urls))
        print(f"List of missing URLs saved to {MANUAL_DOWNLOAD_LIST}")
    else:
        print("All files in manifest are accounted for. Reconciliation complete.")
        # Create an empty file as a checkpoint
        MANUAL_DOWNLOAD_LIST.touch()

if __name__ == "__main__":
    main()