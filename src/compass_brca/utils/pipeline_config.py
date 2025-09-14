# compass_brca/pipeline_config.py
# DEFINITIVE VERSION (Reads from config.yml)

import yaml
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_FILE_PATH = PROJECT_ROOT / "config.yml"

if not CONFIG_FILE_PATH.exists():
    raise FileNotFoundError("CRITICAL: config.yml not found in project root!")

with open(CONFIG_FILE_PATH, 'r') as f:
    config = yaml.safe_load(f)

# --- Expose parameters from the config file ---
DASK_N_WORKERS = config['dask_cluster']['n_workers']
DASK_MEMORY_LIMIT = config['dask_cluster']['memory_limit']
STEP04_BATCH_SIZE = config['step04_build_crosswalk']['batch_size']
STEP04_COLUMN_CHUNK_SIZE = config['step04_build_crosswalk']['column_chunk_size']

# --- Core Data Directories ---
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
LOGS_DIR = PROJECT_ROOT / "logs"
REPORTS_DIR = PROJECT_ROOT / "reports"
MASTER_CROSSWALK_FILENAME = "master_crosswalk.parquet"
MASTER_VOCABULARY_FILENAME = "master_vocabulary.parquet"