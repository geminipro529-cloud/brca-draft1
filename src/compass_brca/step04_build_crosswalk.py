# src/compass_brca/step04_build_crosswalk.py
# DEFINITIVE VERSION V21 (Smarter Output Handling)
import sys
import pandas as pd
from pathlib import Path
from dask.distributed import Client, LocalCluster, wait
from collections import Counter

try:
    from compass_brca.pipeline_config import (
        INTERIM_DATA_DIR, PRIMARY_DATA_DIR, MASTER_CROSSWALK_PATH, LOGS_DIR
    )
except ImportError:
    project_root = Path(__file__).resolve().parents[2]
    sys.path.append(str(project_root))
    from src.compass_brca.pipeline_config import (
        INTERIM_DATA_DIR, PRIMARY_DATA_DIR, MASTER_CROSSWALK_PATH, LOGS_DIR
    )

from rich.console import Console
from rich.progress import track

console = Console()

N_WORKERS = 2
MEMORY_LIMIT = "6GB"
BATCH_SIZE = 250
FAILURE_THRESHOLD = 3 

def get_failure_counts_from_logs() -> Counter:
    counts = Counter()
    for log_file in LOGS_DIR.glob("step04_forensic_log_*.txt"):
        with open(log_file, 'r') as f:
            for line in f:
                counts[line.strip()] += 1
    return counts

def process_parquet_file(file_path: str, log_path: Path) -> pd.DataFrame:
    with open(log_path, 'a') as f:
        f.write(f"{file_path}\n")
    try:
        df = pd.read_parquet(file_path, engine='pyarrow')
        text_columns = df.select_dtypes(include=['object', 'string']).columns
        if text_columns.empty: return pd.DataFrame()
        return pd.DataFrame({'file_path': str(file_path), 'column_name': text_columns})
    except Exception:
        return pd.DataFrame()

def main():
    console.rule("[bold magenta]Step 04: Build Crosswalk (Self-Healing V21)[/]")
    LOGS_DIR.mkdir(exist_ok=True)
    forensic_log_file = LOGS_DIR / f"step04_forensic_log_current.txt"

    failure_counts = get_failure_counts_from_logs()
    known_bombs = {file for file, count in failure_counts.items() if count >= FAILURE_THRESHOLD}
    
    if known_bombs:
        console.print(f"[bold yellow]Found {len(known_bombs)} known bomb(s). They will be skipped.[/]")

    all_files = {str(p) for p in INTERIM_DATA_DIR.rglob("*.parquet")}
    files_to_process = sorted(list(all_files - known_bombs))
    
    if not files_to_process:
        console.print("[green]No new, non-blacklisted files found to process.[/]")
    else:
        console.print(f"Found [cyan]{len(files_to_process):,}[/] files to process.")
        with LocalCluster(n_workers=N_WORKERS, memory_limit=MEMORY_LIMIT, processes=True) as cluster, Client(cluster) as client:
            console.print(f"Dask dashboard: [link={client.dashboard_link}]{client.dashboard_link}[/link]")
            all_results = []
            for i in track(range(0, len(files_to_process), BATCH_SIZE), description="Submitting batches to Dask..."):
                batch = files_to_process[i:i + BATCH_SIZE]
                futures = client.map(process_parquet_file, batch, log_path=forensic_log_file)
                wait(futures)
                results = client.gather(futures, errors='skip')
                all_results.extend([res for res in results if isinstance(res, pd.DataFrame) and not res.empty])
        
        console.print("\n[bold]Dask processing complete. Concatenating results...[/]")
        if all_results:
            final_crosswalk = pd.concat(all_results, ignore_index=True)
            console.print(f"Generated crosswalk with [cyan]{len(final_crosswalk):,}[/] rows.")
            final_crosswalk.to_parquet(MASTER_CROSSWALK_PATH, index=False)
            console.print(f"Master crosswalk saved to: {MASTER_CROSSWALK_PATH}")

    # NEW LOGIC: Ensure the output file ALWAYS exists, even if empty.
    if not MASTER_CROSSWALK_PATH.exists():
        console.print("[yellow]No valid crosswalk data was generated. Creating empty output file.[/]")
        pd.DataFrame(columns=['file_path', 'column_name']).to_parquet(MASTER_CROSSWALK_PATH, index=False)
        
    console.rule("[bold green]Step 04: Build Crosswalk - COMPLETED[/]")

if __name__ == "__main__":
    main()