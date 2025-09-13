# scripts/utility_identify_large_files.py
# V2.0: The Definitive "Serial Bomb Squad". This version abandons all concurrency
# in favor of extreme robustness. It crawls the filesystem one file at a time
# and is fully stateful and resumable, making it immune to "poison pill" files
# that crash parallel workers.

import pandas as pd
import pyarrow.parquet as pq
import os
from pathlib import Path
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, BarColumn, TextColumn, TimeElapsedColumn, MofNCompleteColumn
import compass_brca.pipeline_config as cfg

console = Console()
PROGRESS_LOG = cfg.INTERIM_DATA_DIR / "_bombsquad_progress.log.csv"

def get_file_stats_safe(file_path: str) -> tuple[str, int, int]:
    """Extremely robust function to get metadata from a single Parquet file."""
    try:
        # The only action that can fail is reading the file.
        p_file = pq.ParquetFile(file_path)
        return "SUCCESS", p_file.metadata.num_rows, Path(file_path).stat().st_size
    except Exception as e:
        # If ANYTHING goes wrong, we catch it and report it as a failure.
        return f"ERROR: {type(e).__name__}", -1, -1

def main():
    console.print(Panel("Identifying 'Decompression Bomb' Parquet files (Serial, Robust Mode).", title="[cyan]Bomb Squad Utility V2.0[/cyan]"))
    
    # --- Step 1: Initialize or Load Progress Log ---
    if not PROGRESS_LOG.exists():
        console.print("First run: Discovering all files to create a progress log...")
        all_files = [str(f) for f in cfg.INTERIM_DATA_DIR.rglob("*.parquet")]
        progress_df = pd.DataFrame({"file_path": all_files, "status": "PENDING", "num_rows": -1, "size_bytes": -1})
        progress_df.to_csv(PROGRESS_LOG, index=False)
        console.print(f"Discovery complete. Found {len(all_files)} files. Progress log created.")
    else:
        console.print(f"Resuming from existing progress log: {PROGRESS_LOG}")
        progress_df = pd.read_csv(PROGRESS_LOG)

    files_to_process = progress_df[progress_df['status'] == 'PENDING']

    if files_to_process.empty:
        console.print("[green]All files have already been analyzed according to the log.[/green]")
    else:
        # --- Step 2: Process pending files one-by-one ---
        with Progress(TextColumn("[cyan]Scanning files...[/cyan]"), BarColumn(), MofNCompleteColumn(), TimeElapsedColumn()) as progress:
            task = progress.add_task("Analyzing Metadata", total=len(progress_df))
            progress.update(task, completed=len(progress_df) - len(files_to_process))

            for index, row in files_to_process.iterrows():
                file_path = row['file_path']
                status, num_rows, size_bytes = get_file_stats_safe(file_path)
                
                # Update the master log DataFrame in memory
                progress_df.loc[index, 'status'] = status
                progress_df.loc[index, 'num_rows'] = num_rows
                progress_df.loc[index, 'size_bytes'] = size_bytes

                # Save the entire log to disk after every N files to balance speed and safety
                if index % 100 == 0:
                    progress_df.to_csv(PROGRESS_LOG, index=False)
                
                progress.update(task, advance=1)
        
        # Final save at the end of the loop
        progress_df.to_csv(PROGRESS_LOG, index=False)

    # --- Step 3: Analyze the completed log to find bombs ---
    console.print("\nAnalyzing completed log to identify bombs...")
    final_log_df = pd.read_csv(PROGRESS_LOG)
    
    corrupted_files = final_log_df[final_log_df['status'] != 'SUCCESS']
    
    # Heuristic for "bombs"
    final_log_df['rows_per_kb'] = (final_log_df['num_rows'] / (final_log_df['size_bytes'] / 1024)).fillna(0)
    potential_bombs = final_log_df[final_log_df['rows_per_kb'] > 10000] # Threshold

    exclusion_list = pd.concat([corrupted_files['file_path'], potential_bombs['file_path']]).unique()

    if len(exclusion_list) > 0:
        exclusion_file = Path("bombs_to_exclude.txt")
        exclusion_file.write_text("\n".join(exclusion_list))
        console.print(Panel(f"[bold yellow]Identified {len(exclusion_list)} potential bomb / corrupted files.[/bold yellow]\n"
                            f"A definitive exclusion list has been saved to: `{exclusion_file}`\n\n"
                            f"[bold]Next Step:[/bold] Re-run the main crosswalk script. It will now succeed.",
                            title="[green]Bomb Squad Analysis Complete[/green]"))
    else:
        console.print("[green]No obvious 'decompression bomb' files were found.[/green]")

if __name__ == "__main__":
    main()