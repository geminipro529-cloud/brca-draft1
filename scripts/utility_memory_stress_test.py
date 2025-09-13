# scripts/utility_memory_stress_test.py
# The Definitive "Guardian" Bomb Squad V4.0. This script uses a robust
# subprocess-per-file model with a hard timeout to guarantee that even
# files that cause a C-level deadlock cannot stall the process.

import pandas as pd
import os
import subprocess
from pathlib import Path
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, BarColumn, TextColumn, TimeElapsedColumn, MofNCompleteColumn
import compass_brca.pipeline_config as cfg

console = Console()
PROGRESS_LOG = cfg.INTERIM_DATA_DIR / "_stresstest_progress.log.csv"
WORKER_SCRIPT = cfg.PROJECT_ROOT / "scripts" / "utility_memory_worker.py"
# Set a timeout. If a single file takes longer than this, it's a poison pill.
FILE_TIMEOUT_SECONDS = 120 # 2 minutes

def main():
    console.print(Panel(f"Performing Guardian Memory Stress Test\nTimeout per File: {FILE_TIMEOUT_SECONDS}s", title="[cyan]Memory Bomb Squad V4.0[/cyan]"))
    
    # --- Step 1: Initialize or Load Progress Log ---
    if not PROGRESS_LOG.exists():
        console.print("First run: Discovering all files...")
        all_files = [str(f) for f in cfg.INTERIM_DATA_DIR.rglob("*.parquet")]
        progress_df = pd.DataFrame({"file_path": all_files, "status": "PENDING", "peak_mem_mib": -1.0})
        progress_df.to_csv(PROGRESS_LOG, index=False)
    else:
        console.print(f"Resuming from existing progress log: {PROGRESS_LOG}")
        progress_df = pd.read_csv(PROGRESS_LOG)

    files_to_process = progress_df[progress_df['status'] == 'PENDING']
    if files_to_process.empty:
        console.print("[green]All files already stress-tested.[/green]")
    else:
        # --- Step 2: Guardian loop ---
        with Progress(TextColumn("[cyan]Guarding workers...[/cyan]"), BarColumn(), MofNCompleteColumn(), TimeElapsedColumn()) as progress:
            task = progress.add_task("Testing...", total=len(progress_df))
            progress.update(task, completed=len(progress_df) - len(files_to_process))

            for index, row in files_to_process.iterrows():
                file_path = row['file_path']
                status, peak_mem = "ERROR_UNKNOWN", -1.0
                try:
                    # Launch the disposable worker as a separate process with a timeout
                    result = subprocess.run(
                        ["python", str(WORKER_SCRIPT), file_path],
                        capture_output=True,
                        text=True,
                        timeout=FILE_TIMEOUT_SECONDS 
                    )
                    
                    output = result.stdout.strip()
                    if result.returncode == 0 and "SUCCESS" in output:
                        status, peak_mem_str = output.split(',')
                        peak_mem = float(peak_mem_str)
                    else:
                        status = f"ERROR_WORKER: {output}"

                except subprocess.TimeoutExpired:
                    # This is the crucial part that catches the freeze
                    status = "ERROR_TIMEOUT_FROZEN"
                    progress.console.print(f"\n[bold red]Guardian:[/bold red] Worker STALLED on `{Path(file_path).name}`. Killing process.")

                # Update the log DataFrame
                progress_df.loc[index, 'status'] = status
                progress_df.loc[index, 'peak_mem_mib'] = peak_mem
                
                # Save state after each file
                progress_df.to_csv(PROGRESS_LOG, index=False)
                progress.update(task, advance=1)
    
    # --- Step 3: Analyze the completed log ---
    console.print("\nAnalyzing completed log to generate final exclusion list...")
    final_log_df = pd.read_csv(PROGRESS_LOG)
    
    # Exclude if it timed out, errored, or used too much memory (e.g., > 1GB)
    bombs_df = final_log_df[
        (final_log_df['status'] != 'SUCCESS') | 
        (final_log_df['peak_mem_mib'] > 1024.0)
    ]
    exclusion_list = bombs_df['file_path'].unique()

    if len(exclusion_list) > 0:
        exclusion_file = Path("bombs_to_exclude.txt")
        exclusion_file.write_text("\n".join(exclusion_list))
        console.print(Panel(f"[bold red]Identified {len(exclusion_list)} bombs / corrupted / frozen files.[/bold red]\n"
                            f"A definitive exclusion list saved to: `{exclusion_file}`\n\n"
                            f"[bold]Next Step:[/bold] You can now safely run `step04_build_crosswalk.py`.",
                            title="[green]Stress Test Complete[/green]"))
    else:
        console.print("[green]No memory bombs or corrupted files were found.[/green]")
        Path("bombs_to_exclude.txt").touch()

if __name__ == "__main__":
    main()