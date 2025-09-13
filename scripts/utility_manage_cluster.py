# scripts/utility_manage_cluster.py
# Purpose: A simple control panel to start and stop the standalone
# Dask cluster using background processes, avoiding multiple terminals.

import subprocess
import os
import signal
from pathlib import Path
import argparse
from rich.console import Console
from rich.panel import Panel

console = Console()
PROJECT_ROOT = Path(__file__).resolve().parents[1]

# --- Define paths to the pid files for managing processes ---
PID_DIR = PROJECT_ROOT / ".pids"
SCHEDULER_PID_FILE = PID_DIR / "scheduler.pid"
WORKER_PID_FILE = PID_DIR / "worker.pid"

def start_cluster():
    """Starts the Dask scheduler and workers in new background windows."""
    console.rule("[bold green]Starting Local Dask Cluster[/bold green]")
    PID_DIR.mkdir(exist_ok=True)
    
    # Check if a PID file exists, suggesting the cluster is already running
    if SCHEDULER_PID_FILE.exists() or WORKER_PID_FILE.exists():
        console.print("[yellow]Warning: PID files exist. Cluster may already be running.[/]")
        console.print("Run 'python scripts/utility_manage_cluster.py stop' to terminate existing processes.")
        return

    # Use CREATE_NEW_CONSOLE to launch them in separate, backgrounded windows.
    # The `Popen` object gives us the process ID (pid).
    console.print("Starting scheduler in a new window...")
    scheduler_process = subprocess.Popen(
        ["start", "cmd", "/c", "start-scheduler.bat"], 
        shell=True, creationflags=subprocess.CREATE_NEW_CONSOLE
    )
    # We don't have the child PID directly this way, so we will manage by window title later
    # For simplicity, we'll assume manual closing or use `stop` command.
    
    console.print("Starting workers in a new window...")
    worker_process = subprocess.Popen(
        ["start", "cmd", "/c", "start-workers.bat"], 
        shell=True, creationflags=subprocess.CREATE_NEW_CONSOLE
    )
    
    console.print("\n[bold green]Cluster processes launched in background windows.[/bold green]")
    console.print("You can minimize these windows. They must remain open while step04 runs.")
    console.print("When you are finished, run [cyan]python scripts/utility_manage_cluster.py stop[/]")

def stop_cluster():
    """Stops the Dask scheduler and workers by finding their process IDs."""
    console.rule("[bold red]Stopping Local Dask Cluster[/bold red]")
    
    # Use taskkill to find and terminate the processes by the window title
    # that the .bat files implicitly create.
    console.print("Attempting to close Dask scheduler and worker windows...")
    # These titles are defaults for cmd running a .bat file.
    scheduler_killed = os.system('taskkill /f /fi "WINDOWTITLE eq start-scheduler.bat*"')
    worker_killed = os.system('taskkill /f /fi "WINDOWTITLE eq start-workers.bat*"')
    
    if scheduler_killed == 0 or worker_killed == 0:
        console.print("[bold green]Successfully sent termination signal to cluster processes.[/bold green]")
    else:
        console.print("[yellow]Could not find running cluster processes to stop.[/yellow]")
        console.print("You may need to close the 'start-scheduler.bat' and 'start-workers.bat' command windows manually.")

def main():
    """Main entry point for the cluster management utility."""
    parser = argparse.ArgumentParser(description="Manage the local standalone Dask cluster.")
    parser.add_argument("action", choices=["start", "stop"], help="The action to perform.")
    args = parser.parse_args()
    
    if args.action == "start":
        start_cluster()
    elif args.action == "stop":
        stop_cluster()

if __name__ == "__main__":
    main()