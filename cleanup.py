# cleanup.py
import argparse, shutil
from rich.console import Console
import compass_brca.pipeline_config as cfg

console = Console()
DIRS = {
    'intermediate': [cfg.INTERIM_DATA_DIR, cfg.PRIMARY_DATA_DIR],
    'features': [cfg.FINAL_FEATURES_DIR, cfg.BALANCED_FEATURES_DIR],
    'reports': [cfg.REPORTS_DIR], 'logs': [cfg.LOGS_DIR],
    'all': [cfg.INTERIM_DATA_DIR, cfg.PRIMARY_DATA_DIR, cfg.FINAL_FEATURES_DIR, 
            cfg.BALANCED_FEATURES_DIR, cfg.REPORTS_DIR, cfg.LOGS_DIR]
}

def main():
    parser = argparse.ArgumentParser(description="Clean up pipeline directories.")
    parser.add_argument("level", choices=DIRS.keys(), help="Data level to clean.")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be deleted without action.")
    args = parser.parse_args()

    dirs_to_clean = DIRS[args.level]
    console.print(f"Cleanup Mode: [bold]{args.level}[/], Dry Run: [bold]{args.dry_run}[/]")

    if not args.dry_run:
        console.print("[bold red]This is a destructive operation![/]")
        if input("Type 'YES' to confirm deletion: ") != 'YES':
            console.print("[yellow]Aborted by user.[/]")
            return

    for d in dirs_to_clean:
        if d.exists():
            if args.dry_run:
                console.print(f"[yellow][DRY RUN] Would remove directory: {d}[/]")
            else:
                console.print(f"Removing: {d}")
                shutil.rmtree(d)
        elif args.dry_run:
             console.print(f"[cyan][DRY RUN] Directory not found (already clean): {d}[/]")

if __name__ == "__main__":
    main()