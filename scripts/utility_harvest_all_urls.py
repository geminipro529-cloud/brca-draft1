# scripts/utility_harvest_all_urls.py
import re
from pathlib import Path
import sys

try:
    project_root = Path(__file__).resolve().parents[1]
    sys.path.append(str(project_root / "src"))
    from compass_brca.pipeline_config import RAW_DATA_DIR, MANIFESTS_DIR
except ImportError:
    sys.exit("Fatal Error: Could not import pipeline_config. Ensure src/compass_brca/pipeline_config.py exists.")

from rich.console import Console
from rich.progress import track

console = Console()
URL_REGEX = r'https?://(?:[-\w.]|(?:%[\da-fA-F]{2}))+[/\w\-.?%=&]*'
OUTPUT_FILE = MANIFESTS_DIR / "harvested_urls.txt"
TEXT_EXTENSIONS = {'.json', '.jsonl', '.csv', '.tsv', '.txt', '.xml', '.html'}

def main():
    console.rule("[bold cyan]URL Harvester Start[/]")
    if not RAW_DATA_DIR.exists():
        console.print(f"[bold red]Error: Raw data directory not found at {RAW_DATA_DIR}[/]"); sys.exit(1)
    MANIFESTS_DIR.mkdir(exist_ok=True)
    found_urls = set()
    files_to_scan = [p for p in RAW_DATA_DIR.rglob("*") if p.is_file() and p.suffix.lower() in TEXT_EXTENSIONS]
    if not files_to_scan:
        console.print("[yellow]Warning: No text-based files found to scan.[/]"); OUTPUT_FILE.touch(); return
    console.print(f"Found {len(files_to_scan)} files to scan.")
    for file_path in track(files_to_scan, description="Harvesting URLs..."):
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
                found_urls.update(re.findall(URL_REGEX, content))
        except Exception as e:
            console.log(f"[yellow]Warning: Could not read file {file_path.name}. Reason: {e}[/]")
    console.print(f"\n[bold]Scan Complete.[/] Found [cyan]{len(found_urls)}[/] unique URLs.")
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        for url in sorted(list(found_urls)):
            f.write(url + "\n")
    console.print(f"  - Results saved to [green]{OUTPUT_FILE}[/]")
    console.rule("[bold green]URL Harvester Finished[/]")

if __name__ == "__main__":
    main()