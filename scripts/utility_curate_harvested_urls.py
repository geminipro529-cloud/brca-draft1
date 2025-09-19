# scripts/utility_curate_harvested_urls.py
# V2 - "Spear Fishing" Edition
import sys
from pathlib import Path

try:
    project_root = Path(__file__).resolve().parents[1]
    sys.path.append(str(project_root / "src"))
    from compass_brca.pipeline_config import MANIFESTS_DIR
except ImportError:
    sys.exit("Fatal Error: Could not import pipeline_config.")

from rich.console import Console
console = Console()

# --- EXPERT SYSTEM CONFIGURATION V2 ---
PHYSICAL_PROPERTY_KEYWORDS = {
    'size_nm': 30, 'zeta_potential': 30, 'surface_chemistry': 25, 'core_material': 25,
    'drug_load': 20, 'payload': 20, 'encapsulation': 20, 'particle': 15,
}
POSITIVE_KEYWORDS = {
    '.csv': 5, '.tsv': 5, '.json': 5, '.parquet': 8, '.gz': 3, '.zip': 3, 'ncbi': 10, 'geo': 10,
    'tcga': 10, 'metabric': 10, 'zenodo': 8, 'figshare': 8, 'brca': 8, 'breast-cancer': 8,
    'cancer': 3, 'tumor': 3, 'nanoparticle': 10, 'nanomedicine': 10, 'drug-delivery': 5,
    'gene': 5, 'expression': 5, 'dataset': 8, 'download': 5,
}
POSITIVE_KEYWORDS.update(PHYSICAL_PROPERTY_KEYWORDS)
NEGATIVE_KEYWORDS = {
    'google.com': -10, 'github.com': -5, 'doi.org': -5, 'about': -8, 'contact': -8, 'privacy': -15,
    'terms': -15, 'policy': -15, 'login': -10, 'search': -8, 'help': -8, 'documentation': -8, 'html': -3,
}
RELEVANCE_THRESHOLD = 15
# --- END CONFIGURATION ---

INPUT_FILE = MANIFESTS_DIR / "harvested_urls.txt"
OUTPUT_FILE = MANIFESTS_DIR / "curated_urls.txt"

def calculate_relevance_score(url: str) -> int:
    score = 0
    url_lower = url.lower()
    for keyword, value in POSITIVE_KEYWORDS.items():
        if keyword in url_lower: score += value
    for keyword, value in NEGATIVE_KEYWORDS.items():
        if keyword in url_lower: score += value
    return score

def main():
    console.rule("[bold cyan]URL Curator Start (Relevance Engine V2)[/]")
    if not INPUT_FILE.exists():
        console.print(f"[bold red]Error: Input file not found: {INPUT_FILE}[/]"); sys.exit(1)
    with open(INPUT_FILE, 'r', encoding='utf-8') as f:
        raw_urls = [line.strip() for line in f if line.strip()]
    if not raw_urls:
        console.print("[yellow]Warning: No URLs found to curate.[/]"); OUTPUT_FILE.touch(); return
    console.print(f"Read {len(raw_urls):,} raw URLs. Scoring with V2 relevance engine...")
    curated_urls = [(url, calculate_relevance_score(url)) for url in raw_urls]
    curated_urls = [item for item in curated_urls if item[1] >= RELEVANCE_THRESHOLD]
    curated_urls.sort(key=lambda x: x[1], reverse=True)
    console.print(f"\n[bold]Curation Complete.[/] Kept [green]{len(curated_urls):,}[/] URLs.")
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        for url, score in curated_urls:
            f.write(url + "\n")
    console.print(f"  - Results saved to [green]{OUTPUT_FILE}[/]")
    console.rule("[bold green]URL Curator Finished[/]")

if __name__ == "__main__":
    main()