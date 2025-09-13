#!/usr/bin/env python
# scripts/scan_project_manifest.py
# Scans the project root and outputs CSV, Markdown, and optional tree-view.

from __future__ import annotations
import os
from pathlib import Path
import argparse
import pandas as pd
from rich.console import Console
from rich.progress import track

console = Console()

# ---------------- Purpose inference rules ----------------

def infer_purpose(path: Path) -> str:
    """Infer purpose of file/folder based on name/extension."""
    name = path.name.lower()

    if path.is_dir():
        if "raw" in name:
            return "Raw input data (unprocessed)"
        if "interim" in name:
            return "Intermediate artifacts"
        if "processed" in name:
            return "Final cleaned datasets"
        if "scripts" in name:
            return "Project scripts"
        if "notebook" in name:
            return "Exploratory analysis notebooks"
        if "zipped" in name:
            return "Archived compressed files"
        return "Directory"

    if name.endswith(".csv"):
        return "Comma-separated dataset"
    if name.endswith(".tsv"):
        return "Tab-separated dataset"
    if name.endswith(".parquet"):
        return "Parquet dataset (optimized for analytics)"
    if name.endswith(".db") or name.endswith(".sqlite"):
        return "SQLite database artifact"
    if name.endswith(".py"):
        return "Python script"
    if name.endswith(".ipynb"):
        return "Jupyter notebook"
    if name.endswith(".yaml") or name.endswith(".yml"):
        return "Configuration file"
    if name.endswith(".txt"):
        return "Text file (metadata or logs)"
    if name.endswith(".gz") or name.endswith(".zip"):
        return "Compressed data file"
    if name.endswith(".pdf"):
        return "Journal or report (qualitative source)"
    if name.endswith(".md"):
        return "Markdown documentation/report"

    return "Other file type"


# ---------------- Tree view printer ----------------

def print_tree(root: Path, prefix: str = ""):
    """Recursively print a tree view of the project directory."""
    entries = sorted(root.iterdir(), key=lambda x: (not x.is_dir(), x.name.lower()))
    for i, entry in enumerate(entries):
        connector = "└── " if i == len(entries) - 1 else "├── "
        console.print(f"{prefix}{connector}{entry.name}")
        if entry.is_dir():
            extension = "    " if i == len(entries) - 1 else "│   "
            print_tree(entry, prefix + extension)


# ---------------- Main ----------------

def scan_root(root: Path, out_csv: Path, out_md: Path):
    rows = []
    for dirpath, dirnames, filenames in track(os.walk(root), description="Scanning project root"):
        for f in filenames:
            p = Path(dirpath) / f
            purpose = infer_purpose(p)
            relpath = p.relative_to(root)
            rows.append({
                "path": str(relpath),
                "name": p.name,
                "type": "dir" if p.is_dir() else "file",
                "purpose": purpose,
                "size_kb": round(p.stat().st_size / 1024, 2) if p.is_file() else None,
            })

    df = pd.DataFrame(rows).sort_values("path").reset_index(drop=True)
    df.to_csv(out_csv, index=False, encoding="utf-8")

    # Write Markdown manifest
    lines = ["# Project Manifest", ""]
    for _, row in df.iterrows():
        lines.append(f"- `{row['path']}` → {row['purpose']} ({row['size_kb']} KB)")

    out_md.write_text("\n".join(lines), encoding="utf-8")

    console.log(f"[green]CSV manifest:[/green] {out_csv}")
    console.log(f"[green]Markdown manifest:[/green] {out_md}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Scan project root for directories, files, and purposes.")
    ap.add_argument("--tree-view", action="store_true", help="Print an ASCII tree view of the project structure.")
    args = ap.parse_args()

    root = Path(__file__).resolve().parents[1]
    out_dir = root / "data/00_admin"
    out_dir.mkdir(parents=True, exist_ok=True)

    out_csv = out_dir / "project_manifest.csv"
    out_md = out_dir / "project_manifest.md"

    scan_root(root, out_csv, out_md)

    if args.tree_view:
        console.rule("[bold cyan]Project Directory Tree")
        print_tree(root)
        console.rule("[bold green]Tree view complete")

    console.rule("[bold green]Done scanning project")
