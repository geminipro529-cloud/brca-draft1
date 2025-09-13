# ==============================================================================
# SCRIPT: create_structure_map.py
#
# PURPOSE:
#   Generates a flawless, comprehensive, and ANNOTATED map of the project's
#   file structure. It analyzes key files to provide contextual details.
#
# VERSION: 3.0.0 -- ANNOTATED PERFECTION
# ==============================================================================

import os
import fnmatch
import ast
import csv
import yaml
from datetime import datetime
from pathlib import Path

# --- CONFIGURATION ---
OUTPUT_FILE: str = "project_structure_annotated.txt"
MAX_DEPTH: int = 10
IGNORE_DIRS: set[str] = {".git", "node_modules", ".venv", "__pycache__", "dist", "build", "target", ".vscode", ".idea", "logs"}
IGNORE_FILES: list[str] = ["*.pyc", "*.log", ".DS_Store"]
# Files to analyze for extra details
ANNOTATE_EXTENSIONS: set[str] = {".py", ".yaml", ".csv", ".tsv", ".json"}


def get_file_details(file_path: Path) -> str:
    """Analyzes a file and returns a formatted string of its key details."""
    details = []
    try:
        # Generic details for all annotated files
        size_kb = file_path.stat().st_size / 1024
        mod_time = datetime.fromtimestamp(file_path.stat().st_mtime).strftime('%Y-%m-%d')
        details.append(f"Size: {size_kb:.2f} KB | Modified: {mod_time}")

        # Specific details based on file type
        if file_path.suffix == '.py':
            with file_path.open('r', encoding='utf-8', errors='ignore') as f:
                tree = ast.parse(f.read())
                docstring = ast.get_docstring(tree)
                if docstring:
                    purpose = docstring.strip().split('\n')[0]
                    details.append(f'Purpose: "{purpose}"')

        elif file_path.suffix in ['.csv', '.tsv']:
            with file_path.open('r', encoding='utf-8', errors='ignore') as f:
                delimiter = '\t' if file_path.suffix == '.tsv' else ','
                reader = csv.reader(f, delimiter=delimiter)
                header = next(reader, None)
                if header:
                    preview = ", ".join(header[:4])
                    details.append(f"Header: [{preview}{', ...' if len(header) > 4 else ''}]")

        elif file_path.suffix == '.yaml':
            with file_path.open('r', encoding='utf-8', errors='ignore') as f:
                data = yaml.safe_load(f)
                if isinstance(data, dict):
                    keys = list(data.keys())
                    details.append(f"Top-Level Keys: {keys}")

        return " ➞ " + " | ".join(details)
    except Exception:
        return "" # Fail silently if a file is unreadable or malformed

def create_structure_map() -> bool:
    """Traverses the directory tree and generates the annotated structure map."""
    try:
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            header = [
                "Project Structure Overview (Annotated)",
                "--------------------------------------",
                f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                f"Max Depth: {'Unlimited' if MAX_DEPTH >= 999 else MAX_DEPTH}",
                f"Ignored Directories: {', '.join(sorted(IGNORE_DIRS))}",
                "==================================================\n",
                ".\n"
            ]
            f.write("\n".join(header))

            for root, dirs, files in os.walk(".", topdown=True):
                # Use Path to count levels for cross-platform safety
                level = len(Path(root).relative_to(".").parts)
                if root != '.' and level >= MAX_DEPTH:
                    dirs[:] = []
                    continue

                dirs[:] = [d for d in sorted(dirs) if d not in IGNORE_DIRS]
                files = sorted([f for f in files if not any(fnmatch.fnmatch(f, p) for p in IGNORE_FILES)])

                items = [d + '/' for d in dirs] + files
                indent = '│   ' * level
                
                for i, item_name in enumerate(items):
                    is_last = (i == len(items) - 1)
                    prefix = '└── ' if is_last else '├── '
                    
                    is_file = not item_name.endswith('/')
                    details_str = ""
                    if is_file:
                        full_path = Path(root) / item_name
                        if full_path.suffix in ANNOTATE_EXTENSIONS:
                            details_str = get_file_details(full_path)

                    f.write(f"{indent}{prefix}{item_name}{details_str}\n")
    except IOError as e:
        print(f"CRITICAL FAILURE: Could not write to file '{OUTPUT_FILE}'. Error: {e}")
        return False
    return True


if __name__ == "__main__":
    print("Operation starting. Generating a flawless, annotated structure map...")
    if create_structure_map():
        print(f"SUCCESS: Reconnaissance complete. Map created at '{OUTPUT_FILE}'.")
    else:
        pass