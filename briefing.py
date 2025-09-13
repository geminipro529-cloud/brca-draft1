import argparse
import ast  # Used to safely parse Python code
from pathlib import Path
import csv
import json

# --- Helper Functions for File Analysis ---

def summarize_csv(file_path):
    """Analyzes a CSV file and returns a summary."""
    try:
        with file_path.open('r', newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            headers = next(reader, [])
            num_rows = sum(1 for row in reader)
            return (f"      - Type: CSV\n"
                    f"      - Rows: {num_rows}\n"
                    f"      - Columns: {len(headers)}\n"
                    f"      - Headers: {', '.join(headers)[:100]}...")
    except Exception as e:
        return f"      - Type: CSV\n      - Error: Could not parse. {e}"

def summarize_json(file_path):
    """Analyzes a JSON file and returns a summary of its top-level keys."""
    try:
        with file_path.open('r', encoding='utf-8') as f:
            data = json.load(f)
            keys = list(data.keys()) if isinstance(data, dict) else []
            return (f"      - Type: JSON\n"
                    f"      - Top-Level Keys ({len(keys)}): {', '.join(keys)[:100]}...")
    except Exception as e:
        return f"      - Type: JSON\n      - Error: Could not parse. {e}"

def analyze_python_script(file_path):
    """Analyzes a Python script to find imports and function definitions."""
    try:
        with file_path.open('r', encoding='utf-8') as f:
            content = f.read()
            tree = ast.parse(content)
            
            imports = sorted({node.names[0].name.split('.')[0] for node in ast.walk(tree) if isinstance(node, ast.Import) or isinstance(node, ast.ImportFrom)})
            functions = sorted([node.name for node in ast.walk(tree) if isinstance(node, ast.FunctionDef)])
            
            summary = "      - Type: Python Script\n"
            if imports:
                summary += f"      - Libraries: {', '.join(imports)}\n"
            if functions:
                summary += f"      - Functions: {', '.join(functions)}"
            return summary if imports or functions else "      - Type: Python Script (No imports or functions found)"
            
    except Exception as e:
        return f"      - Type: Python Script\n      - Error: Could not parse. {e}"

# --- Main Scanning Logic ---

FILE_HANDLERS = {
    '.csv': summarize_csv,
    '.json': summarize_json,
    '.py': analyze_python_script,
}

def generate_project_tree(root_path, prefix="", output_lines=None):
    """Recursively scans a directory and builds a formatted report."""
    if output_lines is None:
        output_lines = []
    
    items = sorted([item for item in root_path.iterdir() if not item.name.startswith('.')])
    
    for i, path in enumerate(items):
        connector = "└── " if i == len(items) - 1 else "├── "
        output_lines.append(f"{prefix}{connector}{path.name}")
        
        if path.is_dir():
            new_prefix = prefix + ("    " if i == len(items) - 1 else "│   ")
            generate_project_tree(path, prefix=new_prefix, output_lines=output_lines)
        elif path.is_file():
            handler = FILE_HANDLERS.get(path.suffix.lower())
            if handler:
                summary = handler(path)
                summary_prefix = prefix + ("    " if i == len(items) - 1 else "│   ")
                output_lines.append(f"{summary_prefix}{summary}")
                
    return output_lines

# --- Script Execution ---

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate a comprehensive briefing report for a project directory."
    )
    parser.add_argument(
        "directory",
        help="The path to the project's root directory."
    )
    args = parser.parse_args()
    
    root_directory = Path(args.directory)
    
    print("="*70)
    print(f"🚀 Generating Project Briefing for: {root_directory.resolve()}")
    print("="*70)
    
    if not root_directory.is_dir():
        print(f"Error: The path '{root_directory}' is not a valid directory.")
    else:
        report_lines = [f"📁 {root_directory.name}"]
        generate_project_tree(root_directory, output_lines=report_lines)
        
        print("\n".join(report_lines))
            
    print("\n" + "="*70)
    print("📋 Mission Briefing Complete. Copy all the text above and paste it into our chat.")
    print("="*70)
