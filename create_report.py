import os
import json
import hashlib
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- Configuration ---
# Files larger than this (in bytes) will have less info gathered to save time.
# 10 MB = 10 * 1024 * 1024 bytes
LARGE_FILE_THRESHOLD = 10485760 

# Number of concurrent workers to process files. Adjust based on your system's cores.
MAX_WORKERS = 10

# Directories to ignore. Add project-specific folders like '.git', 'venv', etc.
IGNORE_DIRS = {'.git', '.vscode', '__pycache__', 'venv', 'node_modules', '.idea'}

def get_file_hash(file_path):
    """Calculates the SHA-256 hash for a file, reading it in chunks."""
    h = hashlib.sha256()
    try:
        with open(file_path, 'rb') as f:
            while chunk := f.read(8192):
                h.update(chunk)
        return h.hexdigest()
    except (IOError, PermissionError):
        return None

def process_file(file_path):
    """
    Gathers information about a single file.
    For large files, it skips the time-consuming hash calculation.
    """
    try:
        stat = os.stat(file_path)
        file_size = stat.st_size
        
        file_info = {
            "path": file_path,
            "size_bytes": file_size,
            "modified_timestamp": int(stat.st_mtime)
        }

        # Only calculate hash for smaller files for efficiency
        if file_size < LARGE_FILE_THRESHOLD:
            file_info["sha256_hash"] = get_file_hash(file_path)
        else:
            file_info["note"] = "large_file_hash_skipped"
            
        return file_info
    except (FileNotFoundError, PermissionError) as e:
        print(f"Skipping file {file_path}: {e}")
        return None

def main():
    """
    Main function to walk the directory, process files concurrently, and print a JSON report.
    """
    project_root = os.getcwd()
    print(f"🔍 Starting scan of '{project_root}' with {MAX_WORKERS} workers...")
    
    all_files = []
    # Recursively find all files, respecting IGNORE_DIRS
    for root, dirs, files in os.walk(project_root, topdown=True):
        dirs[:] = [d for d in dirs if d not in IGNORE_DIRS]
        for name in files:
            full_path = os.path.join(root, name)
            # Make path relative to the root for a cleaner report
            relative_path = os.path.relpath(full_path, project_root)
            all_files.append(relative_path)

    file_reports = []
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all files to the thread pool
        future_to_file = {executor.submit(process_file, path): path for path in all_files}
        
        # Process results as they complete
        for i, future in enumerate(as_completed(future_to_file)):
            result = future.result()
            if result:
                file_reports.append(result)
            # Simple progress indicator
            print(f"\rProcessed {i + 1}/{len(all_files)} files...", end="")

    print("\n✅ Scan complete. Generating report...")
    
    # Create the final report object
    final_report = {
        "report_generated_utc": int(time.time()),
        "project_root": os.path.basename(project_root),
        "total_files": len(file_reports),
        "files": sorted(file_reports, key=lambda x: x['path']) # Sort for consistent output
    }
    
    # Generate the most compact JSON possible
    # separators=(',', ':') removes all non-essential whitespace.
    json_output = json.dumps(final_report, indent=None, separators=(',', ':'))
    
    # Save the report to a file
    output_filename = "project_structure_report.json"
    with open(output_filename, 'w') as f:
        f.write(json_output)
        
    print(f"\n✨ Report saved to '{output_filename}' ({len(json_output) / 1024:.2f} KB)")

if __name__ == "__main__":
    main()