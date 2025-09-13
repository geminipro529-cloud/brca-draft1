# scripts/utility_memory_worker.py
# A disposable, single-purpose worker for the Guardian Stress Test.
# It takes one file path as an argument, measures the memory needed to
# read it, and prints the result to standard output for the guardian to capture.

import sys
from pathlib import Path
import pandas as pd
from memory_profiler import memory_usage

def main():
    if len(sys.argv) != 2:
        # Exit with an error code if the input is wrong
        print("ERROR: No file path provided.")
        sys.exit(1)

    file_path = sys.argv[1]
    
    try:
        # The core memory measurement logic
        mem_usage = memory_usage(
            (pd.read_parquet, (file_path,)),
            interval=0.1,
            max_usage=True,
            retval=False
        )
        peak_mem_mib = float(mem_usage[0])
        # Print the result to stdout in a machine-readable format
        print(f"SUCCESS,{peak_mem_mib}")
        sys.exit(0)
        
    except Exception as e:
        # If any error occurs, print it to stdout and exit
        print(f"ERROR,{type(e).__name__}: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()