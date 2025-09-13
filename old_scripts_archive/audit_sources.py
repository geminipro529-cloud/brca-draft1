#!/usr/bin/env python3
"""
audit_sources.py — verify contribution & consistency of merged dataset

Checks:
1. Row contribution per source file (table + bar chart)
2. Sample ID overlaps between rnaseq_merged_20250117 and rnaseq_all
3. Basic ID format consistency (length & allowed chars)

Run:
  python scripts/audit_sources.py --in data/02_interim/PIPELINE_OUT/master_structured_merged.csv
"""

import argparse
import pandas as pd
import matplotlib.pyplot as plt

def main(path):
    print(f"[INFO] Loading dataset: {path}")
    df = pd.read_csv(path)

    # --- Assumption: provenance column exists or fallback ---
    prov_col = None
    for c in df.columns:
        if "source" in c.lower():
            prov_col = c
            break

    if not prov_col:
        print("[WARNING] No explicit provenance column found — cannot compute contribution by source")
    else:
        # 1. Row contribution
        counts = df[prov_col].value_counts()
        print("\n[ROW CONTRIBUTION BY SOURCE]")
        for src, cnt in counts.items():
            print(f"{src:30s} {cnt:6d} rows ({cnt/len(df)*100:.1f}%)")

        # --- Bar chart ---
        plt.figure(figsize=(10, 6))
        counts.plot(kind="bar")
        plt.ylabel("Row Count")
        plt.title("Row Contribution by Source File")
        plt.xticks(rotation=45, ha="right")
        plt.tight_layout()
        out_png = path.replace(".csv", "_source_contribution.png")
        plt.savefig(out_png, dpi=150)
        print(f"[INFO] Bar chart saved to {out_png}")

    # 2. Overlap check
    if "Sample_ID" in df.columns:
        sample_ids = df["Sample_ID"].astype(str)

        rnaseq_merged = set(sample_ids[sample_ids.str.contains("rnaseq_merged_20250117", case=False, na=False)])
        rnaseq_all = set(sample_ids[sample_ids.str.contains("rnaseq_all", case=False, na=False)])
        overlap = rnaseq_merged.intersection(rnaseq_all)

        print("\n[SAMPLE ID OVERLAP CHECK]")
        print(f"rnaseq_merged_20250117 count: {len(rnaseq_merged)}")
        print(f"rnaseq_all count:            {len(rnaseq_all)}")
        print(f"Overlap count:               {len(overlap)}")
    else:
        print("\n[SAMPLE ID OVERLAP] Skipped (no Sample_ID column)")

    # 3. Basic ID format check
    if "Sample_ID" in df.columns:
        lengths = df["Sample_ID"].dropna().astype(str).map(len)
        print("\n[ID FORMAT CHECK]")
        print(f"Min length: {lengths.min()}, Max length: {lengths.max()}")
        bad_ids = df.loc[~df["Sample_ID"].astype(str).str.match(r"^[A-Za-z0-9_\-\.]+$"), "Sample_ID"]
        if not bad_ids.empty:
            print(f"⚠️ Found {len(bad_ids)} IDs with suspicious characters")
            print(bad_ids.head())
        else:
            print("All IDs alphanumeric with - _ .")
    else:
        print("\n[ID FORMAT CHECK] Skipped (no Sample_ID column)")

    print("\n[INFO] Audit complete.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--in", dest="input_path", required=True, help="Path to master_structured_merged.csv")
    args = parser.parse_args()
    main(args.input_path)
