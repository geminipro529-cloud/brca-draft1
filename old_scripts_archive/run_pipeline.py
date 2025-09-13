# File: scripts/run_pipeline.py
# Orchestrates: convert(big) → diagnose → link → annotate (≤2 passes, early stop) → build
import os, sys, subprocess, time, json, argparse

PY = [sys.executable]
SCRIPTS = "scripts"

BIG_FILES = [
    # Add/adjust here as needed
    (r"data\01_raw\STRUCTURED\rnaseq_all_20250117\rnaseq_all_20250117.csv",
     r"data\01_raw\STRUCTURED\parquet",
     ["model_id","gene_symbol","ensembl_gene_id","rsem_tpm","rsem_fpkm","htseq_fpkm"]),
    (r"data\01_raw\STRUCTURED\rnaseq_merged_20250117\rnaseq_merged_20250117.csv",
     r"data\01_raw\STRUCTURED\parquet",
     ["model_id","gene_symbol","ensembl_gene_id","rsem_tpm","rsem_fpkm","htseq_fpkm"]),
    (r"data\01_raw\VOCAB\GDSC2_public_raw_data_27Oct23.csv",
     r"data\01_raw\VOCAB\parquet",
     ["SANGER_MODEL_ID","DRUG_ID","DRUG_NAME","AUC","IC50"]),
    (r"data\01_raw\VOCAB\vocabulary_download_v5\CONCEPT_RELATIONSHIP.csv",
     r"data\01_raw\VOCAB\parquet",
     ["concept_id_1","concept_id_2","relationship_id"]),
    (r"data\01_raw\VOCAB\vocabulary_download_v5\CONCEPT_ANCESTOR.csv",
     r"data\01_raw\VOCAB\parquet",
     ["ancestor_concept_id","descendant_concept_id","min_levels_of_separation"]),
]

def run(cmd):
    print(">>", " ".join(cmd))
    try: subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e: sys.exit(e.returncode)

def changed_since(src, manifest):
    try: return os.path.getmtime(src) > os.path.getmtime(manifest)
    except OSError: return True  # no manifest → needs (re)run

def convert_whales():
    conv = os.path.join(SCRIPTS, "convert_big_csvs_to_parquet.py")
    for src, out_dir, cols in BIG_FILES:
        if not os.path.exists(src): 
            print("skip (missing):", src); continue
        base = os.path.splitext(os.path.basename(src))[0]
        manifest = os.path.join(out_dir, base, "_manifest.json")
        if not os.path.exists(manifest) or changed_since(src, manifest):
            cmd = PY + [conv, "--in", src, "--out", out_dir, "--chunksize", "15000"]
            if cols: cmd += ["--columns"] + cols
            run(cmd)
        else:
            print("ok (up-to-date):", src)

def diagnose():
    run(PY + [os.path.join(SCRIPTS,"diagnose_repository_fast.py"),
              "--root","data","--out", r"data\99_reports\fast_diag"])

def link_ids():
    run(PY + [os.path.join(SCRIPTS,"link_patients_across_cohorts_resumable.py"),
              "--in", r"data\02_interim\structured_annotated_v2",
              "--out", r"data\02_interim\id_map",
              "--checkpoint", r"data\02_interim\_link_state",
              "--chunksize","100000"])

def annotate_then_maybe_once_more():
    ann = os.path.join(SCRIPTS,"annotate_structured_with_vocab.py")
    if not os.path.exists(ann):
        print("note: annotate_structured_with_vocab.py not found — skipping"); return
    vocab = r"data\02_interim\VOCAB_COMPILED\vocabulary_enriched.parquet"
    prev_rows = None
    for i in (1,2):
        out = rf"data\02_interim\structured_annotated_pass{i}"
        cmd = PY + [ann, "--in", r"data\02_interim\structured_annotated_v2",
                    "--vocab", vocab, "--out", out]
        run(cmd)
        # detect gain and stop early if <2%
        rows = _count_rows(os.path.join(out, "annotated.parquet"),
                           os.path.join(out, "annotated.csv"))
        if prev_rows is not None and rows is not None:
            gain = (max(rows - prev_rows, 0) / max(prev_rows,1)) * 100
            print(f"pass {i}: gain ~{gain:.2f}%")
            if gain < 2.0:
                print("enrichment <2% — stopping"); break
        prev_rows = rows

def _count_rows(parq, csv):
    try:
        if os.path.exists(parq):
            import pandas as pd; return len(pd.read_parquet(parq))
        if os.path.exists(csv):
            with open(csv,"r",encoding="utf-8",errors="ignore") as f:
                return max(sum(1 for _ in f)-1, 0)
    except Exception:
        return None
    return None

def build_master(breast_only=False):
    build = os.path.join(SCRIPTS,"build_master_dataset.py")
    if not os.path.exists(build):
        print("note: build_master_dataset.py not found — skipping"); return
    cmd = PY + [build, "--out", r"data\03_primary\MASTER"]
    if breast_only: cmd += ["--breast-only"]
    run(cmd)

def main():
    ap = argparse.ArgumentParser(description="One-touch pipeline (incremental, low-RAM)")
    ap.add_argument("--breast-only", action="store_true")
    args = ap.parse_args()
    convert_whales()
    diagnose()
    link_ids()
    annotate_then_maybe_once_more()
    build_master(args.breast_only)

if __name__ == "__main__":
    main()
