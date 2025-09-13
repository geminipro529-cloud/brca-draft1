# File: scripts/rebuild_all.py
# One-touch clean rebuild: convert(big) → diagnose → link → annotate (pass1) → merge domain terms → annotate (pass2) → build
import os, sys, shutil, subprocess, argparse, time

PY = [sys.executable]
S = "scripts"

BIG = [
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
    subprocess.run(cmd, check=True)

def rmrf(p):
    if os.path.isdir(p): shutil.rmtree(p, ignore_errors=True)
    elif os.path.isfile(p): 
        try: os.remove(p)
        except OSError: pass

def main():
    ap = argparse.ArgumentParser(description="Full rebuild (RAM-safe, resumable).")
    ap.add_argument("--breast-only", action="store_true")
    ap.add_argument("--reconvert", action="store_true", help="Force reconvert even if manifests exist.")
    ap.add_argument("--chunksize", type=int, default=15000, help="Converter chunksize (low RAM).")
    ap.add_argument("--keep-parquet", action="store_true", help="Keep previous parquet outputs on clean.")
    args = ap.parse_args()

    # 0) Clean previous stage outputs (idempotent, safe)
    out_dirs = [
        r"data\99_reports\fast_diag",
        r"data\02_interim\id_map",
        r"data\02_interim\_link_state",
        r"data\02_interim\structured_annotated_pass1",
        r"data\02_interim\structured_annotated_pass2",
        r"data\03_primary\MASTER",
    ]
    for d in out_dirs: rmrf(d)
    if not args.keep_parquet and args.reconvert:
        for d in [r"data\01_raw\STRUCTURED\parquet", r"data\01_raw\VOCAB\parquet"]:
            rmrf(d)

    # 1) Convert BIG files (progress + resume + fuzzy columns)
    conv = os.path.join(S, "convert_big_csvs_to_parquet.py")
    for src, out_dir, cols in BIG:
        if not os.path.exists(src): 
            print("skip (missing):", src); continue
        cmd = PY + [conv, "--in", src, "--out", out_dir, "--chunksize", str(args.chunksize)]
        if cols: cmd += ["--columns"] + cols
        if args.reconvert:
            # delete manifest to force a fresh run
            base = os.path.splitext(os.path.basename(src))[0]
            rmrf(os.path.join(out_dir, base, "_manifest.json"))
        run(cmd)

    # 2) Quick diagnose
    run(PY + [os.path.join(S,"diagnose_repository_fast.py"), "--root","data", "--out", r"data\99_reports\fast_diag"])

    # 3) Link IDs (resumable)
    run(PY + [os.path.join(S,"link_patients_across_cohorts_resumable.py"),
              "--in", r"data\02_interim\structured_annotated_v2",
              "--out", r"data\02_interim\id_map",
              "--checkpoint", r"data\02_interim\_link_state",
              "--chunksize","100000"])

    # 4) Annotate pass 1 (base vocab)
    ann = os.path.join(S,"annotate_structured_with_vocab.py")
    vocab_base = r"data\02_interim\VOCAB_COMPILED\vocabulary_enriched.parquet"
    out1 = r"data\02_interim\structured_annotated_pass1"
    if os.path.exists(ann):
        run(PY + [ann, "--in", r"data\02_interim\structured_annotated_v2",
                  "--vocab", vocab_base, "--out", out1])
    else:
        print("note: annotate_structured_with_vocab.py not found — skipping pass1")

    # 5) Merge breast-cancer + nanomedicine terms into vocab (writes *_plus.parquet)
    merge = os.path.join(S,"merge_domain_terms_into_vocab.py")
    vocab_plus = r"data\02_interim\VOCAB_COMPILED\vocabulary_enriched_plus.parquet"
    run(PY + [merge, "--base", vocab_base, "--out", vocab_plus, "--seed-defaults"])

    # 6) Annotate pass 2 (enriched vocab)
    out2 = r"data\02_interim\structured_annotated_pass2"
    if os.path.exists(ann):
        run(PY + [ann, "--in", r"data\02_interim\structured_annotated_v2",
                  "--vocab", vocab_plus, "--out", out2])
    else:
        print("note: annotate_structured_with_vocab.py not found — skipping pass2")

    # 7) Build master
    build = os.path.join(S,"build_master_dataset.py")
    if os.path.exists(build):
        cmd = PY + [build, "--out", r"data\03_primary\MASTER"]
        if args.breast_only: cmd += ["--breast-only"]
        run(cmd)
    else:
        print("note: build_master_dataset.py not found — skipping build")

    print("OK: rebuild complete.")

if __name__ == "__main__":
    try: main()
    except subprocess.CalledProcessError as e:
        sys.exit(e.returncode)
    except KeyboardInterrupt:
        print("Aborted by user."); sys.exit(1)
