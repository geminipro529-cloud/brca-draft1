#!/usr/bin/env python3
# scripts/clean_or_merge_raw.py
# - Remove empty dirs
# - Merge legacy folders into RAW_ROOT/<SCOPE>/<MODALITY> (idempotent, dedupe)
from __future__ import annotations
import os, sys, re, argparse, shutil, hashlib, time
from pathlib import Path
from typing import Tuple

try:
    from rich.console import Console
    from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn
except ImportError:
    print("[ERROR] Missing 'rich'. Install: pip install rich", file=sys.stderr); sys.exit(1)

LOG = Path("logs") / "log.txt"; LOG.parent.mkdir(parents=True, exist_ok=True)
def log(msg: str):
    with LOG.open("a", encoding="utf-8") as fh:
        fh.write(time.strftime("[%Y-%m-%d %H:%M:%S] ") + msg + "\n")

RE_BREAST = re.compile(r"\b(brca|breast|metabric|tcga[-_]?brca|cptac[-_]?brca)\b", re.I)
MODALITY_RULES = [
    ("TRANSCRIPTOMICS", re.compile(r"\b(rnaseq|rna[-_ ]seq|transcript|counts|htseq|fpkm|tpm|gene[-_ ]exp)\b", re.I)),
    ("GENOMICS",        re.compile(r"\b(mutation|maf|vcf|genome|cnv|cna|exome|germline|somatic)\b", re.I)),
    ("PROTEOMICS",      re.compile(r"\b(proteom|cptac|pdc|mass[-_ ]spec|ms[-_ ]data)\b", re.I)),
    ("SINGLECELL",      re.compile(r"\b(single[-_ ]?cell|scRNA|10x|cellranger|sce|seurat)\b", re.I)),
    ("IMAGING",         re.compile(r"\b(dicom|.dcm|radiology|pathology[-_ ]slide|ws[i|s])\b", re.I)),
    ("METHYLATION",     re.compile(r"\b(methyl|450k|850k|bisulfite|epigen)\b", re.I)),
    ("EV",              re.compile(r"\b(exosome|vesicle|exocarta|vesiclepedia)\b", re.I)),
    ("METABOLOMICS",    re.compile(r"\b(metabolom|metabolite|workbench)\b", re.I)),
    ("PHARMACO",        re.compile(r"\b(depmap|gdsc|drug|dose|ic50|auc|cellminer)\b", re.I)),
    ("CLINICAL",        re.compile(r"\b(clinical|phenotype|metadata|patient|survival|follow[-_ ]up)\b", re.I)),
    ("NANOMEDICINE",    re.compile(r"\b(nano|liposome|nanoparticle|micelle|cananolab|euon)\b", re.I)),
]
EXT_MODALITY = {
    ".fastq": "TRANSCRIPTOMICS", ".fq": "TRANSCRIPTOMICS", ".bam": "TRANSCRIPTOMICS",
    ".vcf": "GENOMICS", ".maf": "GENOMICS",
    ".mzML": "PROTEOMICS", ".mzml": "PROTEOMICS", ".raw": "PROTEOMICS",
    ".svs": "IMAGING", ".dcm": "IMAGING",
    ".h5ad": "SINGLECELL", ".loom": "SINGLECELL",
    ".gct": "TRANSCRIPTOMICS", ".rds": "SINGLECELL",
    ".parquet": "CLINICAL", ".csv": "CLINICAL", ".tsv": "CLINICAL", ".xlsx": "CLINICAL", ".xls": "CLINICAL",
    ".txt": "CLINICAL", ".json": "CLINICAL",
}

def sha1_of(p: Path, limit: int = 4_194_304) -> str:
    import hashlib
    h = hashlib.sha1()
    with p.open("rb") as fh:
        while True:
            b = fh.read(limit)
            if not b: break
            h.update(b)
            if limit and fh.tell() >= limit: break
    return h.hexdigest()

def classify(p: Path) -> Tuple[str,str]:
    joined = " ".join([s.lower() for s in p.parts])
    scope = "BRCA" if RE_BREAST.search(joined) else "GENERIC"
    for mod, rx in MODALITY_RULES:
        if rx.search(joined): return scope, mod
    return scope, EXT_MODALITY.get(p.suffix.lower(), "CLINICAL")

def remove_empty_dirs(root: Path) -> int:
    removed = 0
    for dirpath, dirnames, filenames in os.walk(root, topdown=False):
        d = Path(dirpath)
        try:
            if not any(Path(dirpath).iterdir()):
                d.rmdir(); removed += 1
        except Exception:
            pass
    return removed

def main():
    ap = argparse.ArgumentParser(description="Clean/merge raw tree into new SCOPE/MODALITY layout")
    ap.add_argument("--sources", nargs="*", default=[
        os.path.join("data","01_raw","NANOMEDICINE"),
        os.path.join("data","01_raw","NANOPARTICLES"),
        os.path.join("data","01_raw","BRCA set"),
        os.path.join("data","01_raw","_misc"),
    ], help="Legacy folders to sweep & merge")
    ap.add_argument("--raw-root", default=os.path.join("data","01_raw"), help="Root for final layout")
    ap.add_argument("--dry", action="store_true")
    ap.add_argument("--copy", action="store_true", help="Copy instead of move")
    args = ap.parse_args()

    raw_root = Path(args.raw_root); raw_root.mkdir(parents=True, exist_ok=True)
    src_roots = [Path(s) for s in args.sources if Path(s).exists()]
    files = []
    for r in src_roots:
        files += [p for p in r.rglob("*") if p.is_file()]
    console = Console()
    with Progress(
        TextColumn("[bold blue]Merge"), BarColumn(),
        TextColumn("{task.percentage:>3.0f}% • {task.completed}/{task.total}"),
        TimeRemainingColumn(), console=console, transient=False
    ) as prog:
        t = prog.add_task("Merge", total=len(files))
        merged = skipped = errs = 0
        for p in files:
            try:
                scope, mod = classify(p)
                dst_dir = raw_root / scope / mod
                dst_dir.mkdir(parents=True, exist_ok=True)
                dst = dst_dir / p.name
                if dst.exists():
                    if dst.stat().st_size == p.stat().st_size and sha1_of(dst)==sha1_of(p):
                        if not args.copy and not args.dry:
                            p.unlink(missing_ok=True)
                        skipped += 1; prog.update(t, advance=1); continue
                    stem, ext = os.path.splitext(p.name); k = 1
                    while (dst_dir / f"{stem}__dup{k}{ext}").exists(): k += 1
                    dst = dst_dir / f"{stem}__dup{k}{ext}"
                log(f"[MERGE] {p} -> {dst}")
                if not args.dry:
                    if args.copy:
                        shutil.copy2(p, dst)
                    else:
                        try: os.replace(p, dst)
                        except Exception:
                            shutil.copy2(p, dst); p.unlink(missing_ok=True)
                merged += 1
            except Exception as e:
                log(f"[MERGE][ERROR] {p}: {e}"); errs += 1
            finally:
                prog.update(t, advance=1)
    # cleanup emptied folders
    removed_total = 0
    for r in src_roots:
        removed_total += remove_empty_dirs(r)
    console.print(f"[green]Merge done[/green] merged={merged} skipped={skipped} errors={errs} | removed_empty={removed_total}")
    log(f"[CLEAN] merged={merged} skipped={skipped} errors={errs} removed_empty={removed_total}")

if __name__ == "__main__":
    main()
