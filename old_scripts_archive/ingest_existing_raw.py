#!/usr/bin/env python3
# scripts/ingest_existing_raw.py
# Ingest already-downloaded data: scan → classify → de-dupe → arrange into data/01_raw/<SCOPE>/<MODALITY>
# Logs to logs/log.txt and writes an inventory CSV. Idempotent and safe to re-run.

from __future__ import annotations
import os, sys, re, csv, time, argparse, shutil, hashlib
from pathlib import Path
from typing import Tuple, List

# ---- progress UI
try:
    from rich.console import Console
    from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn
except ImportError:
    print("[ERROR] Missing 'rich'. Install: pip install rich", file=sys.stderr); sys.exit(1)

LOG = Path("logs") / "log.txt"
def log(msg: str) -> None:
    LOG.parent.mkdir(parents=True, exist_ok=True)
    with LOG.open("a", encoding="utf-8") as fh:
        fh.write(time.strftime("[%Y-%m-%d %H:%M:%S] ") + msg + "\n")

# ---- classification heuristics (same rules we use elsewhere)
RE_BREAST = re.compile(r"\b(brca|breast|metabric|tcga[-_]?brca|cptac[-_]?brca)\b", re.I)
MODALITY_RULES: List[Tuple[str, re.Pattern]] = [
    ("TRANSCRIPTOMICS", re.compile(r"\b(rnaseq|rna[-_ ]seq|transcript|counts|htseq|fpkm|tpm|gene[-_ ]exp)\b", re.I)),
    ("GENOMICS",        re.compile(r"\b(mutation|maf|vcf|genome|cnv|cna|exome|germline|somatic)\b", re.I)),
    ("PROTEOMICS",      re.compile(r"\b(proteom|cptac|pdc|mass[-_ ]spec|mzml|raw)\b", re.I)),
    ("SINGLECELL",      re.compile(r"\b(single[-_ ]?cell|scrna|10x|cellranger|sce|seurat|h5ad|loom)\b", re.I)),
    ("IMAGING",         re.compile(r"\b(dicom|\.dcm\b|radiology|pathology[-_ ]slide|\.svs\b)\b", re.I)),
    ("METHYLATION",     re.compile(r"\b(methyl|450k|850k|bisulfite|epigen)\b", re.I)),
    ("EV",              re.compile(r"\b(exosome|vesicle|exocarta|vesiclepedia)\b", re.I)),
    ("METABOLOMICS",    re.compile(r"\b(metabolom|metabolite|workbench)\b", re.I)),
    ("PHARMACO",        re.compile(r"\b(depmap|gdsc|drug|dose|ic50|auc|cellminer)\b", re.I)),
    ("CLINICAL",        re.compile(r"\b(clinical|phenotype|patient|survival|follow[-_ ]up|metadata)\b", re.I)),
    ("NANOMEDICINE",    re.compile(r"\b(nano|liposome|nanoparticle|micelle|cananolab|euon)\b", re.I)),
]
EXT_MODALITY = {
    ".fastq": "TRANSCRIPTOMICS", ".fq": "TRANSCRIPTOMICS", ".bam": "TRANSCRIPTOMICS", ".gct": "TRANSCRIPTOMICS",
    ".vcf": "GENOMICS", ".maf": "GENOMICS",
    ".mzml": "PROTEOMICS", ".raw": "PROTEOMICS",
    ".svs": "IMAGING", ".dcm": "IMAGING",
    ".h5ad": "SINGLECELL", ".loom": "SINGLECELL", ".rds": "SINGLECELL",
    ".parquet": "CLINICAL", ".csv": "CLINICAL", ".tsv": "CLINICAL", ".xlsx": "CLINICAL", ".xls": "CLINICAL",
    ".txt": "CLINICAL", ".json": "CLINICAL",
}

def sha1_of(path: Path, limit: int = 4_194_304) -> str:
    h = hashlib.sha1()
    with path.open("rb") as fh:
        while True:
            b = fh.read(limit)
            if not b: break
            h.update(b)
            if limit and fh.tell() >= limit: break
    return h.hexdigest()

def classify_scope_mod(path: Path) -> Tuple[str, str]:
    joined = " ".join(p.lower() for p in path.parts)
    scope = "BRCA" if RE_BREAST.search(joined) else "GENERIC"
    for mod, rx in MODALITY_RULES:
        if rx.search(joined): return scope, mod
    return scope, EXT_MODALITY.get(path.suffix.lower(), "CLINICAL")

def plan_sources(roots: List[Path]) -> List[Path]:
    files: List[Path] = []
    for r in roots:
        if not r.exists(): continue
        for p in r.rglob("*"):
            if p.is_file():
                files.append(p)
    return files

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def unique_dest(dst_dir: Path, name: str) -> Path:
    stem, ext = os.path.splitext(name)
    candidate = dst_dir / name
    k = 1
    while candidate.exists():
        candidate = dst_dir / f"{stem}__dup{k}{ext}"
        k += 1
    return candidate

def main():
    ap = argparse.ArgumentParser(description="Ingest already-downloaded data into data/01_raw/<SCOPE>/<MODALITY>")
    ap.add_argument("--sources", nargs="*", default=[
        os.path.join("data","01_raw","NANOMEDICINE"),
        os.path.join("data","01_raw","NANOPARTICLES"),
        os.path.join("data","01_raw","BRCA set"),
        os.path.join("data","01_raw","_misc"),
        os.path.join("data","01_raw","_INBOX"),
    ], help="Folders to sweep")
    ap.add_argument("--raw-root", default=os.path.join("data","01_raw"), help="Destination RAW root")
    ap.add_argument("--mode", choices=["move","copy"], default="move", help="Move (default) or copy")
    ap.add_argument("--dry", action="store_true", help="Plan only; no writes")
    ap.add_argument("--inventory", default=os.path.join("data","01_raw","_inventory.csv"),
                    help="CSV to append inventory rows")
    args = ap.parse_args()

    console = Console()
    raw_root = Path(args.raw_root); ensure_dir(raw_root)
    src_roots = [Path(s) for s in args.sources]

    files = plan_sources(src_roots)
    ensure_dir(Path(args.inventory).parent)
    inv_exists = Path(args.inventory).exists()
    inv = open(args.inventory, "a", encoding="utf-8", newline="")
    w = csv.writer(inv)
    if not inv_exists:
        w.writerow(["src","dst","scope","modality","size_bytes","sha1_4mb","action","error"])

    moved = skipped = errs = 0
    with Progress(
        TextColumn("[bold blue]Ingest"), BarColumn(),
        TextColumn("{task.completed}/{task.total} • {task.percentage:>3.0f}%"),
        TimeRemainingColumn(), console=console, transient=False, refresh_per_second=6
    ) as prog:
        t = prog.add_task("Ingest", total=len(files))
        for src in files:
            try:
                scope, mod = classify_scope_mod(src)
                dst_dir = raw_root / scope / mod
                ensure_dir(dst_dir)
                dst = dst_dir / src.name
                size = src.stat().st_size
                sha1 = ""
                action = ""
                if dst.exists():
                    # dedupe (size + partial hash)
                    if dst.stat().st_size == size:
                        try:
                            sha1_src = sha1_of(src)
                            sha1_dst = sha1_of(dst)
                            if sha1_src == sha1_dst:
                                action = "skip_identical"
                                if args.mode == "move" and not args.dry:
                                    try: src.unlink()
                                    except Exception: pass
                                skipped += 1
                                w.writerow([str(src), str(dst), scope, mod, size, sha1_src, action, ""])
                                prog.update(t, advance=1)
                                continue
                            else:
                                dst = unique_dest(dst_dir, src.name)
                        except Exception:
                            dst = unique_dest(dst_dir, src.name)
                    else:
                        dst = unique_dest(dst_dir, src.name)

                log(f"[INGEST] {src} -> {dst}")
                if not args.dry:
                    if args.mode == "move":
                        try:
                            os.replace(src, dst)  # atomic if same volume
                        except Exception:
                            shutil.copy2(src, dst); src.unlink(missing_ok=True)
                    else:
                        shutil.copy2(src, dst)
                moved += 1; action = f"{args.mode}"
                try: sha1 = sha1_of(dst)
                except Exception: sha1 = ""
                w.writerow([str(src), str(dst), scope, mod, size, sha1, action, ""])
            except Exception as e:
                errs += 1
                err = f"{type(e).__name__}: {e}"
                log(f"[INGEST][ERROR] {src}: {err}")
                w.writerow([str(src), "", "", "", "", "", "error", err])
            finally:
                prog.update(t, advance=1)
    inv.close()
    console.print(f"[green]Ingest done[/green] moved={moved} skipped={skipped} errors={errs}")
    log(f"[INGEST] moved={moved} skipped={skipped} errors={errs} inventory={args.inventory}")

if __name__ == "__main__":
    main()
