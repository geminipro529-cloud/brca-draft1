#!/usr/bin/env python3
# scripts/pipeline_fullrun.py
# One-command orchestrator:
# 1) Read all manifests (CSV/TXT) in one folder
# 2) Download → data/01_raw/_INBOX
# 3) Arrange by scope/modality → data/01_raw/<SCOPE>/<MODALITY>
# 4) Optionally run: structured → convert → annotate → unstructured → crosswalk → master
# Logs: logs/log.txt

from __future__ import annotations
import os, sys, re, csv, json, time, argparse, hashlib, shutil, subprocess
from pathlib import Path
from typing import Dict, List, Tuple, Optional

# ---- Rich + logging (no extra deps beyond rich/requests in your env)
try:
    from rich.console import Console
    from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn, SpinnerColumn
except ImportError:
    print("[ERROR] Missing 'rich'. Install: pip install rich", file=sys.stderr)
    sys.exit(1)

LOG_DIR = Path("logs"); LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "log.txt"
def log(msg: str):
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    with LOG_FILE.open("a", encoding="utf-8") as fh:
        fh.write(time.strftime("[%Y-%m-%d %H:%M:%S] ") + msg + "\n")

console = Console()

# ---- Repo paths (edit if your script names differ)
SCRIPTS = Path("scripts")
DOWNLOADER = SCRIPTS / "download_from_manifest.py"              # robust fetcher you already have
STRUCTURED_UNION = SCRIPTS / "build_structured_union.py"        # optional
BATCH_CONVERT = SCRIPTS / "batch_csvs_to_parquet.py"            # optional
ANNOTATE = SCRIPTS / "annotate_structured_with_vocab.py"        # optional
UNSTRUCTURED = SCRIPTS / "extract_unstructured_lowmem.py"       # optional
CROSSWALK = SCRIPTS / "build_id_crosswalk.py"                   # optional
MASTER = SCRIPTS / "build_master_dataset.py"                    # optional

# ---- Defaults
MANIFEST_DIR = Path("data/00_sources/manifest")
RAW_ROOT = Path("data/01_raw")
INBOX = RAW_ROOT / "_INBOX"   # unified bucket for downloads
ARRANGED = RAW_ROOT           # final tree under RAW_ROOT/<SCOPE>/<MODALITY>

# ---- Classifier (heuristics; YAML optional later)
RE_BREAST = re.compile(r"\b(brca|breast|metabric|tcga[-_]?brca|cptac[-_]?brca)\b", re.I)

MODALITY_RULES: List[Tuple[str, re.Pattern]] = [
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

# fallback by extension
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

def sha1_of(path: Path, size_limit: int = 4_194_304) -> str:
    h = hashlib.sha1()
    with path.open("rb") as fh:
        while True:
            chunk = fh.read(size_limit)
            if not chunk: break
            h.update(chunk)
            if size_limit and fh.tell() >= size_limit: break
    return h.hexdigest()

def classify_scope_and_modality(fp: Path) -> Tuple[str, str]:
    name = fp.name.lower()
    segs = [s.lower() for s in fp.parts]
    joined = " ".join(segs)
    scope = "BRCA" if RE_BREAST.search(joined) else "GENERIC"
    # modality by name/path first
    for mod, rgx in MODALITY_RULES:
        if rgx.search(joined): return scope, mod
    # else by extension
    mod = EXT_MODALITY.get(fp.suffix.lower(), "CLINICAL")
    return scope, mod

def arrange_inbox(inbox: Path, arranged_root: Path, move: bool=True, dry: bool=False) -> Tuple[int,int,int]:
    """Move or copy files from INBOX to ARRANGED/<SCOPE>/<MODALITY>, de-duping by size/hash."""
    files = [p for p in inbox.rglob("*") if p.is_file()]
    moved = skipped = errors = 0
    total = len(files)
    with Progress(
        TextColumn("[bold blue]Arrange"), BarColumn(),
        TextColumn("{task.percentage:>3.0f}% • {task.completed}/{task.total}"),
        TimeRemainingColumn(), console=console, transient=False, refresh_per_second=6
    ) as prog:
        t = prog.add_task("Arrange", total=total)
        for p in files:
            try:
                scope, mod = classify_scope_and_modality(p)
                dst_dir = arranged_root / scope / mod
                dst_dir.mkdir(parents=True, exist_ok=True)
                dst = dst_dir / p.name
                # collision handling
                if dst.exists():
                    if dst.stat().st_size == p.stat().st_size and sha1_of(dst)==sha1_of(p):
                        # identical → remove source
                        if not dry:
                            try: p.unlink()
                            except Exception: pass
                        skipped += 1; prog.update(t, advance=1); continue
                    stem, ext = os.path.splitext(p.name)
                    k = 1
                    while (dst_dir / f"{stem}__dup{k}{ext}").exists(): k += 1
                    dst = dst_dir / f"{stem}__dup{k}{ext}"
                log(f"[ARRANGE] {p} -> {dst}")
                if not dry:
                    if move:
                        try: os.replace(p, dst)
                        except Exception:
                            # fallback: copy+remove (handles cross-device)
                            shutil.copy2(p, dst); p.unlink(missing_ok=True)
                    else:
                        shutil.copy2(p, dst)
                moved += 1
            except Exception as e:
                log(f"[ARRANGE][ERROR] {p}: {e}")
                errors += 1
            finally:
                prog.update(t, advance=1)
    return moved, skipped, errors

def run_subprocess(cmd: List[str], timeout: Optional[int]=None, name: str="step") -> int:
    log(f"[RUN] {' '.join(cmd)}")
    try:
        rc = subprocess.call(cmd, timeout=timeout)
        log(f"[DONE] {name} rc={rc}")
        return rc
    except subprocess.TimeoutExpired:
        log(f"[TIMEOUT] {name}")
        return 124

def main():
    ap = argparse.ArgumentParser(description="Full-run pipeline: download → arrange → (optional) steps")
    ap.add_argument("--manifest-dir", default=str(MANIFEST_DIR), help="Folder with CSV/TXT manifests")
    ap.add_argument("--inbox", default=str(INBOX), help="Unified download bucket")
    ap.add_argument("--arranged-root", default=str(ARRANGED), help="Final RAW root (SCOPE/MODALITY under here)")
    ap.add_argument("--workers", type=int, default=6)
    ap.add_argument("--per-host", type=int, default=2)
    ap.add_argument("--resume", action="store_true")
    ap.add_argument("--dry", action="store_true", help="Plan only (no writes)")
    ap.add_argument("--no-download", action="store_true")
    ap.add_argument("--no-arrange", action="store_true")
    ap.add_argument("--run-structured", action="store_true")
    ap.add_argument("--run-convert", action="store_true")
    ap.add_argument("--run-annotate", action="store_true")
    ap.add_argument("--run-unstructured", action="store_true")
    ap.add_argument("--run-crosswalk", action="store_true")
    ap.add_argument("--run-master", action="store_true")
    args = ap.parse_args()

    manifest_dir = Path(args.manifest_dir)
    inbox = Path(args.inbox); inbox.mkdir(parents=True, exist_ok=True)
    arranged_root = Path(args.arranged_root); arranged_root.mkdir(parents=True, exist_ok=True)

    with Progress(
        SpinnerColumn(), TextColumn("[bold]Full pipeline[/bold]"), BarColumn(),
        TextColumn("{task.percentage:>3.0f}%"), TimeRemainingColumn(),
        console=console, transient=False
    ) as prog:
        total_steps = 2 + sum([
            args.run_structured, args.run_convert, args.run_annotate,
            args.run_unstructured, args.run_crosswalk, args.run_master
        ])
        total_steps = max(1, total_steps)
        tk = prog.add_task("Pipeline", total=total_steps)

        # ---- 1) Download → INBOX
        if not args.no_download:
            if not DOWNLOADER.exists():
                console.print("[red]Missing downloader script:[/red] scripts\\download_from_manifest.py")
                sys.exit(2)
            cmd = [
                sys.executable, str(DOWNLOADER),
                "--manifest", str(manifest_dir),
                "--out-root", str(inbox),
                "--workers", str(args.workers),
                "--per-host", str(args.per_host),
            ]
            if args.resume: cmd.append("--resume")
            if args.dry: cmd.append("--dry")
            rc = run_subprocess(cmd, name="download")
            if rc not in (0,): console.print(f"[red]Downloader exited rc={rc}[/red]")
        prog.update(tk, advance=1)

        # ---- 2) Arrange → RAW_ROOT/SCOPE/MODALITY
        if not args.no_arrange:
            moved, skipped, errors = arrange_inbox(inbox, arranged_root, move=not args.dry, dry=args.dry)
            console.print(f"[green]Arrange complete[/green] moved={moved} skipped={skipped} errors={errors}")
        prog.update(tk, advance=1)

        # ---- Optional stages (call existing scripts if present)
        def maybe(name: str, script: Path, argv: List[str], run_flag: bool):
            if not run_flag: return
            if not script.exists():
                console.print(f"[yellow]Skip {name} (script not found): {script}[/yellow]")
                prog.update(tk, advance=1); return
            rc = run_subprocess([sys.executable, str(script)] + argv, name=name)
            if rc != 0: console.print(f"[red]{name} failed rc={rc}[/red]")
            prog.update(tk, advance=1)

        maybe("structured_union", STRUCTURED_UNION, [
            "--in-dirs", str(arranged_root / "BRCA" / "TRANSCRIPTOMICS"),
            str(arranged_root / "BRCA" / "GENOMICS"),
            str(arranged_root / "BRCA" / "CLINICAL"),
            "--chunksize", "50000"
        ], args.run_structured)

        maybe("convert_parquet", BATCH_CONVERT, [
            "--in-dir", str(arranged_root / "BRCA"),
            "--out-dir", os.path.join("data","03_processed","parquet"),
            "--schema-cache-dir", os.path.join("data","02_interim","schema_cache"),
            "--skip-existing", "--min-mb", "700", "--compression", "zstd",
            "--workers", str(max(2, args.workers//2))
        ], args.run_convert)

        maybe("annotate", ANNOTATE, [
            "--in", os.path.join("data","02_interim","structured_union"),
            "--vocab", os.path.join("data","02_interim","VOCAB_COMPILED","vocabulary_enriched.parquet"),
            "--out", os.path.join("data","02_interim","structured_annotated_v2")
        ], args.run_annotate)

        maybe("unstructured", UNSTRUCTURED, [
            "--in", str(arranged_root / "BRCA"),
            "--vocab", os.path.join("data","02_interim","VOCAB_COMPILED","vocabulary_enriched.parquet"),
            "--out", os.path.join("data","02_interim","unstructured_lowmem"),
            "--output-mode", "single", "--progress"
        ], args.run_unstructured)

        maybe("crosswalk", CROSSWALK, [
            "--in-v2", os.path.join("data","02_interim","structured_annotated_v2"),
            "--in-old", os.path.join("data","02_interim","structured_annotated"),
            "--out", os.path.join("data","02_interim","id_map"), "--prefer", "sample"
        ], args.run_crosswalk)

        maybe("master", MASTER, [
            "--structured", os.path.join("data","02_interim","structured_annotated_v2"),
            "--quant", os.path.join("data","02_interim","quantitative"),
            "--qual", os.path.join("data","02_interim","unstructured_lowmem"),
            "--out", os.path.join("data","03_processed","master_dataset"),
            "--breast-only"
        ], args.run_master)

        prog.update(tk, completed=total_steps)

    console.print("[bold green]Full pipeline finished.[/bold green]")
    log("[PIPELINE] done")

if __name__ == "__main__":
    main()
