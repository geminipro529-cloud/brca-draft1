#!/usr/bin/env python3
# Arrange files into a minimal, BRCA-strict layout with dedupe & logging.
from __future__ import annotations
import os, sys, re, time, argparse, shutil, hashlib
from pathlib import Path

try:
    from rich.console import Console
    from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn
except Exception:
    print("[ERROR] Missing 'rich'. Install with: pip install rich", file=sys.stderr); sys.exit(1)

LOG = Path("logs") / "log.txt"

# ---- heuristics (minimal folders) ----
RE_BRCA = re.compile(r"\b(brca|breast|metabric|tcga[-_]?brca|cptac[-_]?brca)\b", re.I)

IMAGING_EXT = {".dcm",".dicom",".svs",".tif",".tiff",".ndpi",".png",".jpg",".jpeg",".bmp",".mrxs"}
DOCS_EXT    = {".pdf",".txt",".md",".rtf",".doc",".docx"}
# Everything else (tables + omics binaries) goes to OMICS to keep folders minimal
OMICS_EXT   = {
    ".csv",".tsv",".parquet",".xlsx",".xls",".json",".gct",".vcf",".maf",".bed",".bw",".gtf",".gff",
    ".fastq",".fq",".bam",".cram",".sra",".h5ad",".loom",".rds",".mzml",".raw"
}

def log(msg:str)->None:
    LOG.parent.mkdir(parents=True, exist_ok=True)
    with LOG.open("a", encoding="utf-8") as fh:
        fh.write(time.strftime("[%Y-%m-%d %H:%M:%S] ")+msg+"\n")

def sha1_4mb(p:Path)->str:
    h=hashlib.sha1()
    with p.open("rb") as fh:
        while True:
            b=fh.read(4_194_304)
            if not b: break
            h.update(b)
            if fh.tell()>=4_194_304: break
    return h.hexdigest()

def classify_scope(path:Path)->str:
    joined=" ".join(s.lower() for s in path.parts)
    return "BRCA" if RE_BRCA.search(joined) else "GENERIC"

def classify_bucket(path:Path)->str:
    ext=path.suffix.lower()
    if ext in IMAGING_EXT: return "IMAGING"
    if ext in DOCS_EXT:    return "DOCS"
    return "OMICS"  # default minimal bucket

def unique_dest(dst_dir:Path, name:str)->Path:
    stem,ext=os.path.splitext(name); k=1
    cand=dst_dir/name
    while cand.exists():
        cand=dst_dir/f"{stem}__dup{k}{ext}"; k+=1
    return cand

def arrange(sources:list[Path], raw_root:Path, mode:str, flat:bool, dry:bool)->tuple[int,int,int]:
    files=[]
    for r in sources:
        if not r.exists(): continue
        for p in r.rglob("*"):
            if p.is_file(): files.append(p)
    console=Console()
    moved=skipped=errs=0
    with Progress(
        TextColumn("[bold blue]Arrange"), BarColumn(),
        TextColumn("{task.completed}/{task.total} • {task.percentage:>3.0f}%"),
        TimeRemainingColumn(), console=console, transient=False, refresh_per_second=6
    ) as prog:
        t=prog.add_task("Arrange", total=len(files))
        for src in files:
            try:
                scope=classify_scope(src)
                bucket=classify_bucket(src)
                dst_dir = raw_root / scope if flat else raw_root / scope / bucket
                dst_dir.mkdir(parents=True, exist_ok=True)
                dst=dst_dir/src.name
                if dst.exists() and dst.stat().st_size==src.stat().st_size:
                    try:
                        if sha1_4mb(dst)==sha1_4mb(src):
                            # identical → remove source on move
                            if mode=="move" and not dry:
                                try: src.unlink()
                                except Exception: pass
                            skipped+=1; prog.update(t,advance=1); continue
                        else:
                            dst = unique_dest(dst_dir, src.name)
                    except Exception:
                        dst = unique_dest(dst_dir, src.name)
                log(f"[ARRANGE] {src} -> {dst}")
                if not dry:
                    if mode=="move":
                        try: os.replace(src, dst)  # atomic same-volume
                        except Exception:
                            shutil.copy2(src, dst); src.unlink(missing_ok=True)
                    else:
                        shutil.copy2(src, dst)
                moved+=1
            except Exception as e:
                errs+=1; log(f"[ARRANGE][ERROR] {src}: {type(e).__name__}: {e}")
            finally:
                prog.update(t, advance=1)
    return moved, skipped, errs

def main():
    ap=argparse.ArgumentParser(description="Arrange files into minimal BRCA/GENERIC tree.")
    ap.add_argument("--sources", nargs="*", default=[
        os.path.join("data","01_raw","_INBOX"),
        os.path.join("data","01_raw","NANOMEDICINE"),
        os.path.join("data","01_raw","NANOPARTICLES"),
        os.path.join("data","01_raw","BRCA set"),
        os.path.join("data","01_raw","_misc"),
    ], help="Folders to sweep")
    ap.add_argument("--raw-root", default=os.path.join("data","01_raw"))
    ap.add_argument("--mode", choices=["move","copy"], default="move")
    ap.add_argument("--flat", action="store_true", help="Only BRCA/ and GENERIC/ (no OMICS/IMAGING/DOCS)")
    ap.add_argument("--dry", action="store_true")
    args=ap.parse_args()

    raw_root=Path(args.raw_root); raw_root.mkdir(parents=True, exist_ok=True)
    moved, skipped, errs = arrange([Path(s) for s in args.sources], raw_root, args.mode, args.flat, args.dry)
    print(f"[OK] moved={moved} skipped={skipped} errors={errs} → {raw_root}")
    log(f"[ARRANGE][SUMMARY] moved={moved} skipped={skipped} errors={errs} flat={args.flat} mode={args.mode}")

if __name__=="__main__":
    main()
