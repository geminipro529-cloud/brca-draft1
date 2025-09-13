#!/usr/bin/env python3
# Promote files from OMICS/misc into rnaseq/genomics/methylation/proteomics/singlecell
# Safe, idempotent, resumable. Logs -> logs/log.txt
from __future__ import annotations
import os, sys, time, csv, argparse, random, hashlib, shutil, concurrent.futures as cf
from pathlib import Path

# ---- UI ----
try:
    from rich.console import Console
    from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn
except Exception:
    print("[ERROR] Missing 'rich'. Install: pip install rich", file=sys.stderr); sys.exit(1)

console = Console()
LOG = Path("logs") / "log.txt"

def log(msg:str)->None:
    LOG.parent.mkdir(parents=True, exist_ok=True)
    with LOG.open("a", encoding="utf-8") as fh:
        fh.write(time.strftime("[%Y-%m-%d %H:%M:%S] ")+msg+"\n")

# ---- heuristics ----
# double-exts handled (e.g., vcf.gz, fastq.gz, mzml.gz)
DOUBLE_EXTS = ("vcf.gz","fastq.gz","fq.gz","mzml.gz")
GENOMICS_EXT = {"maf","vcf","vcf.gz","bcf","seg","bed","gff","gtf"}
METHYL_EXT   = {"idat","rds"}
PROTEO_EXT   = {"mzml","raw","mzml.gz"}
SC_EXT       = {"h5ad","loom","mtx"}
RNASEQ_EXT   = {"fastq","fq","fastq.gz","fq.gz","bam","cram","sra","gct"}

NAME_MARKERS = {
    "genomics":   ("maf","vcf","gistic","segment","cnv","mutation","variants","somatic"),
    "methylation":("methyl","idat","beta","m_value","cg","illumina 450k","epic"),
    "proteomics": ("proteome","proteomics","phospho","mzml","maxquant","pride","cptac"),
    "singlecell": ("singlecell","scRNA","scanpy","seurat","h5ad","loom","matrix.mtx","barcodes.tsv","features.tsv"),
    "rnaseq":     ("rnaseq","rna_seq","fpkm","tpm","htseq","counts","fastq","star","salmon","kallisto"),
}

# CSV/TSV header tokens to sniff
HEADER_MARKERS = {
    "genomics":   {"hugo_symbol","chrom","chromosome","pos","position","ref","alt","tumor_sample_barcode","variant_classification","segment_mean"},
    "methylation":{"beta","m_value","cg","probe_id","illumina"},
    "proteomics": {"protein","peptide","psm","intensity","accession","uniprot"},
    "singlecell": {"barcodes","features","genes","umi","cell_id","seurat","scanpy"},
    "rnaseq":     {"gene_id","gene","ensembl","count","counts","tpm","fpkm","htseq"},
}

TEXT_EXT = {".csv",".tsv",".txt"}

def read_header_tokens(p: Path, max_bytes: int = 4096) -> set[str]:
    try:
        with p.open("r", encoding="utf-8", errors="ignore") as fh:
            head = fh.read(max_bytes)
        line = head.splitlines()[0] if head else ""
        # fall back to the first non-empty line
        for ln in head.splitlines():
            if ln.strip():
                line = ln; break
        # split by comma/tab/semicolon/pipe
        tokens = [t.strip().lower() for t in re_split_any(line)]
        return set(tokens)
    except Exception:
        return set()

def re_split_any(s: str):
    import re
    return re.split(r"[,\t;|]", s)

def lower_ext2(p: Path) -> str:
    name = p.name.lower()
    for de in DOUBLE_EXTS:
        if name.endswith("."+de): return de
    return p.suffix.lower().lstrip(".")

def files_identical(a: Path, b: Path) -> bool:
    try:
        if not a.exists() or not b.exists(): return False
        sa, sb = a.stat().st_size, b.stat().st_size
        if sa != sb: return False
        return sha1_4mb(a) == sha1_4mb(b)
    except Exception:
        return False

def sha1_4mb(p: Path) -> str:
    h=hashlib.sha1()
    with p.open("rb") as fh:
        while True:
            b=fh.read(4_194_304)
            if not b: break
            h.update(b)
            if fh.tell()>=4_194_304: break
    return h.hexdigest()

def unique_dest(dst: Path) -> Path:
    parent, stem = dst.parent, dst.stem
    ext = dst.suffix
    k=1; cand=dst
    while cand.exists():
        cand = parent / f"{stem}__dup{k}{ext}"
        k+=1
    return cand

def choose_bucket(fp: Path) -> str | None:
    """Return one of rnaseq/genomics/methylation/proteomics/singlecell, or None if unsure."""
    ext2 = lower_ext2(fp)
    name = fp.name.lower()

    # 1) Extension-first (strong)
    if ext2 in GENOMICS_EXT: return "genomics"
    if ext2 in METHYL_EXT:   return "methylation"
    if ext2 in PROTEO_EXT:   return "proteomics"
    if ext2 in SC_EXT:       return "singlecell"
    if ext2 in RNASEQ_EXT:   return "rnaseq"

    # 2) Filename markers (moderate)
    for bucket, keys in NAME_MARKERS.items():
        if any(k.lower() in name for k in keys):
            return bucket

    # 3) Header sniff (csv/tsv/txt only)
    if fp.suffix.lower() in TEXT_EXT:
        hdr = read_header_tokens(fp)
        if hdr:
            # whichever bucket gets most hits wins
            scores = {b: len(hdr & set(HEADER_MARKERS[b])) for b in HEADER_MARKERS}
            bucket, score = max(scores.items(), key=lambda kv: kv[1])
            if score >= 2:
                return bucket

    # 4) We’re not confident; leave in misc for manual review
    return None

def move_or_copy(src: Path, dst: Path, mode: str, retries: int = 3) -> str:
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.exists():
        if files_identical(src, dst):
            return "skip_identical"
        dst = unique_dest(dst)
    last=None
    for attempt in range(1, retries+1):
        try:
            if mode == "copy":
                shutil.copy2(src, dst)
            else:
                try:
                    os.replace(src, dst)   # atomic when same volume
                except Exception:
                    shutil.copy2(src, dst)
                    try: src.unlink()
                    except Exception: pass
            return "ok"
        except Exception as e:
            last = e
            time.sleep(0.25*attempt + random.random()*0.2)
    raise last  # type: ignore

def iter_misc_files(raw_root: Path, scopes: list[str]) -> list[tuple[Path, Path, str]]:
    """Yield (file, scope_root, scope_name) for everything under <raw_root>/<scope>/OMICS/misc"""
    out=[]
    for scope in scopes:
        base = raw_root / scope / "OMICS" / "misc"
        if not base.exists(): continue
        for p in base.rglob("*"):
            if p.is_file():
                out.append((p, base.parent, scope))  # scope_root = <raw>/<scope>/OMICS
    return out

def worker(item: tuple[Path, Path, str], mode: str) -> tuple[str, str, str, str]:
    src, scope_root, scope = item
    try:
        bucket = choose_bucket(src)
        if not bucket:
            return ("unsure", str(src), "", scope)
        dst = scope_root / bucket / src.name
        status = move_or_copy(src, dst, mode)
        if status == "ok":
            log(f"[PROMOTE][{mode.upper()}] {src} -> {dst}")
        elif status == "skip_identical":
            log(f"[PROMOTE][SKIP_IDENT] {src} == {dst}")
        return (status, str(src), str(dst), scope)
    except Exception as e:
        log(f"[PROMOTE][ERROR] {src}: {type(e).__name__}: {e}")
        return ("error", str(src), "", scope)

def main():
    ap = argparse.ArgumentParser(description="Promote OMICS/misc files into typed buckets automatically.")
    ap.add_argument("--raw-root", default=os.path.join("data","01_raw"))
    ap.add_argument("--scopes", nargs="*", default=["BRCA","NANOMEDICINE","GENERIC"])
    ap.add_argument("--mode", choices=["move","copy"], default="move")
    ap.add_argument("--workers", type=int, default=3)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    raw_root = Path(args.raw_root)
    items = iter_misc_files(raw_root, args.scopes)
    if not items:
        console.print("[yellow]No files under OMICS/misc — nothing to promote.[/yellow]")
        return

    promoted=skipped=unsure=errors=0
    with Progress(
        TextColumn("[bold blue]Promote OMICS/misc"), BarColumn(),
        TextColumn("{task.completed}/{task.total} • {task.percentage:>3.0f}%"),
        TimeRemainingColumn(), console=console, refresh_per_second=8, transient=False
    ) as prog:
        t = prog.add_task("Promote OMICS/misc", total=len(items))
        if args.dry_run:
            # fast scan summary
            for it in items:
                b = choose_bucket(it[0])
                if b: promoted += 1
                else: unsure += 1
                prog.update(t, advance=1)
            console.print(f"[cyan]DRY-RUN[/cyan] candidates={promoted} unsure={unsure}")
            return

        with cf.ThreadPoolExecutor(max_workers=max(1,args.workers)) as ex:
            futs = [ex.submit(worker, it, args.mode) for it in items]
            for fut in cf.as_completed(futs):
                status, _src, _dst, _scope = fut.result()
                if status == "ok": promoted += 1
                elif status in ("skip_identical",): skipped += 1
                elif status == "unsure": unsure += 1
                else: errors += 1
                prog.update(t, advance=1)

    console.print(f"[green]Done.[/green] promoted={promoted} skipped={skipped} unsure={unsure} errors={errors}")
    log(f"[PROMOTE][SUMMARY] mode={args.mode} workers={args.workers} promoted={promoted} skipped={skipped} unsure={unsure} errors={errors}")

if __name__ == "__main__":
    main()
