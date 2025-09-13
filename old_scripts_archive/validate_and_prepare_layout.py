#!/usr/bin/env python3
# validate_and_prepare_layout.py — creates a minimal raw-data schema & (optionally) audits
# Fixes: auto-writes data/layout.yaml if missing; no PyYAML requirement to run --create-only.
from __future__ import annotations
import os, sys, csv, json, time, argparse, hashlib
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

# ---------- Rich UI ----------
try:
    from rich.console import Console
    from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn
except Exception:
    print("[ERROR] Missing 'rich'. Install with: pip install rich", file=sys.stderr); sys.exit(1)

LOG = Path("logs") / "log.txt"
def log(msg:str)->None:
    LOG.parent.mkdir(parents=True, exist_ok=True)
    with LOG.open("a", encoding="utf-8") as fh:
        fh.write(time.strftime("[%Y-%m-%d %H:%M:%S] ")+msg+"\n")

# ---------- Default schema (professor-grade, minimal & extensible) ----------
DEFAULT_YAML = """\
version: 1
raw_root: "data/01_raw"
scopes: ["BRCA", "NANOMEDICINE", "GENERIC"]
special_folders:
  inbox: "_INBOX"
  quarantine: "_QUARANTINE"
buckets:
  OMICS:
    rnaseq:         ["parquet","csv","tsv","gct","txt","gz","fastq","fq","bam","cram","sra"]
    genomics:       ["maf","vcf","vcf.gz","bed","gff","gtf","seg","txt","tsv","csv"]
    proteomics:     ["mzml","raw","csv","tsv","parquet"]
    methylation:    ["idat","csv","tsv","parquet","rds"]
    metabolomics:   ["csv","tsv","parquet","mzml"]
    singlecell:     ["h5ad","loom","rds","mtx","tsv","csv","parquet"]
    exosomes:       ["csv","tsv","parquet","xlsx","xls"]
  IMAGING:
    _flat:          ["dcm","dicom","svs","tif","tiff","ndpi","png","jpg","jpeg","bmp"]
  CLINICAL:
    _flat:          ["csv","tsv","parquet","xlsx","xls","json"]
  DOCS:
    _flat:          ["pdf"]
noisy_extensions: ["part","php","cgi","jsp","aspx","html","htm"]
noisy_names_like: ["index","download","query","search","view"]
hints:
  brca_keywords: ["brca","breast","metabric","tcga-brca","cptac-brca"]
  rnaseq_like:   ["rnaseq","rna_seq","fpkm","tpm","htseq","counts"]
  nano_like:     ["nano","nanoparticle","liposome","micelle","cananolab","euon","vesicle","exosome"]
"""

DEFAULT_CFG = {
    "version": 1,
    "raw_root": "data/01_raw",
    "scopes": ["BRCA","NANOMEDICINE","GENERIC"],
    "special_folders": {"inbox":"_INBOX","quarantine":"_QUARANTINE"},
    "buckets": {
        "OMICS": {
            "rnaseq": ["parquet","csv","tsv","gct","txt","gz","fastq","fq","bam","cram","sra"],
            "genomics": ["maf","vcf","vcf.gz","bed","gff","gtf","seg","txt","tsv","csv"],
            "proteomics": ["mzml","raw","csv","tsv","parquet"],
            "methylation": ["idat","csv","tsv","parquet","rds"],
            "metabolomics": ["csv","tsv","parquet","mzml"],
            "singlecell": ["h5ad","loom","rds","mtx","tsv","csv","parquet"],
            "exosomes": ["csv","tsv","parquet","xlsx","xls"],
        },
        "IMAGING": {"_flat": ["dcm","dicom","svs","tif","tiff","ndpi","png","jpg","jpeg","bmp"]},
        "CLINICAL": {"_flat": ["csv","tsv","parquet","xlsx","xls","json"]},
        "DOCS": {"_flat": ["pdf"]},
    },
    "noisy_extensions": ["part","php","cgi","jsp","aspx","html","htm"],
    "noisy_names_like": ["index","download","query","search","view"],
    "hints": {
        "brca_keywords":["brca","breast","metabric","tcga-brca","cptac-brca"],
        "rnaseq_like":["rnaseq","rna_seq","fpkm","tpm","htseq","counts"],
        "nano_like":["nano","nanoparticle","liposome","micelle","cananolab","euon","vesicle","exosome"],
    },
}

def write_default_layout(path: Path) -> dict:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(DEFAULT_YAML, encoding="utf-8")
    # also drop a JSON twin for editors / tooling
    path.with_suffix(".json").write_text(json.dumps(DEFAULT_CFG, indent=2), encoding="utf-8")
    log(f"[LAYOUT] Created default at {path} and {path.with_suffix('.json')}")
    return DEFAULT_CFG

def load_layout(path: Path) -> dict:
    if not path.exists():
        return write_default_layout(path)
    try:
        import yaml  # type: ignore
        with path.open("r", encoding="utf-8") as f:
            return yaml.safe_load(f)  # type: ignore
    except Exception:
        js = path.with_suffix(".json")
        if js.exists():
            try:
                return json.loads(js.read_text(encoding="utf-8"))
            except Exception:
                pass
        Console().print("[yellow]PyYAML not installed and JSON twin missing; using built-in defaults.[/yellow]")
        log("[LAYOUT][WARN] Using built-in defaults (install PyYAML to parse YAML).")
        return DEFAULT_CFG

# ---------- Tree creation & (optional) audit ----------
def ensure_tree(cfg: dict):
    root = Path(cfg["raw_root"])
    for scope in cfg["scopes"]:
        sroot = root / scope; sroot.mkdir(parents=True, exist_ok=True)
        for bucket, subs in cfg["buckets"].items():
            bdir = sroot / bucket
            if "_flat" in subs: bdir.mkdir(parents=True, exist_ok=True)
            for sub in subs:
                if sub == "_flat": continue
                (bdir / sub).mkdir(parents=True, exist_ok=True)
    (root / cfg["special_folders"]["inbox"]).mkdir(parents=True, exist_ok=True)
    (root / cfg["special_folders"]["quarantine"]).mkdir(parents=True, exist_ok=True)
    log(f"[LAYOUT] Ensured tree under {root}")

def sha1_4mb(p: Path) -> str:
    h=hashlib.sha1()
    with p.open("rb") as fh:
        while True:
            b=fh.read(4_194_304)
            if not b: break
            h.update(b)
            if fh.tell()>=4_194_304: break
    return h.hexdigest()

def allowed_ext_for(bucket: str, sub: str, cfg: dict) -> set[str]:
    bs = cfg["buckets"].get(bucket, {})
    out=set()
    if sub in bs: out.update(x.lower() for x in bs[sub])
    if "_flat" in bs: out.update(x.lower() for x in bs["_flat"])
    return out

def classify_path(p: Path, cfg: dict):
    try:
        parts = p.relative_to(cfg["raw_root"]).parts  # type: ignore
    except Exception:
        return (None,None,None)
    if not parts: return (None,None,None)
    scope = parts[0] if parts[0] in cfg["scopes"] else None
    if not scope: return (None,None,None)
    if len(parts)==1: return (scope,None,None)
    bucket = parts[1] if parts[1] in cfg["buckets"] else None
    sub=None
    if bucket:
        subs = cfg["buckets"][bucket]
        if "_flat" in subs: sub="_flat"
        elif len(parts)>=3 and parts[2] in subs: sub=parts[2]
    return (scope,bucket,sub)

def portal_like(name: str, cfg: dict) -> bool:
    base = name.lower()
    if any(base.endswith("."+ext) for ext in cfg["noisy_extensions"]): return True
    if any(k in base for k in cfg["noisy_names_like"]): return True
    return False

def audit(cfg: dict, fix_noisy: bool, workers: int):
    raw_root = Path(cfg["raw_root"])
    qdir = raw_root / cfg["special_folders"]["quarantine"]
    report_dir = raw_root / "_validation"; report_dir.mkdir(parents=True, exist_ok=True)

    rpt = (report_dir / "report.csv").open("w", encoding="utf-8", newline="")
    dup = (report_dir / "duplicates.csv").open("w", encoding="utf-8", newline="")
    noisy = (report_dir / "noisy.csv").open("w", encoding="utf-8", newline="")
    wr = csv.writer(rpt); wr.writerow(["file","scope","bucket","sub","ext","status","reason"])
    wd = csv.writer(dup); wd.writerow(["sha1_4mb","size","count","examples"])
    wn = csv.writer(noisy); wn.writerow(["file","reason","action"])

    hashes = {}
    files=[p for p in raw_root.rglob("*") if p.is_file()]
    console = Console()
    total=len(files)

    def process(f: Path):
        try:
            scope,bucket,sub = classify_path(f, cfg)
            ext = f.suffix.lower().lstrip(".")
            if portal_like(f.name, cfg):
                action=""
                if fix_noisy:
                    qdir.mkdir(parents=True, exist_ok=True)
                    dest = qdir / f.name
                    try: os.replace(f, dest)
                    except Exception:
                        dest.write_bytes(f.read_bytes()); f.unlink(missing_ok=True)
                    action="moved_to_quarantine"
                wn.writerow([str(f), "portal_or_temp", action])
                wr.writerow([str(f), scope, bucket, sub, ext, "NOISY", "portal_or_temp"])
                return
            if not scope:
                wr.writerow([str(f), "", "", "", ext, "OUT_OF_TREE", "not_under_scopes"])
            elif not bucket:
                wr.writerow([str(f), scope, "", "", ext, "MISPLACED", "missing_bucket"])
            else:
                allowed = allowed_ext_for(bucket, sub or "_flat", cfg)
                if ext and allowed and ext not in allowed:
                    wr.writerow([str(f), scope, bucket, sub, ext, "EXT_MISMATCH", f"not in {sorted(allowed)}"])
                else:
                    wr.writerow([str(f), scope, bucket, sub, ext, "OK", ""])
            try:
                sz=f.stat().st_size
                if sz>0:
                    h=sha1_4mb(f)
                    hashes.setdefault((sz,h), []).append(str(f))
            except Exception:
                pass
        except Exception:
            pass

    with Progress(
        TextColumn("[bold blue]Audit"), BarColumn(),
        TextColumn("{task.completed}/{task.total} • {task.percentage:>3.0f}% • {task.fields[remaining]} left"),
        TimeRemainingColumn(), console=console, transient=False, refresh_per_second=6
    ) as prog:
        t=prog.add_task("Audit", total=total, remaining=total)
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futs={ex.submit(process,f):f for f in files}
            for i,fut in enumerate(as_completed(futs),1):
                fut.result()
                prog.update(t, advance=1, remaining=total-i)

    for (sz,h), paths in hashes.items():
        if len(paths)>1:
            wd.writerow([h, sz, len(paths), " | ".join(paths[:5])])

    rpt.close(); dup.close(); noisy.close()
    console.print(f"[green]Audit complete[/green] → {report_dir}")
    log(f"[AUDIT] report={report_dir/'report.csv'} dup={report_dir/'duplicates.csv'} noisy={report_dir/'noisy.csv'} fix_noisy={fix_noisy}")

def main():
    
    ap = argparse.ArgumentParser(description="Prepare minimal schema & audit raw data.")
    ap.add_argument("--layout", default=os.path.join("data","layout.yaml"))
    ap.add_argument("--create-only", action="store_true", help="Only create folders, skip audit")
    ap.add_argument("--fix-noisy", action="store_true", help="Move .part/portal-like files to _QUARANTINE")
    ap.add_argument("--workers", type=int, default=4, help="Parallel workers for audit")
    args = ap.parse_args()

    cfg = load_layout(Path(args.layout))
    ensure_tree(cfg)
    if args.create_only:
        Console().print("[green]OK[/green] Layout folders ensured.")
        return
    audit(cfg, fix_noisy=args.fix_noisy, workers=args.workers)

if __name__ == "__main__":
    main()
