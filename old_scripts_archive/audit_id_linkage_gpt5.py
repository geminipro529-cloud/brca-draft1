#!/usr/bin/env python3
# audit_id_linkage_gpt5.py — fast, concurrent ID linkage auditor (GPT-5 Thinking)
# Windows-safe; minimal deps (pandas, optional pyarrow). Uses os/os.path style.
import os, sys, argparse, glob, time, tempfile, random
import concurrent.futures as cf
import pandas as pd
import re 

# ---------- Optional Rich UI ----------
try:
    from rich.console import Console
    from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn
    RICH = True
    console = Console()
except Exception:
    RICH = False

# ---------- Heuristics ----------
CANDIDATES = {
    "clinical": [
        "sample_id", "patient_id", "case_id", "submitter_id", "barcode", "bcr_patient_barcode"
    ],
    "expression": [
        "sample_id", "barcode", "aliquot_id", "case_id", "sample", "patient_id", "tumor_sample_barcode"
    ],
    "mutations": [
        "tumor_sample_barcode", "sample_id", "barcode", "tumor_sample", "sample", "aliquot_id"
    ],
}
GENE_COLS = {"gene", "hugo_symbol", "ensembl", "gene_symbol", "entrez", "feature", "id", "symbol", "geneid"}

# Recognize common accession namespaces (lightweight regexes)
ACC_PATTERNS = {
    "TCGA": re.compile(r"TCGA-[A-Z0-9]{2}-[A-Z0-9]{4}-\d{2}[A-Z](?:-[A-Z0-9-]+)?", re.I),
    "GEO":  re.compile(r"\bGSE\d+|\bGSM\d+", re.I),
    "SRA":  re.compile(r"\bSRR\d+|\bSRS\d+|\bSRX\d+|\bPRJNA\d+", re.I),
    "ENA":  re.compile(r"\bERR\d+|\bERX\d+|\bERP\d+", re.I),
    "PRIDE":re.compile(r"\bPXD\d{5}", re.I),
    "EGA":  re.compile(r"\bEG[A-Z]\w+", re.I),
    "BioS": re.compile(r"\bSAMN?\d+", re.I),
}

# Domain vocab — tight, high-signal terms only
BREAST_VOCAB = {
    # subtypes & biomarkers
    "breast cancer","brca","tnbc","triple-negative","her2","er+","pr+","luminal a","basal-like",
    # canonical cell lines / models
    "mda-mb-231","mcf-7","t47d","bt-474","sk-br-3","hs578t","zr-75-1","sum149","4t1","pdx","xenograft",
    # therapies/targets often paired with nano-delivery
    "trastuzumab","tamoxifen","fulvestrant","palbociclib","alpelisib","pi3k","cdk4/6","esr1",
}

NANO_VOCAB = {
    # particle families
    "nanoparticle","nanomedicine","liposome","lnp","exosome","micelle","dendrimer","plga","peg","pegylated",
    "spion","iron oxide","gold nanoparticle","aunp","silver nanoparticle","agnp","quantum dot","qd",
    "carbon nanotube","cnt","graphene","mof","silica","sio2","chitosan","albumin","nanocapsule",
    # phys-chem & delivery
    "hydrodynamic diameter","dls","zeta potential","pdi","epr effect","biodistribution","pharmacokinetics",
    # assays & readouts
    "cell uptake","ic50","mtt","cck-8","annexin v","caspase-3","ros","flow cytometry","confocal","tem","sem","icp-ms",
}


GENE_COLS = {"gene","hugo_symbol","ensembl","gene_symbol","entrez","feature","id","symbol","geneid","gene_name","hgnc_symbol","ensembl_gene_id","entrez_id","hugo_symbol"}
def _lc_set(s):
    return {x.lower() for x in s}

BREAST_VOCAB_LC = _lc_set(BREAST_VOCAB)
NANO_VOCAB_LC   = _lc_set(NANO_VOCAB)

def tag_header(columns):
    """
    Tiny classifier from header text; returns tags like {'breast','nano'} for quick triage.
    Use with CSV/TSV headers or Parquet schema (cheap, no row reads).
    """
    text = " ".join(map(str, columns)).lower()
    tags = set()
    if any(k in text for k in BREAST_VOCAB_LC): tags.add("breast")
    if any(k in text for k in NANO_VOCAB_LC):   tags.add("nano")
    # add a hint if accessions are present in headers (rare but useful)
    if any(p.search(text) for p in ACC_PATTERNS.values()): tags.add("accession")
    return tags


# ---------- Parquet schema helper (optional) ----------
def parquet_columns(path):
    try:
        import pyarrow.parquet as pq
        return list(pq.ParquetFile(path).schema.names)
    except Exception:
        # Fallback: read minimal (may be heavy; used rarely)
        try:
            df = pd.read_parquet(path, engine=None)
            return list(df.columns)
        except Exception:
            return []

# ---------- Path resolution ----------
def iter_paths(spec):
    """Accepts file/dir/glob/semicolon list. Yields files sorted."""
    if not spec: return []
    out = []
    for token in str(spec).split(";"):
        token = token.strip()
        if not token: continue
        if os.path.isdir(token):
            for ext in ("*.csv","*.tsv","*.parquet"):
                out += glob.glob(os.path.join(token, "**", ext), recursive=True)
        else:
            out += glob.glob(token, recursive=True) if any(ch in token for ch in "*?[]") else ([token] if os.path.exists(token) else [])
    return sorted({os.path.normpath(p) for p in out})

# ---------- Robust CSV/TSV reader: header-only or single column ----------
def _read_csv_header(path):
    try:
        # engine='python' allows sep=None sniffing and weird delimiters
        df = pd.read_csv(path, sep=None, engine="python", nrows=0)
        return list(df.columns)
    except Exception:
        return []

def _read_csv_column(path, col):
    try:
        s = pd.read_csv(path, sep=None, engine="python", usecols=[col], dtype="string")[col]
        return s.astype(str).tolist()
    except Exception:
        return []

# ---------- Robust Parquet reader: single column ----------
def _read_parquet_column(path, col):
    try:
        return pd.read_parquet(path, columns=[col], engine=None)[col].astype(str).tolist()
    except Exception:
        # last resort: full read (avoid when possible)
        try:
            return pd.read_parquet(path, engine=None)[col].astype(str).tolist()
        except Exception:
            return []

# ---------- ID extraction per file ----------
def normalize_id(x: str) -> str:
    return str(x).strip().upper().replace(" ", "").replace("\t","")

def pick_id_column(columns, role):
    cols_l = [c.lower() for c in columns]
    # exact or contains match by priority
    for cand in CANDIDATES.get(role, []):
        for c in columns:
            cl = c.lower()
            if cl == cand or cand in cl:
                return c
    return None

def extract_ids_from_file(path, role):
    # retry small backoff for AV/locks on Windows
    for attempt in range(3):
        try:
            ext = os.path.splitext(path)[1].lower()
            if ext in (".csv",".tsv",".txt"):
                cols = _read_csv_header(path)
                if not cols: return set(), f"no_header"
                id_col = pick_id_column(cols, role)
                # Wide expression? columns are sample IDs (minus gene columns)
                if not id_col and role == "expression" and len(cols) >= 10:
                    ids = [c for c in cols if c.lower() not in GENE_COLS]
                    return {normalize_id(i) for i in ids}, "wide_header"
                if id_col:
                    vals = _read_csv_column(path, id_col)
                    return {normalize_id(v) for v in vals if v and v != "NAN"}, f"col:{id_col}"
                return set(), "no_match"
            elif ext == ".parquet":
                cols = parquet_columns(path)
                if not cols: return set(), "no_header"
                id_col = pick_id_column(cols, role)
                if not id_col and role == "expression" and len(cols) >= 10:
                    ids = [c for c in cols if c.lower() not in GENE_COLS]
                    return {normalize_id(i) for i in ids}, "wide_header"
                if id_col:
                    vals = _read_parquet_column(path, id_col)
                    return {normalize_id(v) for v in vals if v and v != "NAN"}, f"col:{id_col}"
                return set(), "no_match"
            else:
                return set(), "skip_ext"
        except Exception as e:
            if attempt == 2: return set(), f"error:{e.__class__.__name__}"
            time.sleep(0.2 + random.random()*0.4)
    return set(), "error:unknown"

# ---------- Aggregate per role with concurrency ----------
def collect_ids(paths, role, workers):
    ids, notes = set(), {}
    if RICH:
        task_total = len(paths)
        with Progress(
            TextColumn(f"[bold]{role}"),
            BarColumn(),
            TextColumn("{task.completed}/{task.total}"),
            TimeRemainingColumn(),
            console=console, transient=True
        ) as prog:
            t = prog.add_task(f"Scanning {role}", total=task_total)
            with cf.ThreadPoolExecutor(max_workers=max(1,workers)) as ex:
                for p, (s, note) in zip(paths, ex.map(lambda q: extract_ids_from_file(q, role), paths)):
                    ids |= s
                    notes[note] = notes.get(note, 0) + 1
                    prog.advance(t)
    else:
        print(f"[INFO] {role}: {len(paths)} files")
        with cf.ThreadPoolExecutor(max_workers=max(1,workers)) as ex:
            for p, (s, note) in zip(paths, ex.map(lambda q: extract_ids_from_file(q, role), paths)):
                ids |= s
                notes[note] = notes.get(note, 0) + 1
    return ids, notes

# ---------- Report + Atomic write ----------
def atomic_to_csv(df, out_path):
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".tmp_idlink_", suffix=".csv", dir=os.path.dirname(out_path) or ".")
    os.close(fd)
    try:
        df.to_csv(tmp, index=False)
        os.replace(tmp, out_path)
    except Exception:
        try: os.remove(tmp)
        except Exception: pass
        raise

def main():
    ap = argparse.ArgumentParser(description="Audit ID linkage across clinical/expression/mutations (GPT-5 Thinking).")
    ap.add_argument("--clinical",   required=True, help="File/dir/glob/;-list for clinical inputs")
    ap.add_argument("--expression", required=True, help="File/dir/glob/;-list for expression inputs")
    ap.add_argument("--mutations",  required=True, help="File/dir/glob/;-list for mutation inputs")
    ap.add_argument("--workers", type=int, default=8, help="Parallel file readers")
    ap.add_argument("--out", default="data/02_interim/id_linkage_report.csv", help="CSV output path")
    args = ap.parse_args()

    groups = {
        "clinical":   iter_paths(args.clinical),
        "expression": iter_paths(args.expression),
        "mutations":  iter_paths(args.mutations),
    }
    if not any(groups.values()):
        print("[ERR ] No input files found. Check paths/globs.")
        sys.exit(2)

    if RICH: console.rule("[bold cyan]ID Linkage Audit (GPT-5 Thinking)")
    all_ids, notes = {}, {}
    for role, paths in groups.items():
        ids, n = collect_ids(paths, role, args.workers)
        all_ids[role] = ids
        notes[role] = n
        print(f"[INFO] {role}: {len(ids)} unique IDs from {len(paths)} files | notes={dict(sorted(n.items()))}")

    # Pairwise overlaps
    names = list(all_ids.keys())
    rows = []
    for i in range(len(names)):
        for j in range(i+1,len(names)):
            a, b = names[i], names[j]
            A, B = all_ids[a], all_ids[b]
            inter = A & B
            denom = max(len(A), 1)
            pctA = 100.0*len(inter)/denom
            rows.append({"A":a,"B":b,"Overlap":len(inter),"Pct_vs_A":round(pctA,1),"A_total":len(A),"B_total":len(B)})
            print(f"[CHECK] {a} ↔ {b}: overlap={len(inter)} ({pctA:.1f}% of {a})")
    common = set.intersection(*(all_ids[k] for k in names if isinstance(all_ids[k], set))) if all(all_ids[k] is not None for k in names) else set()
    print(f"[SUMMARY] Common IDs across all: {len(common)}")

    # Save
    df = pd.DataFrame(rows, columns=["A","B","Overlap","Pct_vs_A","A_total","B_total"])
    atomic_to_csv(df, args.out)
    print(f"[DONE] Report → {args.out}")

    # Hints when zero overlap
    if all(r["Overlap"] == 0 for r in rows):
        print("[HINT] Zero overlaps detected. Likely causes:")
        print("       • Mismatched ID namespaces (e.g., Tumor_Sample_Barcode vs Case_ID).")
        print("       • Expression wide-matrix columns not matching clinical submitter IDs.")
        print("       • Mixed cohorts; missing crosswalk table. Build sample↔case↔patient map and retry.")

if __name__ == "__main__":
    main()
