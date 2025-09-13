#!/usr/bin/env python3
# scripts/build_crosswalk_idmap.py
# Crosswalk TCGA patient/sample barcodes across clinical/expression/mutations.
# Minimal deps: pandas (pyarrow optional). Windows-safe, idempotent.

import os, sys, argparse, glob, tempfile, time, random, re
import concurrent.futures as cf
import pandas as pd

# -------- Optional progress UI (kept optional; script works without it) --------
try:
    from rich.console import Console
    from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn
    RICH = True
    console = Console()
except Exception:
    RICH = False

# -------- Heuristics (conservative to avoid false positives) --------
GENE_COLS = {
    "gene","hugo_symbol","ensembl","gene_symbol","entrez","feature","id","symbol","geneid",
    "gene_name","hgnc_symbol","ensembl_gene_id","entrez_id"
}
CANDIDATES = {
    "clinical":  ["bcr_patient_barcode","patient_barcode","patient_id","case_id","submitter_id",
                  "case_submitter_id","subject_id","participant_id","barcode","sample_id"],
    "expression":["sample_id","barcode","aliquot_id","case_id","sample","patient_id",
                  "tumor_sample_barcode","sample_barcode","aliquot_barcode","submitter_id",
                  "case_submitter_id","rna_sample_id","rna_sample_barcode"],
    "mutations": ["tumor_sample_barcode","sample_barcode","sample_id","barcode","tumor_sample",
                  "submitter_id","case_submitter_id"]
}
# TCGA patterns (patient & sample). No capture-group use in pandas to avoid warnings.
TCGA_PAT  = re.compile(r"\bTCGA-[A-Z0-9]{2}-[A-Z0-9]{4}\b", re.I)
TCGA_SAMP = re.compile(r"\bTCGA-[A-Z0-9]{2}-[A-Z0-9]{4}-[0-9]{2}[A-Z]\b", re.I)

# -------- Path expansion (dir / glob / ; list). No Path().glob absolute pitfalls --------
def iter_paths(spec):
    if not spec: return []
    out = []
    for token in str(spec).split(";"):
        s = token.strip().replace("“", '"').replace("”", '"').strip('"').strip("'")
        if not s: continue
        s = os.path.expandvars(os.path.expanduser(s))
        if os.path.isdir(s):
            for ext in ("*.csv","*.tsv","*.parquet"):
                out += glob.glob(os.path.join(s, "**", ext), recursive=True)
        elif any(ch in s for ch in "*?[]"):
            out += glob.glob(s, recursive=True)
        elif os.path.exists(s):
            out.append(s)
    # normalize + dedupe existing only
    seen, out2 = set(), []
    for p in out:
        try:
            rp = os.path.normpath(os.path.abspath(p))
            if os.path.exists(rp) and rp not in seen:
                seen.add(rp); out2.append(rp)
        except Exception:
            continue
    return sorted(out2)

# -------- Lightweight readers --------
def _read_csv_header(path):
    try:
        return list(pd.read_csv(path, sep=None, engine="python", nrows=0).columns)
    except Exception:
        return []

def _read_csv_sample(path, n=400):
    try:
        return pd.read_csv(path, sep=None, engine="python", nrows=n, dtype="string")
    except Exception:
        return pd.DataFrame()

def _read_csv_col(path, col):
    try:
        s = pd.read_csv(path, sep=None, engine="python", usecols=[col], dtype="string")[col]
        return s.astype(str).tolist()
    except Exception:
        return []

def parquet_columns(path):
    try:
        import pyarrow.parquet as pq
        return list(pq.ParquetFile(path).schema.names)
    except Exception:
        try:
            return list(pd.read_parquet(path, engine=None, nrows=0).columns)
        except Exception:
            return []

def _read_parquet_col(path, col):
    try:
        return pd.read_parquet(path, columns=[col], engine=None)[col].astype(str).tolist()
    except Exception:
        try:
            return pd.read_parquet(path, engine=None)[col].astype(str).tolist()
        except Exception:
            return []

# -------- ID detection --------
def pick_id_column(columns, role):
    for cand in CANDIDATES.get(role, []):
        for c in columns:
            cl = c.lower()
            if cl == cand or cand in cl:
                return c
    return None

def sniff_idcol_by_values_csv(path):
    df = _read_csv_sample(path, n=500)
    if df.empty: return None
    best, hits = None, -1
    for c in df.columns:
        col = pd.Series(df[c], dtype="string").fillna("")
        # Count via direct regex search (no .str.contains and no capture-group warnings)
        cnt = int(col.apply(lambda v: bool(TCGA_SAMP.search(str(v))) or bool(TCGA_PAT.search(str(v)))).sum())
        if cnt > hits:
            best, hits = c, cnt
    return best if hits > 0 else None

def derive_tcga_keys(value):
    s = str(value)
    mS = TCGA_SAMP.search(s)
    mP = TCGA_PAT.search(s)
    sample  = mS.group(0) if mS else None
    patient = mP.group(0) if mP else (None if not sample else "-".join(sample.split("-")[0:3]))
    return patient, sample

# -------- Per-file extractor with retries (Windows AV/file-lock tolerant) --------
def extract_ids_from_file(path, role):
    for attempt in range(3):
        try:
            ext = os.path.splitext(path)[1].lower()
            if ext in (".csv",".tsv",".txt"):
                cols = _read_csv_header(path)
                if not cols: return [], "no_header"
                idcol = pick_id_column(cols, role) or sniff_idcol_by_values_csv(path)
                # Wide expression: treat column names (minus gene cols) as sample IDs
                if not idcol and role == "expression" and len(cols) >= 10:
                    ids = [c for c in cols if c.lower() not in GENE_COLS]
                    return ids, "wide_header"
                if not idcol: return [], "no_match"
                return _read_csv_col(path, idcol), f"col:{idcol}"
            elif ext == ".parquet":
                cols = parquet_columns(path)
                if not cols: return [], "no_header"
                idcol = pick_id_column(cols, role)
                if not idcol and role == "expression" and len(cols) >= 10:
                    ids = [c for c in cols if c.lower() not in GENE_COLS]
                    return ids, "wide_header"
                if not idcol: return [], "no_match"
                return _read_parquet_col(path, idcol), f"col:{idcol}"
            else:
                return [], "skip_ext"
        except Exception as e:
            if attempt == 2: return [], f"error:{e.__class__.__name__}"
            time.sleep(0.2 + random.random()*0.4)

# -------- Worker (assignable) --------
def worker(paths, role):
    out, notes = [], {}
    for p in paths:
        vals, note = extract_ids_from_file(p, role)
        notes[note] = notes.get(note, 0) + 1
        for v in vals:
            patient, sample = derive_tcga_keys(v)
            if patient or sample:
                out.append((role, os.path.basename(p), patient, sample, str(v)))
    return out, notes

# -------- Atomic write --------
def atomic_to_csv(df, out_path):
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".tmp_cross_", suffix=".csv", dir=os.path.dirname(out_path) or ".")
    os.close(fd)
    try:
        df.to_csv(tmp, index=False)
        os.replace(tmp, out_path)
    except Exception:
        try: os.remove(tmp)
        except Exception: pass
        raise

# -------- Main --------
def main():
    ap = argparse.ArgumentParser(description="Build TCGA patient/sample crosswalk from clinical/expression/mutations.")
    ap.add_argument("--clinical",   required=True, help="File/dir/glob/;-list")
    ap.add_argument("--expression", required=True, help="File/dir/glob/;-list")
    ap.add_argument("--mutations",  required=True, help="File/dir/glob/;-list")
    ap.add_argument("--workers", type=int, default=12, help="Parallel file readers")
    ap.add_argument("--out", default="data/02_interim/id_crosswalk.csv", help="CSV output path")
    args = ap.parse_args()

    groups = {
        "clinical":   iter_paths(args.clinical),
        "expression": iter_paths(args.expression),
        "mutations":  iter_paths(args.mutations),
    }
    if not any(groups.values()):
        print("[ERR ] No inputs found. Check paths/globs."); sys.exit(2)

    rows, notes_all = [], {}
    for role, paths in groups.items():
        if not paths:
            print(f"[INFO] {role}: no files")
            continue
        if RICH:
            with Progress(
                TextColumn(f"[bold]{role}"),
                BarColumn(),
                TextColumn("{task.completed}/{task.total}"),
                TimeRemainingColumn(),
                console=console, transient=True
            ) as prog:
                t = prog.add_task(f"scan {role}", total=len(paths))
                # chunk the file list so each batch advances the bar deterministically
                w = max(1, args.workers)
                stride = max(1, len(paths)//w)
                batches = [paths[i:i+stride] for i in range(0, len(paths), stride)]
                with cf.ThreadPoolExecutor(max_workers=w) as ex:
                    results = ex.map(lambda b: worker(b, role), batches)
                    for b, res in zip(batches, results):
                        out, notes = res
                        rows.extend(out)
                        for k, v in notes.items():
                            notes_all[(role, k)] = notes_all.get((role, k), 0) + v
                        prog.advance(t, advance=len(b))
        else:
            print(f"[INFO] {role}: {len(paths)} files")
            with cf.ThreadPoolExecutor(max_workers=max(1, args.workers)) as ex:
                results = ex.map(lambda p: worker([p], role), paths)
                for out, notes in results:
                    rows.extend(out)
                    for k, v in notes.items():
                        notes_all[(role, k)] = notes_all.get((role, k), 0) + v

    if not rows:
        print("[ERR ] No TCGA-like IDs discovered in provided inputs."); sys.exit(3)

    df = pd.DataFrame(rows, columns=["role","source_file","patient_barcode","sample_barcode","id_raw"])

    def _uniq_sorted(seq, limit=None):
        vals = sorted({str(x) for x in seq if pd.notna(x) and str(x).strip()})
        return ",".join(vals if limit is None else vals[:limit])

    agg = (
        df.groupby(["patient_barcode","sample_barcode"], dropna=False)
          .agg(
              roles   = ("role",        _uniq_sorted),
              sources = ("source_file", _uniq_sorted),
              raw_ids = ("id_raw",      lambda s: _uniq_sorted(s, limit=5)),
          ).reset_index()
    )
    for r in ("clinical","expression","mutations"):
        agg[f"in_{r}"] = agg["roles"].str.contains(r).fillna(False)

    atomic_to_csv(agg, args.out)
    print(f"[DONE] Crosswalk → {args.out}  rows={len(agg)}")
    print(f"[SUMMARY] with_patient_only={(agg['patient_barcode'].notna() & agg['sample_barcode'].isna()).sum()} | "
          f"with_sample={agg['sample_barcode'].notna().sum()} | both={(agg['patient_barcode'].notna() & agg['sample_barcode'].notna()).sum()}")

    # compact notes
    per_role = {}
    for (role, k), v in notes_all.items():
        per_role.setdefault(role, []).append(f"{k}:{v}")
    for role, items in per_role.items():
        print(f"[INFO] {role} notes: "+"; ".join(sorted(items)))

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[WARN] Interrupted by user."); sys.exit(130)
