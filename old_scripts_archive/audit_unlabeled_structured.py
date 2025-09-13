
"""
Audit structured_annotated folders and enrich the unlabeled catalog.
- Detect ID columns, uniqueness, example values
- Classify files: table / wide_matrix / caselist / metadata / empty
- Auto-rescue case lists:
    * cBioPortal-style (case_list_ids:) in .txt
    * CSV/TSV with one row and a cell containing space/comma/semicolon-separated IDs
    * one-ID-per-line .txt
- Outputs:
  - unlabeled_catalog_plus.csv
  - rescued_caselist_index.parquet/csv  (if any)
"""

from __future__ import annotations
import argparse, os, glob, re, csv
from pathlib import Path
from typing import List, Dict, Optional, Tuple

import polars as pl

ID_CANDIDATES = [
    "sample_id","model_id","tumor_sample_barcode","tsb","patient_id",
    "case_id","depmap_id","sample","model","cell_id","cell_line","specimen_id"
]
META_HINTS = ("meta_", "resource_", "assay_information")
CASELIST_HINTS = ("case_lists", "cases_", "caselist")
WIDE_MATRIX_ROW_HINTS = ("hugo_symbol","gene_symbol","gene","symbol","entrez","ensembl")
TCGA_LIKE = re.compile(r"^TCGA-[A-Z0-9]{2}-[A-Z0-9]{4}", re.I)
ID_TOKEN = re.compile(r"^[A-Za-z0-9._:-]+$")

def safe_mkdir(p:str): os.makedirs(p, exist_ok=True)
def relpath(p, root): 
    try: return os.path.relpath(p, root).replace("\\","/")
    except ValueError: return p.replace("\\","/")

def discover_files(root: str) -> List[str]:
    pats = ("**/*.parquet","**/*.csv","**/*.tsv","**/*.txt")
    files: List[str] = []
    for pat in pats:
        files.extend(glob.glob(os.path.join(root, pat), recursive=True))
    return sorted(set(files))

def read_table(path: str) -> Optional[pl.DataFrame]:
    ext = Path(path).suffix.lower()
    try:
        if ext == ".parquet":
            return pl.read_parquet(path)
        if ext == ".tsv":
            return pl.read_csv(path, separator="\t", infer_schema_length=5000)
        if ext == ".csv":
            return pl.read_csv(path, infer_schema_length=5000)
        return None
    except Exception:
        return None

def read_text(path: str) -> str:
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            return f.read()
    except Exception:
        return ""

def classify_kind(path: str, df: Optional[pl.DataFrame], text: Optional[str]) -> str:
    lpath = path.lower()
    if any(h in lpath for h in META_HINTS):
        return "metadata"
    if any(h in lpath for h in CASELIST_HINTS):
        return "caselist"
    if df is None:
        return "empty" if not text or not text.strip() else "text"
    if df.is_empty():
        return "empty"
    # table vs wide_matrix
    cols_lower = [c.lower() for c in df.columns]
    if any(h in cols_lower for h in WIDE_MATRIX_ROW_HINTS):
        # gene matrix; check if *columns* look like sample IDs
        if any(TCGA_LIKE.match(c) for c in df.columns):
            return "wide_matrix"
    # if huge #columns and few rows, likely wide
    if df.width > 1000 and df.height < 500:
        return "wide_matrix"
    return "table"

def id_diagnostics(df: pl.DataFrame) -> Tuple[List[str], Dict[str, Dict[str, object]]]:
    cand = []
    diag: Dict[str, Dict[str, object]] = {}
    cols_map = {c.lower(): c for c in df.columns}
    # include heuristic matches too
    heur = [c for c in df.columns if any(tok in c.lower() for tok in ("sample","barcode","patient","model","depmap","case","specimen","cell"))]
    for k in ID_CANDIDATES + heur:
        if k.lower() in cols_map:
            col = cols_map[k.lower()]
            if col in cand: 
                continue
            s = df[col]
            nn = int(s.len() - s.null_count())
            nu = int(s.n_unique())
            ex = [str(x) for x in s.cast(pl.Utf8).drop_nulls().head(3).to_list()]
            diag[col] = {
                "non_null": nn,
                "n_unique": nu,
                "unique_pct": round(100.0 * nu / max(1, nn), 2),
                "examples": "|".join(ex)
            }
            cand.append(col)
    return cand, diag

def parse_caselist_from_text(text: str) -> List[str]:
    # cBioPortal case_list_ids: ...
    ids = []
    for ln in text.splitlines():
        if ln.lower().startswith("case_list_ids:"):
            _, v = ln.split(":", 1)
            ids = [t for t in re.split(r"[,\s;]+", v.strip()) if t]
            break
    # fallback: one-ID-per-line
    if not ids:
        ids = [ln.strip() for ln in text.splitlines() if ln.strip() and ID_TOKEN.match(ln.strip()) and ":" not in ln]
    # require at least a few
    return ids if len(ids) >= 3 else []

def parse_caselist_from_csv(df: pl.DataFrame) -> List[str]:
    # Heuristic: single row, some cell contains a huge string of IDs separated by space/comma/semicolon
    if df.height == 1:
        for c in df.columns:
            cell = df[c][0]
            if cell is None:
                continue
            s = str(cell)
            parts = [t for t in re.split(r"[,\s;]+", s.strip()) if t]
            # drop obvious headers
            parts = [p for p in parts if ":" not in p]
            if len(parts) >= 3:
                return parts
    # Also accept a column that itself looks like a list of IDs
    for c in df.columns:
        s = df[c].cast(pl.Utf8).drop_nulls()
        if s.is_empty():
            continue
        # sample a few rows
        vals = s.head(10).to_list()
        hits = [v for v in vals if ID_TOKEN.match(v)]
        if len(hits) >= max(3, int(0.6 * len(vals))):
            # treat as explicit ID column in rows
            return s.to_list()
    return []

def main():
    ap = argparse.ArgumentParser(description="Enrich unlabeled catalog and auto-rescue caselists/ID-lists.")
    ap.add_argument("--in-old", default="", help="older structured_annotated dir (optional)")
    ap.add_argument("--in-v2",  default="", help="v2 structured_annotated dir (optional)")
    ap.add_argument("--out",    required=True, help="output folder")
    args = ap.parse_args()

    if not args.in_old and not args.in_v2:
        print("Provide at least one input folder."); return
    safe_mkdir(args.out)

    roots = [p for p in (args.in_v2, args.in_old) if p]
    rows = []
    rescued_rows = []

    for root in roots:
        files = discover_files(root)
        for p in files:
            ext = Path(p).suffix.lower()
            rel = relpath(p, root)
            tag = "v2" if root == args.in_v2 else "old"

            text = read_text(p) if ext == ".txt" else ""
            df = read_table(p) if ext in (".csv",".tsv",".parquet") else None

            kind = classify_kind(p, df, text)

            # default diagnostics
            d = {
                "file": p, "relpath": rel, "tag": tag, "kind": kind,
                "rows": int(df.height) if df is not None else (len(text.splitlines()) if text else 0),
                "cols": int(df.width) if df is not None else 0,
                "id_candidates": "", "id_details": "", "suggestion": "", "notes": ""
            }

            # ID diagnostics (tables only)
            if df is not None and not df.is_empty() and kind in ("table","wide_matrix"):
                cand, diag = id_diagnostics(df)
                d["id_candidates"] = "|".join(cand) if cand else ""
                # compact JSON-ish dict for human reading
                dd = []
                for c in cand:
                    m = diag.get(c, {})
                    dd.append(f"{c}[nn={m.get('non_null')},nu={m.get('n_unique')},uniq%={m.get('unique_pct')},ex={m.get('examples')}]")
                d["id_details"] = " ; ".join(dd)

                if not cand:
                    if kind == "wide_matrix":
                        d["suggestion"] = "wide_matrix: sample IDs likely in column headers; skip for master_structured; process via quantitative pipeline"
                    else:
                        d["suggestion"] = "no obvious ID col; check metadata/crosswalk, or map via doc/sample ID heuristics"
                else:
                    # Promote the strongest candidate
                    best = max(cand, key=lambda c: diag[c]["unique_pct"])
                    d["suggestion"] = f"use_id:{best}"

            # Auto-rescue caselists
            if kind in ("caselist","text"):
                ids = []
                if text:
                    ids = parse_caselist_from_text(text)
                if not ids and df is not None:
                    # some case_lists got saved as CSV with one row
                    ids = parse_caselist_from_csv(df)
                if ids:
                    label = Path(p).stem
                    for sid in ids:
                        rescued_rows.append({
                            "sample_id": sid,
                            "case_list_id": label, "case_list_name": label, "case_list_category": "unknown",
                            "source_relpath": rel, "source_file": Path(p).name, "source_dir": str(Path(rel).parent)
                        })
                    d["suggestion"] = f"rescued_caselist:{label} n={len(ids)}"
                elif kind == "caselist":
                    d["suggestion"] = "caselist_detected_but_unparsed"

            # metadata
            if kind == "metadata":
                d["suggestion"] = "metadata: keep for provenance, not merged into master"

            rows.append(d)

    cat = pl.DataFrame(rows) if rows else pl.DataFrame({"file":[]})
    cat_out = os.path.join(args.out, "unlabeled_catalog_plus.csv")
    cat.write_csv(cat_out)

    if rescued_rows:
        idx = pl.DataFrame(rescued_rows)
        idx_out_pq = os.path.join(args.out, "rescued_caselist_index.parquet")
        idx_out_csv = os.path.join(args.out, "rescued_caselist_index.csv")
        idx.write_parquet(idx_out_pq); idx.write_csv(idx_out_csv)
        print(f"[OK] Rescued caselist rows: {idx.height}")

    print(f"[OK] Wrote {cat_out}")

if __name__ == "__main__":
    main()
