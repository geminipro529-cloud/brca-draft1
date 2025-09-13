#!/usr/bin/env python3
"""
diagnose_repository.py — maximum transparency diagnostics (safe & low-memory)

What it does (high level)
-------------------------
- Walks a root folder (default: data/01_raw) and inventories files
- Streams/samples to avoid OOM: CSV/TSV (nrows=0 + small chunks), Parquet (metadata + tiny batches),
  XLSX (read-only), TXT/MD (first N bytes), PDF (first pages)
- Detects:
  * Likely codebooks/dictionaries (by name + content)
  * Patient/sample/model ID *columns* and *values* (TCGA/UUID/DepMap/ACH/SIDM by default)
  * Header-only ID presence in massive matrices (fast)
  * “Numeric whale” tables (low text ratio → suggest exclude)
  * Encodings (best-effort; chardet optional)
- Produces in OUT dir:
  * report.md                          — human summary + recommendations
  * inventory.csv                      — one row per file (size, ext, flags)
  * ext_counts.csv                     — counts & total bytes by extension
  * largest_files.csv                  — top-N by size
  * headers_samples.jsonl              — headers per file (CSV/TSV/Parquet/XLSX)
  * column_signals.jsonl               — per-file column-level ID hints & textiness
  * id_hits.jsonl                      — sampled ID-like VALUES per file
  * codebook_candidates.csv            — files likely to be codebooks
  * suggested_include_globs.txt        — patterns for pipeline include
  * suggested_exclude_globs.txt        — patterns for pipeline exclude
  * errors.log                         — any issues encountered
  * bundle.zip                         — everything zipped for sharing

Optional deps (auto-detected at runtime; safe to run w/o them):
  pip install tqdm pyarrow openpyxl chardet PyMuPDF pdfminer.six

Usage (Windows PowerShell)
--------------------------
# From your repo root (D:\breast_cancer_project_root)
$env:OMP_NUM_THREADS="1"; $env:MKL_NUM_THREADS="1"; $env:OPENBLAS_NUM_THREADS="1"

python scripts\\diagnose_repository.py `
  --root data\\01_raw `
  --out _diagnostics_full `
  --max-bytes 200000 `
  --csv-sample-rows 2000 `
  --parquet-batch-rows 4096 `
  --pdf-pages 4 `
  --progress --yes

Quick (faster, shallower):
python scripts\\diagnose_repository.py --root data\\01_raw --out _diag_quick --progress --yes --quick
"""
from __future__ import annotations
import argparse, csv, io, json, math, os, re, sys, traceback, fnmatch, zipfile, platform, hashlib
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Iterable, Iterator

# ---------- Optional libs ----------
try:
    from tqdm.auto import tqdm
except Exception:
    tqdm = None

# chardet for sniffing encodings (optional)
try:
    import chardet
except Exception:
    chardet = None

# ---------- Heuristics & regex ----------
CODEBOOK_NAME_HINT = re.compile(r"(codebook|dictionary|dict|mapping|lookup|legend|terms?|synonym|gazetteer|ontology)", re.I)

ID_PATTERNS = {
    "tcga_patient": re.compile(r"(TCGA-[A-Z0-9]{2}-[A-Z0-9]{4})", re.I),
    "uuid":         re.compile(r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b", re.I),
    "depmap_ach":   re.compile(r"\bACH[-_ ]?\d{4,6}\b", re.I),
    "sidm":         re.compile(r"\bSIDM\d{3,6}\b", re.I),
}

ID_LIKE_COLNAME_HINTS = [
    "patient","patient_id","case","case_id","subject","participant","donor","individual",
    "sample","sample_id","tumor_sample_barcode","tcga_barcode","barcode",
    "aliquot","aliquot_id","specimen","specimen_id",
    "model","model_id","cell_line","cell_line_id","depmap_id","ach",
    "uuid","gdc","case_uuid"
]

PAIR_RE = re.compile(r"^\s*([A-Za-z][A-Za-z0-9._/-]{1,24})\s*(?:=|:|-{1,2}|–|—)\s+(.{2,200})\s*$")
NUMERIC_TOKEN_RE = re.compile(r"^[\d.+-eENaNinfINF]+$")

TEXT_EXT = {".txt",".md",".rtf",".text"}
TABLE_EXT = {".csv",".tsv",".parquet",".xlsx",".xls"}
PDF_EXT = {".pdf"}

# ---------- Small utils ----------
def ensure_dir(p: Path): p.mkdir(parents=True, exist_ok=True)

def log_append(path: Path, msg: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as fh:
        fh.write(msg.rstrip() + "\n")

def sizeof_fmt(num: int) -> str:
    for unit in ['B','KB','MB','GB','TB']:
        if abs(num) < 1024.0: return f"{num:3.1f}{unit}"
        num /= 1024.0
    return f"{num:.1f}PB"

def sniff_encoding(path: Path, max_bytes: int = 65536) -> str:
    try:
        with open(path, "rb") as fh:
            raw = fh.read(max_bytes)
        if chardet:
            r = chardet.detect(raw)
            return r.get("encoding") or "utf-8"
        # fallback
        return "utf-8"
    except Exception:
        return "utf-8"

def safe_read_text_prefix(path: Path, max_bytes: int) -> str:
    try:
        enc = sniff_encoding(path, min(max_bytes, 65536))
        with open(path, "r", encoding=enc, errors="ignore") as fh:
            return fh.read(max_bytes)
    except Exception:
        return ""

def numeric_ratio(sample_vals: List[str]) -> float:
    if not sample_vals: return 0.0
    n = len(sample_vals)
    k = 0
    for v in sample_vals:
        s = (v or "").strip()
        if not s: 
            n -= 1
            continue
        if NUMERIC_TOKEN_RE.match(s):
            k += 1
    return (k / max(1, n))

def avg_strlen(sample_vals: List[str]) -> float:
    if not sample_vals: return 0.0
    tot = 0; cnt = 0
    for v in sample_vals:
        if v is None: continue
        s = str(v)
        tot += len(s); cnt += 1
    return tot / max(1, cnt)

def find_ids_in_text(text: str) -> Dict[str, List[str]]:
    hits: Dict[str,List[str]] = {}
    for name, rgx in ID_PATTERNS.items():
        vals = sorted(set(m.group(1) if m.groups() else m.group(0) for m in rgx.finditer(text)))
        if vals:
            hits[name] = vals[:200]  # cap
    return hits

def suggest_globs_from_path(path: Path) -> Optional[str]:
    s = str(path).replace("\\","/")
    # short heuristics
    for key in ["rnaseq_all","rnaseq_merged","vocabulary_download_v5","OmicsCNGene","OmicsExpression","SomaticMutations","GDSC2_public_raw_data","CCLE_RNAseq"]:
        if key.lower() in s.lower():
            return f"*{key}*"
    # sections with 'clinical' 'metadata' for include
    for key in ["clinical","metadata","sample","cases","patient"]:
        if key.lower() in s.lower():
            return f"*{key}*"
    return None

# ---------- Readers ----------
def csv_headers(path: Path) -> Tuple[List[str], str]:
    enc = sniff_encoding(path)
    # try pandas fast header
    try:
        import pandas as pd
        df = pd.read_csv(path, nrows=0, encoding=enc, engine="python")
        return list(df.columns), enc
    except Exception:
        # fallback: guess delimiter
        try:
            with open(path, "r", encoding=enc, errors="ignore") as fh:
                first = fh.readline()
            delim = "\t" if first.count("\t") > first.count(",") else ","
            return [c.strip() for c in first.split(delim)], enc
        except Exception:
            return [], enc

def csv_sample_rows(path: Path, usecols: Optional[List[str]], nrows: int) -> Tuple[List[Dict], Optional[List[str]]]:
    try:
        import pandas as pd
    except Exception:
        return [], None
    enc = sniff_encoding(path)
    try:
        df = pd.read_csv(path, dtype=str, encoding=enc, engine="python", nrows=nrows, usecols=usecols, on_bad_lines="skip", low_memory=True)
        return df.to_dict(orient="records"), list(df.columns)
    except Exception:
        return [], None

def parquet_schema_and_sample(path: Path, batch_rows: int = 2048) -> Tuple[List[str], Dict[str,str], List[Dict]]:
    try:
        import pyarrow.parquet as pq
    except Exception:
        return [], {}, []
    try:
        pf = pq.ParquetFile(str(path))
        cols = list(pf.schema.names)
        types = {name: str(pf.schema.field(name).type) for name in cols}
        # one small batch
        for rb in pf.iter_batches(batch_size=max(256, batch_rows), columns=None):
            df = rb.to_pandas(types_mapper=str)
            return cols, types, df.head( min(len(df), 200) ).astype(str).to_dict(orient="records")
        return cols, types, []
    except Exception:
        return [], {}, []

def xlsx_headers_and_sample(path: Path, row_limit: int = 300) -> Tuple[List[str], List[Dict]]:
    try:
        import openpyxl
    except Exception:
        return [], []
    try:
        wb = openpyxl.load_workbook(str(path), read_only=True, data_only=True)
        ws = wb.worksheets[0]
        header = None
        rows = []
        for i, row in enumerate(ws.iter_rows(values_only=True)):
            vals = [None if v is None else str(v) for v in row]
            if header is None:
                # first row with 2+ non-nulls considered header
                if sum(1 for x in vals if x not in (None, "")) >= 2:
                    header = [v if v else f"col_{j}" for j, v in enumerate(vals)]
                continue
            rows.append({header[j] if j < len(header) else f"col_{j}": vals[j] if j < len(vals) else None for j in range(len(header or []))})
            if len(rows) >= row_limit:
                break
        return header or [], rows
    except Exception:
        return [], []

def pdf_meta_and_sample_text(path: Path, max_pages: int = 3, max_chars: int = 200000) -> Tuple[int, int, Dict[str,int]]:
    tried = []
    chars = 0
    pages = 0
    stats = {"text_pages": 0, "empty_pages": 0}
    # try PyMuPDF
    try:
        import fitz
        with fitz.open(str(path)) as doc:
            pages = len(doc)
            for p in range(min(max_pages, pages)):
                txt = doc[p].get_text("text") or ""
                if txt.strip():
                    stats["text_pages"] += 1
                    chars += min(max_chars - chars, len(txt))
                else:
                    stats["empty_pages"] += 1
                if chars >= max_chars: break
            return pages, chars, stats
    except Exception as e:
        tried.append(("fitz", str(e)))
    # fallback pdfminer
    try:
        from pdfminer.high_level import extract_text
        txt = extract_text(str(path)) or ""
        pages = max_pages  # unknown; set sample count
        chars = min(max_chars, len(txt))
        stats["text_pages"] = 1 if chars > 0 else 0
        return pages, chars, stats
    except Exception as e:
        tried.append(("pdfminer", str(e)))
        return 0, 0, {"text_pages":0,"empty_pages":0}

# ---------- core diagnostics ----------
def diagnose(root: Path, outdir: Path, args):
    ensure_dir(outdir)
    err_log = outdir / "errors.log"

    # inventory
    files: List[Path] = []
    for p in root.rglob("*"):
        if not p.is_file(): continue
        # skip common virtual/env/build trash
        if any(seg.lower() in {".git","node_modules",".venv","__pycache__"} for seg in p.parts):
            continue
        files.append(p)

    # ordering & cap
    if args.order == "size_asc":
        files.sort(key=lambda p: p.stat().st_size if p.exists() else 0)
    elif args.order == "size_desc":
        files.sort(key=lambda p: -(p.stat().st_size if p.exists() else 0))
    else:
        files.sort(key=lambda p: str(p).lower())

    if args.max_files and args.max_files > 0:
        files = files[:args.max_files]

    if len(files) >= args.confirm_threshold and not args.yes:
        print(f"[PRE-FLIGHT] Selected {len(files)} files. Re-run with --yes or lower --max-files.")
        sys.exit(2)

    # outputs
    inv_csv = outdir / "inventory.csv"
    ext_counts_csv = outdir / "ext_counts.csv"
    largest_csv = outdir / "largest_files.csv"
    headers_jsonl = outdir / "headers_samples.jsonl"
    colsignals_jsonl = outdir / "column_signals.jsonl"
    idhits_jsonl = outdir / "id_hits.jsonl"
    codebooks_csv = outdir / "codebook_candidates.csv"
    include_txt = outdir / "suggested_include_globs.txt"
    exclude_txt = outdir / "suggested_exclude_globs.txt"
    report_md = outdir / "report.md"

    # write headers
    with open(inv_csv, "w", encoding="utf-8", newline="") as fh:
        w = csv.writer(fh); w.writerow([
            "path","rel_path","ext","size_bytes","size_h","encoding_guess","is_codebook_name","is_numeric_whale","has_header_ids","has_value_ids"
        ])
    # counts
    ext_counts: Dict[str, Dict[str,int]] = {}
    largest: List[Tuple[int, str]] = []
    include_suggestions: Dict[str,int] = {}
    exclude_suggestions: Dict[str,int] = {}

    outer = tqdm(total=len(files), desc="Diagnosing files", unit="file", dynamic_ncols=True) if args.progress and tqdm else None

    # aggregate signals
    total_id_hits = 0
    codebook_rows: List[Tuple[str,int,str]] = []  # path, score, reason

    for fp in files:
        rel = str(fp.relative_to(root)) if fp.is_relative_to(root) else str(fp)
        ext = fp.suffix.lower()
        size = fp.stat().st_size if fp.exists() else 0
        sizeh = sizeof_fmt(size)
        enc = ""
        is_codebook_name = bool(CODEBOOK_NAME_HINT.search(fp.name))
        is_numeric_whale = False
        has_header_ids = False
        has_value_ids = False

        # collect ext counts
        ekey = ext or "(none)"
        rec = ext_counts.setdefault(ekey, {"count":0, "bytes":0})
        rec["count"] += 1; rec["bytes"] += size
        largest.append((size, str(fp)))

        # quick encoding sniff (for text/csv-ish)
        if ext in TEXT_EXT or ext in {".csv",".tsv",".json"}:
            enc = sniff_encoding(fp)

        # suggest globs
        g = suggest_globs_from_path(fp)
        if g:
            if any(k in (g.lower()) for k in ["rnaseq","vocabulary_download","omics","somatic","gds","ccle"]):
                exclude_suggestions[g] = exclude_suggestions.get(g,0)+1
            else:
                include_suggestions[g] = include_suggestions.get(g,0)+1

        try:
            # ---------- CSV / TSV ----------
            if ext in {".csv",".tsv"}:
                hdr, enc = csv_headers(fp)
                # write header sample
                if hdr:
                    with open(headers_jsonl, "a", encoding="utf-8") as fh:
                        fh.write(json.dumps({"path": rel, "ext": ext, "headers": hdr}, ensure_ascii=False) + "\n")
                # header ID scan
                if hdr:
                    joined = " ".join(str(x) for x in hdr[:500])
                    idhits = find_ids_in_text(joined)
                    if idhits:
                        has_header_ids = True
                        for key, vals in idhits.items():
                            with open(idhits_jsonl, "a", encoding="utf-8") as fh:
                                fh.write(json.dumps({"path": rel, "where":"header", "pattern": key, "values": vals[:50]}, ensure_ascii=False) + "\n")
                            total_id_hits += len(vals)
                # sample rows (limited)
                sample_rows, cols_eff = ([], hdr)
                if not args.quick:
                    sample_rows, cols_eff = csv_sample_rows(fp, hdr if hdr else None, args.csv_sample_rows)
                # textiness per column
                if cols_eff and sample_rows:
                    col_textiness = []
                    for c in cols_eff:
                        vals = [str(r.get(c) or "") for r in sample_rows[: min(len(sample_rows), 1000)]]
                        nr = numeric_ratio(vals)
                        av = avg_strlen(vals)
                        col_textiness.append((c, nr, av))
                        # ID values scan on sample
                        text_blob = " ".join(vals[:2000])
                        idhits2 = find_ids_in_text(text_blob)
                        if idhits2:
                            has_value_ids = True
                            for key, vals2 in idhits2.items():
                                with open(idhits_jsonl, "a", encoding="utf-8") as fh:
                                    fh.write(json.dumps({"path": rel, "where":"values", "column": c, "pattern": key, "values": vals2[:50]}, ensure_ascii=False) + "\n")
                                total_id_hits += len(vals2)
                    # write column signals
                    with open(colsignals_jsonl, "a", encoding="utf-8") as fh:
                        fh.write(json.dumps({"path": rel, "ext": ext, "columns": [
                            {"name": c, "numeric_ratio": nr, "avg_strlen": av} for (c, nr, av) in col_textiness
                        ]}, ensure_ascii=False) + "\n")
                    # whale heuristic: mostly numeric + many columns + large file
                    if size > 500_000_000 and cols_eff and sum(1 for _,nr,_ in col_textiness if nr >= 0.95) / len(col_textiness) >= 0.7:
                        is_numeric_whale = True
                        exclude_suggestions["*"+fp.name+"*"] = exclude_suggestions.get("*"+fp.name+"*",0)+1

                # codebook content detection: many code: label pairs in first bytes
                if not args.quick:
                    prefix = safe_read_text_prefix(fp, args.max_bytes)
                    lines = prefix.splitlines()[:2000]
                    code_pairs = 0
                    for ln in lines:
                        if PAIR_RE.match(ln): code_pairs += 1
                    if code_pairs >= 10 or is_codebook_name:
                        codebook_rows.append((rel, code_pairs, "pairs>=10" if code_pairs>=10 else "name_hint"))

            # ---------- Parquet ----------
            elif ext == ".parquet":
                cols, types, sample = parquet_schema_and_sample(fp, args.parquet_batch_rows)
                if cols:
                    with open(headers_jsonl, "a", encoding="utf-8") as fh:
                        fh.write(json.dumps({"path": rel, "ext": ext, "headers": cols, "types": types}, ensure_ascii=False) + "\n")
                if cols:
                    joined = " ".join(cols[:500])
                    idhits = find_ids_in_text(joined)
                    if idhits:
                        has_header_ids = True
                        for key, vals in idhits.items():
                            with open(idhits_jsonl, "a", encoding="utf-8") as fh:
                                fh.write(json.dumps({"path": rel, "where":"header", "pattern": key, "values": vals[:50]}, ensure_ascii=False) + "\n")
                            total_id_hits += len(vals)
                # sample values for IDs (light)
                if sample and not args.quick:
                    blob = " ".join(" ".join(r.values()) for r in sample[:200])
                    idhits2 = find_ids_in_text(blob)
                    if idhits2:
                        has_value_ids = True
                        for key, vals2 in idhits2.items():
                            with open(idhits_jsonl, "a", encoding="utf-8") as fh:
                                fh.write(json.dumps({"path": rel, "where":"values", "pattern": key, "values": vals2[:50]}, ensure_ascii=False) + "\n")
                            total_id_hits += len(vals2)
                # column signals (textiness proxy: treat as non-numeric unless obvious)
                if cols:
                    with open(colsignals_jsonl, "a", encoding="utf-8") as fh:
                        fh.write(json.dumps({"path": rel, "ext": ext, "columns": [
                            {"name": c, "numeric_ratio": 0.0, "avg_strlen": 0.0} for c in cols[:200]
                        ]}, ensure_ascii=False) + "\n")
                # whale heuristic: very large & many columns likely numeric matrix
                if size > 1_000_000_000 and cols and len(cols) >= 1000:
                    is_numeric_whale = True
                    exclude_suggestions["*"+fp.name+"*"] = exclude_suggestions.get("*"+fp.name+"*",0)+1

            # ---------- Excel ----------
            elif ext in {".xlsx",".xls"}:
                hdr, rows = xlsx_headers_and_sample(fp, row_limit=300 if not args.quick else 80)
                if hdr:
                    with open(headers_jsonl, "a", encoding="utf-8") as fh:
                        fh.write(json.dumps({"path": rel, "ext": ext, "headers": hdr}, ensure_ascii=False) + "\n")
                    joined = " ".join(hdr[:500])
                    idhits = find_ids_in_text(joined)
                    if idhits:
                        has_header_ids = True
                        for key, vals in idhits.items():
                            with open(idhits_jsonl, "a", encoding="utf-8") as fh:
                                fh.write(json.dumps({"path": rel, "where":"header", "pattern": key, "values": vals[:50]}, ensure_ascii=False) + "\n")
                            total_id_hits += len(vals)
                if rows and not args.quick:
                    # textiness proxy for first ~12 columns
                    colnames = hdr[:12] if hdr else [f"col_{i}" for i in range(12)]
                    col_textiness = []
                    for c in colnames:
                        vals = [str(r.get(c) or "") for r in rows[:200]]
                        col_textiness.append((c, numeric_ratio(vals), avg_strlen(vals)))
                        idhits2 = find_ids_in_text(" ".join(vals))
                        if idhits2:
                            has_value_ids = True
                            for key, vals2 in idhits2.items():
                                with open(idhits_jsonl, "a", encoding="utf-8") as fh:
                                    fh.write(json.dumps({"path": rel, "where":"values", "column": c, "pattern": key, "values": vals2[:50]}, ensure_ascii=False) + "\n")
                                total_id_hits += len(vals2)
                    with open(colsignals_jsonl, "a", encoding="utf-8") as fh:
                        fh.write(json.dumps({"path": rel, "ext": ext, "columns": [
                            {"name": c, "numeric_ratio": nr, "avg_strlen": av} for (c, nr, av) in col_textiness
                        ]}, ensure_ascii=False) + "\n")
                # codebook-like detection in the sheet sample (look for pairs)
                if not args.quick:
                    prefix = "\n".join(",".join([k for k in hdr[:50]]) for _ in range(3))
                    if is_codebook_name and hdr:
                        codebook_rows.append((rel, 1, "name_hint"))

            # ---------- PDF ----------
            elif ext in PDF_EXT:
                pages, chars, pstats = pdf_meta_and_sample_text(fp, max_pages=args.pdf_pages, max_chars=args.max_bytes)
                # if text pages == 0 → likely scanned image (note for transparency)
                if chars > 0:
                    with open(headers_jsonl, "a", encoding="utf-8") as fh:
                        fh.write(json.dumps({"path": rel, "ext": ext, "pdf_pages": pages, "text_pages": pstats.get("text_pages",0)}, ensure_ascii=False) + "\n")
                    sample = safe_read_text_prefix(fp, 1024)  # very small fallback
                    idhits = find_ids_in_text(sample)
                    if idhits:
                        has_value_ids = True
                        for key, vals in idhits.items():
                            with open(idhits_jsonl, "a", encoding="utf-8") as fh:
                                fh.write(json.dumps({"path": rel, "where":"pdf_text", "pattern": key, "values": vals[:50]}, ensure_ascii=False) + "\n")
                            total_id_hits += len(vals)

            # ---------- TEXT ----------
            elif ext in TEXT_EXT:
                prefix = safe_read_text_prefix(fp, args.max_bytes)
                if prefix:
                    idhits = find_ids_in_text(prefix)
                    if idhits:
                        has_value_ids = True
                        for key, vals in idhits.items():
                            with open(idhits_jsonl, "a", encoding="utf-8") as fh:
                                fh.write(json.dumps({"path": rel, "where":"text", "pattern": key, "values": vals[:50]}, ensure_ascii=False) + "\n")
                            total_id_hits += len(vals)
                    # codebook pairs?
                    code_pairs = 0
                    for ln in prefix.splitlines()[:2000]:
                        if PAIR_RE.match(ln): code_pairs += 1
                    if code_pairs >= 10 or is_codebook_name:
                        codebook_rows.append((rel, code_pairs, "pairs>=10" if code_pairs>=10 else "name_hint"))

            # ---------- Other ----------
            else:
                pass

            # Write inventory row
            with open(inv_csv, "a", encoding="utf-8", newline="") as fh:
                w = csv.writer(fh)
                w.writerow([str(fp), rel, ext, size, sizeh, enc, int(is_codebook_name), int(is_numeric_whale), int(has_header_ids), int(has_value_ids)])

        except Exception as e:
            log_append(err_log, f"{fp}\t{type(e).__name__}: {e}")
            log_append(err_log, traceback.format_exc())

        if outer: outer.update(1)

    if outer: outer.close()

    # ext counts
    with open(ext_counts_csv, "w", encoding="utf-8", newline="") as fh:
        w = csv.writer(fh); w.writerow(["extension","count","total_bytes","total_size"])
        for ext, d in sorted(ext_counts.items(), key=lambda kv: (-kv[1]["count"], kv[0])):
            w.writerow([ext, d["count"], d["bytes"], sizeof_fmt(d["bytes"])])

    # largest N
    largest.sort(reverse=True)
    with open(largest_csv, "w", encoding="utf-8", newline="") as fh:
        w = csv.writer(fh); w.writerow(["size_bytes","size_h","path"])
        for sz, p in largest[:200]:
            w.writerow([sz, sizeof_fmt(sz), p])

    # codebook candidates
    if codebook_rows:
        with open(codebooks_csv, "w", encoding="utf-8", newline="") as fh:
            w = csv.writer(fh); w.writerow(["path","score","reason"])
            for path, score, reason in sorted(codebook_rows, key=lambda x: (-x[1], x[0]))[:2000]:
                w.writerow([path, score, reason])

    # suggested globs
    if include_suggestions:
        with open(include_txt, "w", encoding="utf-8") as fh:
            for g, n in sorted(include_suggestions.items(), key=lambda kv: (-kv[1], kv[0])):
                fh.write(f"{g}\n")
    if exclude_suggestions:
        with open(exclude_txt, "w", encoding="utf-8") as fh:
            for g, n in sorted(exclude_suggestions.items(), key=lambda kv: (-kv[1], kv[0])):
                fh.write(f"{g}\n")

    # report
    inv_rows = sum(1 for _ in open(inv_csv, "r", encoding="utf-8", errors="ignore")) - 1
    ext_rows = sum(1 for _ in open(ext_counts_csv, "r", encoding="utf-8", errors="ignore")) - 1
    largest_rows = sum(1 for _ in open(largest_csv, "r", encoding="utf-8", errors="ignore")) - 1

    with open(report_md, "w", encoding="utf-8") as fh:
        fh.write("# Repository Diagnostics Report\n\n")
        fh.write(f"- Root: `{root}`\n")
        fh.write(f"- Files scanned: **{len(files)}**\n")
        fh.write(f"- Inventory rows: **{inv_rows}**\n")
        fh.write(f"- Extensions observed: **{ext_rows}**\n")
        fh.write(f"- Top-{min(200, largest_rows)} largest listed\n")
        fh.write(f"- Total ID hits (sampled): **{total_id_hits}** (see `id_hits.jsonl`)\n")
        fh.write("\n## Suggested Includes (for pipeline)\n")
        if include_suggestions:
            for g,_ in sorted(include_suggestions.items(), key=lambda kv: (-kv[1], kv[0]))[:40]:
                fh.write(f"- `{g}`\n")
        else:
            fh.write("- (none detected)\n")
        fh.write("\n## Suggested Excludes (for pipeline)\n")
        if exclude_suggestions:
            for g,_ in sorted(exclude_suggestions.items(), key=lambda kv: (-kv[1], kv[0]))[:40]:
                fh.write(f"- `{g}`\n")
        else:
            fh.write("- (none detected)\n")
        fh.write("\n## Codebook Candidates\n")
        if codebook_rows:
            for path, score, reason in sorted(codebook_rows, key=lambda x: (-x[1], x[0]))[:40]:
                fh.write(f"- `{path}` (score={score}, {reason})\n")
        else:
            fh.write("- (none detected)\n")
        fh.write("\n## Largest Files (top 20)\n")
        for sz, p in largest[:20]:
            fh.write(f"- {sizeof_fmt(sz)} — {p}\n")
        fh.write("\n## Files & Artifacts\n")
        fh.write("- `inventory.csv` — per-file summary with flags\n")
        fh.write("- `ext_counts.csv` — counts & total bytes by extension\n")
        fh.write("- `largest_files.csv` — top-200 by size\n")
        fh.write("- `headers_samples.jsonl` — headers/schemas per file\n")
        fh.write("- `column_signals.jsonl` — column textiness & hints\n")
        fh.write("- `id_hits.jsonl` — sampled ID-like values and headers\n")
        fh.write("- `codebook_candidates.csv` — likely vocab/codebook files\n")
        fh.write("- `suggested_*_globs.txt` — include/exclude patterns\n")
        fh.write("- `errors.log` — any read issues & stack traces\n")

    # bundle zip
    bundle = outdir / "bundle.zip"
    with zipfile.ZipFile(bundle, "w", zipfile.ZIP_DEFLATED) as z:
        for p in [inv_csv, ext_counts_csv, largest_csv, headers_jsonl, colsignals_jsonl, idhits_jsonl, codebooks_csv, include_txt, exclude_txt, report_md, err_log]:
            if p.exists(): z.write(p, p.name)
    print(f"[OUT] {report_md}")
    print(f"[OUT] {bundle}")

# ---------- CLI ----------
def main(argv=None):
    ap = argparse.ArgumentParser(description="Diagnose repository content with maximum transparency (safe & low-memory)")
    ap.add_argument("--root", type=str, default="data/01_raw", help="Root folder to scan")
    ap.add_argument("--out",  type=str, default="_diagnostics_full", help="Output folder")
    ap.add_argument("--order", choices=["name","size_asc","size_desc"], default="size_desc")
    ap.add_argument("--max-files", type=int, default=0, help="Cap number of files (0 means no cap)")

    # Sampling caps
    ap.add_argument("--max-bytes", type=int, default=200_000, help="Max bytes to read from text-like files")
    ap.add_argument("--csv-sample-rows", type=int, default=2000, help="Sample rows for CSV/TSV")
    ap.add_argument("--parquet-batch-rows", type=int, default=4096, help="Batch size for Parquet sampling")
    ap.add_argument("--pdf-pages", type=int, default=3, help="Number of PDF pages to sample")

    # Modes
    ap.add_argument("--quick", action="store_true", help="Faster scan: smaller samples")
    ap.add_argument("--progress", action="store_true")
    ap.add_argument("--no-progress", dest="progress", action="store_false")
    ap.set_defaults(progress=True)
    ap.add_argument("--yes", action="store_true", help="Skip preflight confirmation")
    ap.add_argument("--confirm-threshold", type=int, default=5000)

    args = ap.parse_args(argv)

    # lowmem tweak for quick mode
    if args.quick:
        args.csv_sample_rows = min(args.csv_sample_rows, 800)
        args.parquet_batch_rows = min(args.parquet_batch_rows, 2048)
        args.max_bytes = min(args.max_bytes, 120_000)
        args.pdf_pages = min(args.pdf_pages, 2)

    # calm heavy libs
    os.environ.setdefault("OMP_NUM_THREADS","1")
    os.environ.setdefault("MKL_NUM_THREADS","1")
    os.environ.setdefault("OPENBLAS_NUM_THREADS","1")

    root = Path(args.root).resolve()
    outdir = Path(args.out).resolve()
    ensure_dir(outdir)

    diagnose(root, outdir, args)

if __name__ == "__main__":
    main()
