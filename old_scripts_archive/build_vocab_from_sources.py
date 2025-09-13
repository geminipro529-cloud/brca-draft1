#!/usr/bin/env python3
"""
build_vocab_from_sources.py — streaming + progress bars + low memory

What it does
------------
- Scans "structured" tables (CSV/TSV/Parquet/XLSX) and "raw" text/PDFs for vocab
- Harvests rows → temp JSONL (streaming) to keep RAM low
- Deduplicates by alias (casefold) with precedence: base > codebook > scan_raw > scan > seed
- Emits:
  * vocabulary_enriched_v2.parquet (or .csv if parquet not available)
  * alias_map.json (alias→normalized)
  * conflicts.csv (aliases with >1 normalized across sources)
  * build_report.md (summary)

Progress
--------
- Outer tqdm shows files processed and % complete
- Per-file print lines: how many rows harvested from that file so far

Example (PowerShell)
--------------------
python scripts\\build_vocab_from_sources.py `
  --base     data\\02_interim\\VOCAB_COMPILED\\vocabulary_enriched.parquet `
  --scan     data\\01_raw\\STRUCTURED data\\01_raw\\VOCAB `
  --scan-raw data\\01_raw\\QUALITATIVE data\\01_raw\\VOCAB `
  --raw-include-glob "*codebook*" --raw-include-glob "*dictionary*" --raw-include-glob "*lookup*" --raw-include-glob "*legend*" `
  --order size_asc --max-files-scan 2000 --max-files-raw 600 `
  --csv-chunksize 5000 --parquet-batch-rows 8192 --max-chars-raw 150000 `
  --out data\\02_interim\\VOCAB_COMPILED_V2 --progress --yes
"""
from __future__ import annotations
import argparse, csv, json, os, re, sys, traceback, fnmatch
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Iterable
import pandas as pd

try:
    from tqdm.auto import tqdm  # type: ignore
except Exception:
    tqdm = None

# ---------- name/schema hints ----------
VOCAB_NAME_HINT = re.compile(r"(vocab|dictionary|dict|codebook|mapping|lookup|legend|synonym|gazetteer|ontology|terms?)", re.I)
TERM_COL_HINT  = re.compile(r"^(term|alias|surface|synonym(s)?|name|value|token|pattern)$", re.I)
NORM_COL_HINT  = re.compile(r"^(norm(alized)?|canonical|label|id|preferred|concept)$", re.I)
ENT_COL_HINT   = re.compile(r"^(entity|type|class|category)$", re.I)
CODE_HINT      = re.compile(r"(code|abbr|symbol|short|id)$", re.I)
LABEL_HINT     = re.compile(r"(label|meaning|desc|definition|name|term|value)$", re.I)

EXCEL_EXT = {".xlsx"}
TABLE_EXT = {".csv",".tsv",".parquet",".xlsx"}

# ---------- RAW code→label pairs ----------
PAIR_RE = re.compile(r"^\s*([A-Za-z][A-Za-z0-9._/-]{1,24})\s*(?:=|:|-{1,2}|–|—)\s+(.{2,200})\s*$")

def ensure_dir(p: Path): p.mkdir(parents=True, exist_ok=True)

def log_append(path: Path, msg: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as fh:
        fh.write(msg.rstrip() + "\n")

# ---------- table readers (streaming) ----------
def load_csv_stream(path: Path, sep=",", chunksize=5000):
    return pd.read_csv(path, sep=sep, dtype=str, on_bad_lines="skip", encoding="utf-8",
                       engine="python", chunksize=chunksize)

def load_parquet_stream(path: Path, batch_rows=8192):
    import pyarrow.parquet as pq
    pf = pq.ParquetFile(str(path))
    for rb in pf.iter_batches(batch_size=max(1024, batch_rows)):
        yield rb.to_pandas(types_mapper=str)

def load_excel_stream(path: Path, sample_rows=10000):
    import openpyxl
    wb = openpyxl.load_workbook(filename=str(path), read_only=True, data_only=True)
    for sheet in wb.worksheets:
        # infer header from first row with 2+ non-nulls
        header = None
        rows = []
        for i, row in enumerate(sheet.iter_rows(values_only=True)):
            if header is None:
                nonnull = [x for x in (row or []) if x is not None]
                if len(nonnull) >= 2:
                    header = [str(x) if x is not None else f"col_{j}" for j,x in enumerate(row or [])]
                    continue
            else:
                rows.append(row)
            if len(rows) >= sample_rows:
                break
        if header:
            df = pd.DataFrame(rows, columns=header)
            yield df.astype(str)

# ---------- pick alias/norm columns ----------
def pick_columns(df: pd.DataFrame) -> Tuple[List[str], Optional[str], Optional[str]]:
    cols = list(df.columns)
    alias_cols: List[str] = []
    norm_col = None
    ent_col = None

    if len(cols) == 2:
        a,b = cols[0], cols[1]
        def code_like(col):
            s = df[col].dropna().astype(str)
            return bool(CODE_HINT.search(str(col))) or (not s.empty and s.str.match(r"^[A-Za-z0-9._\-]+$").mean() >= 0.6)
        def label_like(col):
            s = df[col].dropna().astype(str)
            return bool(LABEL_HINT.search(str(col))) or (not s.empty and s.str.contains(r"\s").mean() >= 0.5)
        if code_like(a) and label_like(b):
            alias_cols, norm_col = [a], b
        elif code_like(b) and label_like(a):
            alias_cols, norm_col = [b], a
        else:
            alias_cols, norm_col = [a], b
    else:
        for c in cols:
            cn = str(c)
            if TERM_COL_HINT.match(cn): alias_cols.append(c)
            if NORM_COL_HINT.match(cn): norm_col = norm_col or c
            if ENT_COL_HINT.match(cn):  ent_col  = ent_col or c
        if not alias_cols and cols: alias_cols = [cols[0]]
        if not norm_col and len(cols) >= 2: norm_col = cols[1]
    return alias_cols, norm_col, ent_col

def explode_aliases(val: str) -> List[str]:
    if val is None: return []
    s = str(val).strip()
    if not s: return []
    parts = re.split(r"[|;,/]", s)
    out = []
    for p in parts:
        p2 = re.sub(r"\s+", " ", p.strip().strip('"').strip("'"))
        if p2: out.append(p2)
    return out if out else [s]

# ---------- file discovery ----------
def iter_files(dirs: List[Path], include_globs: Optional[List[str]], exclude_globs: Optional[List[str]],
               allowed_exts: set, order: str, max_files: int) -> List[Path]:
    files: List[Path] = []
    for d in dirs:
        if not d.exists(): continue
        for p in d.rglob("*"):
            if not p.is_file(): continue
            if p.suffix.lower() not in allowed_exts: continue
            s = str(p).replace("\\","/")
            if include_globs and not any(fnmatch.fnmatch(s, g) for g in include_globs): continue
            if exclude_globs and any(fnmatch.fnmatch(s, g) for g in exclude_globs): continue
            files.append(p)
    if order == "name":
        files.sort(key=lambda p: str(p).lower())
    elif order == "size_asc":
        files.sort(key=lambda p: (p.stat().st_size if p.exists() else 0))
    else:
        files.sort(key=lambda p: -(p.stat().st_size if p.exists() else 0))
    if max_files and max_files > 0:
        files = files[:max_files]
    return files

# ---------- RAW readers ----------
def pdf_iter_text(path: Path, max_chars: int):
    chars_left = max_chars if max_chars > 0 else None
    def take(txt: str) -> Optional[str]:
        nonlocal chars_left
        if chars_left is None: return txt
        if chars_left <= 0: return None
        if len(txt) > chars_left:
            out = txt[:chars_left]; chars_left = 0; return out
        chars_left -= len(txt); return txt
    # try fitz
    try:
        import fitz
        with fitz.open(str(path)) as doc:
            for page in doc:
                t = page.get_text("text") or ""
                if not t: continue
                t2 = take(t)
                if t2 is None: return
                yield t2
        return
    except Exception:
        pass
    # fallback pdfminer
    try:
        from pdfminer.high_level import extract_text
        t = extract_text(str(path)) or ""
        t2 = take(t)
        if t2 is not None: yield t2
    except Exception:
        return

def harvest_pairs_from_text_lines(lines: Iterable[str], threshold_min=5) -> List[Tuple[str,str]]:
    rows: List[Tuple[str,str]] = []
    nonempty = 0; matched = 0
    for ln in lines:
        if not ln or not ln.strip(): continue
        nonempty += 1
        m = PAIR_RE.match(ln)
        if not m: continue
        code, label = m.group(1).strip(), m.group(2).strip()
        if len(code) < 2 or len(label) < 2: continue
        if code.isdigit(): continue
        matched += 1
        rows.append((code, label))
        if len(rows) >= 10000: break
    if matched >= max(threshold_min, int(0.05 * max(1, nonempty))):
        return rows
    return []

# ---------- temp JSONL writer ----------
def write_jsonl_row(path: Path, row: Dict):
    with open(path, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(row, ensure_ascii=False) + "\n")

# ---------- loading sources ----------
def load_base(path: Optional[Path], tmp_jsonl: Path, counters: Dict[str,int]):
    if not path: return
    for _, df in load_any_table(path):
        if df is None or df.empty: continue
        cols = {c.lower(): c for c in df.columns}
        alias_cols = [cols.get(k) for k in ["alias","term","surface"] if k in cols] or [df.columns[0]]
        norm_col  = next((cols.get(k) for k in ["normalized","norm","canonical","label","id"] if k in cols), (df.columns[1] if len(df.columns)>1 else None))
        ent_col   = next((cols.get(k) for k in ["entity","type","class","category"] if k in cols), None)
        for _, r in df.iterrows():
            ent = (str(r.get(ent_col)).strip() if ent_col and pd.notna(r.get(ent_col)) else None)
            norm = (str(r.get(norm_col)).strip() if norm_col and pd.notna(r.get(norm_col)) else "")
            for ac in alias_cols:
                val = r.get(ac)
                if pd.isna(val): continue
                for alias in explode_aliases(val):
                    alias_s = alias.strip()
                    if not alias_s: continue
                    write_jsonl_row(tmp_jsonl, {
                        "alias": alias_s, "normalized": norm or alias_s, "entity": ent,
                        "source": "base", "source_file": str(path), "source_sheet": "", "source_cols": ",".join(map(str, alias_cols + ([norm_col] if norm_col else []))),
                    })
                    counters["rows"] += 1

def load_any_table(path: Path) -> Iterable[Tuple[Optional[str], pd.DataFrame]]:
    ext = path.suffix.lower()
    try:
        if ext == ".csv":
            yield None, pd.read_csv(path, low_memory=True, on_bad_lines="skip", encoding="utf-8", engine="python")
        elif ext == ".tsv":
            yield None, pd.read_csv(path, sep="\t", low_memory=True, on_bad_lines="skip", encoding="utf-8", engine="python")
        elif ext == ".parquet":
            try: yield None, pd.read_parquet(path, engine="pyarrow")
            except Exception: yield None, pd.read_parquet(path)
        elif ext == ".json":
            try: yield None, pd.read_json(path, lines=True)
            except ValueError: yield None, pd.read_json(path)
        elif ext in EXCEL_EXT:
            x = pd.ExcelFile(path)
            for sh in x.sheet_names:
                try: df = pd.read_excel(path, sheet_name=sh, engine=None)
                except Exception: df = pd.read_excel(path, sheet_name=sh, engine="openpyxl")
                yield sh, df
        else:
            return
    except Exception:
        return

# ---------- scanners (streaming to JSONL) ----------
def scan_tables_to_jsonl(files: List[Path], tmp_jsonl: Path, counters: Dict[str,int], csv_chunksize: int, parquet_batch_rows: int):
    outer = tqdm(total=len(files), desc="Tables", unit="file", dynamic_ncols=True) if counters.get("progress") and tqdm else None
    for fp in files:
        harvested = 0
        try:
            ext = fp.suffix.lower()
            if ext == ".csv":
                reader = load_csv_stream(fp, ",", csv_chunksize)
                for chunk in reader:
                    alias_cols, norm_col, ent_col = pick_columns(chunk)
                    if not alias_cols: continue
                    for _, row in chunk.iterrows():
                        ent = (str(row.get(ent_col)).strip() if ent_col and pd.notna(row.get(ent_col)) else None)
                        norm = (str(row.get(norm_col)).strip() if norm_col and pd.notna(row.get(norm_col)) else "")
                        for ac in alias_cols:
                            val = row.get(ac)
                            if pd.isna(val): continue
                            for alias in explode_aliases(val):
                                a = alias.strip()
                                if not a: continue
                                write_jsonl_row(tmp_jsonl, {
                                    "alias": a, "normalized": norm or a, "entity": ent,
                                    "source": "scan", "source_file": str(fp), "source_sheet": "", "source_cols": ",".join(map(str, alias_cols + ([norm_col] if norm_col else []))),
                                })
                                harvested += 1
                                counters["rows"] += 1
            elif ext == ".tsv":
                reader = load_csv_stream(fp, "\t", csv_chunksize)
                for chunk in reader:
                    alias_cols, norm_col, ent_col = pick_columns(chunk)
                    if not alias_cols: continue
                    for _, row in chunk.iterrows():
                        ent = (str(row.get(ent_col)).strip() if ent_col and pd.notna(row.get(ent_col)) else None)
                        norm = (str(row.get(norm_col)).strip() if norm_col and pd.notna(row.get(norm_col)) else "")
                        for ac in alias_cols:
                            val = row.get(ac)
                            if pd.isna(val): continue
                            for alias in explode_aliases(val):
                                a = alias.strip()
                                if not a: continue
                                write_jsonl_row(tmp_jsonl, {
                                    "alias": a, "normalized": norm or a, "entity": ent,
                                    "source": "scan", "source_file": str(fp), "source_sheet": "", "source_cols": ",".join(map(str, alias_cols + ([norm_col] if norm_col else []))),
                                })
                                harvested += 1
                                counters["rows"] += 1
            elif ext == ".parquet":
                try:
                    for chunk in load_parquet_stream(fp, parquet_batch_rows):
                        alias_cols, norm_col, ent_col = pick_columns(chunk)
                        if not alias_cols: continue
                        for _, row in chunk.iterrows():
                            ent = (str(row.get(ent_col)).strip() if ent_col and pd.notna(row.get(ent_col)) else None)
                            norm = (str(row.get(norm_col)).strip() if norm_col and pd.notna(row.get(norm_col)) else "")
                            for ac in alias_cols:
                                val = row.get(ac)
                                if pd.isna(val): continue
                                for alias in explode_aliases(val):
                                    a = alias.strip()
                                    if not a: continue
                                    write_jsonl_row(tmp_jsonl, {
                                        "alias": a, "normalized": norm or a, "entity": ent,
                                        "source": "scan", "source_file": str(fp), "source_sheet": "", "source_cols": ",".join(map(str, alias_cols + ([norm_col] if norm_col else []))),
                                    })
                                    harvested += 1
                                    counters["rows"] += 1
                except Exception as e:
                    log_append(counters["err_log"], f"{fp}\tParquet skipped: {e}")
            elif ext in EXCEL_EXT:
                try:
                    for chunk in load_excel_stream(fp):
                        alias_cols, norm_col, ent_col = pick_columns(chunk)
                        if not alias_cols: continue
                        for _, row in chunk.iterrows():
                            ent = (str(row.get(ent_col)).strip() if ent_col and pd.notna(row.get(ent_col)) else None)
                            norm = (str(row.get(norm_col)).strip() if norm_col and pd.notna(row.get(norm_col)) else "")
                            for ac in alias_cols:
                                val = row.get(ac)
                                if pd.isna(val): continue
                                for alias in explode_aliases(val):
                                    a = alias.strip()
                                    if not a: continue
                                    write_jsonl_row(tmp_jsonl, {
                                        "alias": a, "normalized": norm or a, "entity": ent,
                                        "source": "scan", "source_file": str(fp), "source_sheet": "", "source_cols": ",".join(map(str, alias_cols + ([norm_col] if norm_col else []))),
                                    })
                                    harvested += 1
                                    counters["rows"] += 1
                except Exception as e:
                    log_append(counters["err_log"], f"{fp}\tExcel skipped: {e}")
            else:
                pass
        except Exception as e:
            log_append(counters["err_log"], f"{fp}\t{type(e).__name__}: {e}")
        finally:
            if outer:
                outer.update(1)
                outer.set_postfix(items=harvested)
            else:
                print(f"[SCAN] {fp} -> +{harvested}")
    if outer: outer.close()

def scan_raw_to_jsonl(files: List[Path], tmp_jsonl: Path, counters: Dict[str,int], max_chars_raw: int):
    outer = tqdm(total=len(files), desc="RAW", unit="file", dynamic_ncols=True) if counters.get("progress") and tqdm else None
    for fp in files:
        harvested = 0
        try:
            if fp.suffix.lower() in {".txt",".md",".rtf"}:
                text = Path(fp).read_text(encoding="utf-8", errors="ignore")
                text = text[:max_chars_raw] if max_chars_raw > 0 else text
                pairs = harvest_pairs_from_text_lines(text.splitlines())
            elif fp.suffix.lower() == ".pdf":
                chunks = []
                for t in pdf_iter_text(fp, max_chars_raw):
                    chunks.append(t)
                    if sum(len(c) for c in chunks) >= max_chars_raw > 0:
                        break
                pairs = harvest_pairs_from_text_lines("\n".join(chunks).splitlines())
            else:
                pairs = []
            for code, label in pairs:
                write_jsonl_row(tmp_jsonl, {
                    "alias": code, "normalized": label, "entity": None,
                    "source": "scan_raw", "source_file": str(fp), "source_sheet": "", "source_cols": "text_pair",
                })
                harvested += 1
                counters["rows"] += 1
        except Exception as e:
            log_append(counters["err_log"], f"{fp}\tRAW skipped: {e}")
        finally:
            if outer:
                outer.update(1)
                outer.set_postfix(items=harvested)
            else:
                print(f"[RAW] {fp} -> +{harvested}")
    if outer: outer.close()

# ---------- unify (streaming) ----------
def unify_from_jsonl(tmp_jsonl: Path, base_alias_keys: Optional[set], out_dir: Path):
    # precedence
    prio = {"base":0, "codebook":1, "scan_raw":2, "scan":3, "seed":4}
    best: Dict[str, Dict] = {}
    conflicts: Dict[str, set] = {}

    # stream the JSONL so we never hold all rows
    with open(tmp_jsonl, "r", encoding="utf-8") as fh:
        for line in fh:
            r = json.loads(line)
            alias = str(r["alias"]).strip()
            if not alias: continue
            key = alias.casefold()
            r_norm = str(r.get("normalized") or "").strip()
            r_ent  = r.get("entity", None)
            source = r.get("source","scan")
            # conflict tracking
            if key not in conflicts: conflicts[key] = set()
            if r_norm: conflicts[key].add(r_norm)

            if key not in best:
                best[key] = {
                    "alias": alias,
                    "normalized": r_norm or alias,
                    "entity": r_ent,
                    "source": source,
                    "source_file": r.get("source_file",""),
                    "source_sheet": r.get("source_sheet",""),
                    "source_cols": r.get("source_cols",""),
                    "__prio": prio.get(source, 9)
                }
            else:
                cur = best[key]
                new_prio = prio.get(source, 9)
                cur_prio = cur["__prio"]
                # choose better row:
                # 1) higher precedence
                # 2) same precedence but current normalized empty and new has value
                better = False
                if new_prio < cur_prio:
                    better = True
                elif new_prio == cur_prio and (not cur.get("normalized")) and r_norm:
                    better = True
                if better:
                    cur.update({
                        "normalized": r_norm or alias,
                        "entity": r_ent,
                        "source": source,
                        "source_file": r.get("source_file",""),
                        "source_sheet": r.get("source_sheet",""),
                        "source_cols": r.get("source_cols",""),
                        "__prio": new_prio
                    })

    # build dataframe
    rows = []
    for key, r in best.items():
        rows.append({k:v for k,v in r.items() if not k.startswith("__")})
    df = pd.DataFrame(rows)
    # conflicts dataframe
    conflict_rows = []
    for key, norms in conflicts.items():
        if len(norms) > 1:
            conflict_rows.append({"alias_key": key, "normalized_variants": sorted(norms)})
    conflicts_df = pd.DataFrame(conflict_rows)

    # outputs
    ensure_dir(out_dir)
    vocab_path = out_dir / "vocabulary_enriched_v2.parquet"
    alias_map  = out_dir / "alias_map.json"
    report_md  = out_dir / "build_report.md"
    conflicts_csv = out_dir / "conflicts.csv"

    try:
        df.to_parquet(vocab_path, index=False)
    except Exception:
        df.to_csv(str(vocab_path).replace(".parquet",".csv"), index=False, encoding="utf-8")

    amap = {a.lower(): n for a,n in zip(df["alias"].astype(str), df["normalized"].astype(str))}
    alias_map.write_text(json.dumps(amap, indent=2, ensure_ascii=False), encoding="utf-8")

    if not conflicts_df.empty:
        conflicts_df.to_csv(conflicts_csv, index=False, encoding="utf-8")

    # diff vs base (if provided)
    new_aliases_n = None
    if base_alias_keys is not None:
        new_aliases_n = len(set(df["alias"].astype(str).str.casefold()) - base_alias_keys)

    with open(report_md, "w", encoding="utf-8") as fh:
        fh.write("# Vocab Build Report\n\n")
        fh.write(f"- Rows in final vocab: {len(df)}\n")
        if new_aliases_n is not None:
            fh.write(f"- New aliases vs base: {new_aliases_n}\n")
        fh.write(f"- Conflicts (>1 normalized per alias): {len(conflicts_df)}\n")

    print(f"[OUT] {vocab_path}")
    print(f"[OUT] {alias_map}")
    print(f"[OUT] {report_md}")
    if not conflicts_df.empty:
        print(f"[OUT] {conflicts_csv}")

# ---------- misc helpers ----------
def preflight_or_exit(dirs: List[Path], label: str, files: List[Path], yes: bool, threshold: int):
    n = len(files)
    suspicious = any("VOCAB" in str(d).upper() and "UNSTRUCTURED" not in str(d).upper() and "STRUCTURED" not in str(d).upper() for d in dirs)
    huge = (n >= threshold)
    if suspicious or huge:
        print(f"\n[PRE-FLIGHT:{label}]")
        print("  target dirs:")
        for d in dirs: print("   ·", d)
        print(f"  files discovered: {n}")
        for s in [str(p.name) for p in files[:10]]:
            print("   -", s)
        if huge:
            print(f"  WARN: large scan (>{threshold} files).")
        if suspicious:
            print("  WARN: path contains 'VOCAB' (ensure this is intentional).")
        if not yes:
            print("  Refusing to proceed. Re-run with --yes or reduce using --max-files-* / globs.")
            sys.exit(2)

# ---------- CLI ----------
def main(argv=None):
    ap = argparse.ArgumentParser(description="Build enriched vocab from multiple sources (streaming + progress)")
    ap.add_argument("--base",     type=str, default=None, help="Existing compiled vocab (parquet/csv/tsv/json)")
    ap.add_argument("--codebook", type=str, default=None, help="codebook_mappings.csv (optional)")
    ap.add_argument("--seed",     type=str, default=None, help="vocab_seed.csv (optional)")

    ap.add_argument("--scan",     type=str, nargs="*", default=None, help="Dirs to scan for table-like vocab files (CSV/TSV/Parquet/XLSX)")
    ap.add_argument("--scan-raw", type=str, nargs="*", default=None, help="Dirs with RAW TXT/PDF to mine code→label pairs")
    ap.add_argument("--include-glob", action="append", default=None, help="Filter files to include (repeatable)")
    ap.add_argument("--exclude-glob", action="append", default=None, help="Filter files to exclude (repeatable)")
    ap.add_argument("--order", type=str, default="size_asc", choices=["name","size_asc","size_desc"])

    ap.add_argument("--max-files-scan", type=int, default=0, help="Cap number of table files to scan")
    ap.add_argument("--max-files-raw",  type=int, default=0, help="Cap number of RAW files to scan")
    ap.add_argument("--csv-chunksize",  type=int, default=5000)
    ap.add_argument("--parquet-batch-rows", type=int, default=8192)
    ap.add_argument("--max-chars-raw",  type=int, default=200000)

    ap.add_argument("--out", type=str, required=True, help="Output directory")

    ap.add_argument("--progress", action="store_true")
    ap.add_argument("--no-progress", dest="progress", action="store_false")
    ap.set_defaults(progress=True)

    ap.add_argument("--yes", action="store_true", help="Skip preflight confirmations")
    ap.add_argument("--confirm-threshold", type=int, default=2500)

    # Low-memory preset
    ap.add_argument("--lowmem", action="store_true", help="Tighten caps for low RAM")

    args = ap.parse_args(argv)

    # calm numeric libs
    os.environ.setdefault("OMP_NUM_THREADS","1")
    os.environ.setdefault("MKL_NUM_THREADS","1")
    os.environ.setdefault("OPENBLAS_NUM_THREADS","1")

    out_dir = Path(args.out).resolve()
    ensure_dir(out_dir)
    tmp_jsonl = out_dir / "harvest.tmp.jsonl"
    if tmp_jsonl.exists(): tmp_jsonl.unlink()
    err_log = out_dir / "logs.txt"
    counters = {"rows": 0, "progress": args.progress, "err_log": err_log}

    if args.lowmem:
        args.csv_chunksize = min(args.csv_chunksize, 4000)
        args.parquet_batch_rows = min(args.parquet_batch_rows, 8192)
        args.max_chars_raw = min(args.max_chars_raw, 150000)
        if not args.max_files_scan: args.max_files_scan = 2000
        if not args.max_files_raw:  args.max_files_raw  = 600

    # 0) base / codebook / seed (loaded once, in-memory -> temp jsonl)
    base_alias_keys: Optional[set] = None
    try:
        if args.base:
            print(f"[INFO] Loading base vocab: {args.base}")
            base_rows = []
            for _, df in load_any_table(Path(args.base)):
                if df is None or df.empty: continue
                base_rows.append(df)
            if base_rows:
                dfb = pd.concat(base_rows, ignore_index=True)
                # emit to tmp jsonl to keep one path
                cols = {c.lower(): c for c in dfb.columns}
                alias_cols = [cols.get(k) for k in ["alias","term","surface"] if k in cols] or [dfb.columns[0]]
                norm_col = next((cols.get(k) for k in ["normalized","norm","canonical","label","id"] if k in cols), (dfb.columns[1] if len(dfb.columns)>1 else None))
                ent_col  = next((cols.get(k) for k in ["entity","type","class","category"] if k in cols), None)
                for _, r in dfb.iterrows():
                    ent = (str(r.get(ent_col)).strip() if ent_col and pd.notna(r.get(ent_col)) else None)
                    norm = (str(r.get(norm_col)).strip() if norm_col and pd.notna(r.get(norm_col)) else "")
                    for ac in alias_cols:
                        val = r.get(ac)
                        if pd.isna(val): continue
                        for alias in explode_aliases(val):
                            a = alias.strip()
                            if not a: continue
                            write_jsonl_row(tmp_jsonl, {"alias": a, "normalized": norm or a, "entity": ent,
                                                        "source": "base", "source_file": str(Path(args.base).resolve()),
                                                        "source_sheet": "", "source_cols": ",".join(map(str, alias_cols + ([norm_col] if norm_col else [])))})
                            counters["rows"] += 1
                base_alias_keys = set(dfb[alias_cols[0]].astype(str).str.casefold().tolist())
        if args.codebook and Path(args.codebook).exists():
            print(f"[INFO] Loading codebook mappings: {args.codebook}")
            df = pd.read_csv(args.codebook, dtype=str)
            for _, r in df.iterrows():
                code = str(r.get("code") or r.get("CODE") or "").strip()
                lab  = str(r.get("label") or r.get("LABEL") or "").strip()
                if not code: continue
                write_jsonl_row(tmp_jsonl, {"alias": code, "normalized": lab or code, "entity": None,
                                            "source": "codebook", "source_file": str(Path(args.codebook).resolve()),
                                            "source_sheet": r.get("sheet","") if "sheet" in r else "", "source_cols": "code,label"})
                counters["rows"] += 1
        if args.seed and Path(args.seed).exists():
            print(f"[INFO] Loading seed: {args.seed}")
            df = pd.read_csv(args.seed, dtype=str)
            for _, r in df.iterrows():
                val = str(r.get("value","")).strip()
                ent = str(r.get("entity_hint") or r.get("entity") or "").strip() or None
                if not val: continue
                write_jsonl_row(tmp_jsonl, {"alias": val, "normalized": val, "entity": ent,
                                            "source": "seed", "source_file": str(Path(args.seed).resolve()),
                                            "source_sheet": "", "source_cols": "value"})
                counters["rows"] += 1
    except Exception as e:
        print(f"[FATAL] {type(e).__name__}: {e}", file=sys.stderr)
        traceback.print_exc()
        sys.exit(2)

    # 1) structured tables
    if args.scan:
        scan_dirs = [Path(p).resolve() for p in args.scan]
        table_files = iter_files(scan_dirs, args.include_glob, args.exclude_glob, TABLE_EXT, args.order, args.max_files_scan)
        preflight_or_exit(scan_dirs, "TABLES", table_files, args.yes, args.confirm_threshold)
        print(f"[INFO] Scanning {len(table_files)} table file(s)...")
        scan_tables_to_jsonl(table_files, tmp_jsonl, counters, args.csv_chunksize, args.parquet_batch_rows)

    # 2) RAW text/PDF
    if args.scan_raw:
        raw_dirs = [Path(p).resolve() for p in args.scan_raw]
        raw_exts = {".txt",".md",".rtf",".pdf"}
        raw_files = iter_files(raw_dirs, args.include_glob, args.exclude_glob, raw_exts, args.order, args.max_files_raw)
        preflight_or_exit(raw_dirs, "RAW", raw_files, args.yes, args.confirm_threshold)
        print(f"[INFO] Scanning {len(raw_files)} RAW file(s)...")
        scan_raw_to_jsonl(raw_files, tmp_jsonl, counters, args.max_chars_raw)

    print(f"[INFO] Harvested rows: {counters['rows']}")

    # 3) unify (streamed from jsonl) + outputs
    unify_from_jsonl(tmp_jsonl, base_alias_keys, Path(args.out))

    # 4) cleanup temp
    try:
        tmp_jsonl.unlink()
    except Exception:
        pass

if __name__ == "__main__":
    main()
