# File: scripts/convert_big_csvs_to_parquet.py
# Resumable CSV/TSV -> Parquet (or .csv.gz fallback) with ASCII progress and fuzzy column matching.
import os, sys, argparse, time, json, gzip, csv, traceback
import pandas as pd

def human_mb(x): return f"{x/1024/1024:.1f}MB"

def atomic_write(path, text):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8", errors="replace") as f: f.write(text)
    try: os.replace(tmp, path)
    except PermissionError: time.sleep(0.3); os.replace(tmp, path)

def detect_sep(path):
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            first = f.readline()
        return "\t" if first.count("\t") >= first.count(",") else ","
    except Exception:
        return ","

def estimate_rows(path, sample_bytes=32*1024*1024):
    try:
        size = os.path.getsize(path)
        with open(path, "rb") as f: buf = f.read(min(sample_bytes, size))
        lines = max(buf.count(b"\n"), 1)
        avg = max(len(buf)/lines, 100)
        return size, max(int(size/avg)-1, 1), avg
    except Exception:
        size = os.path.getsize(path)
        return size, 1_000_000, max(size/1_000_000, 100)

def print_prog(prefix, done_rows, est_rows, avg_bytes, total_bytes):
    est_bytes = min(int(done_rows*avg_bytes), total_bytes)
    pct = min((done_rows/max(est_rows,1))*100, 99.9)
    bar = int(pct/4)  # 25 chars
    sys.stdout.write("\r%s [%s%s] %5.1f%%  %s/%s" % (
        prefix, "#"*bar, "."*(25-bar), pct, human_mb(est_bytes), human_mb(total_bytes)))
    sys.stdout.flush()

def write_part_parquet(df, out_dir, base, idx):
    try:
        import pyarrow  # noqa
        p = os.path.join(out_dir, f"{base}_part-{idx:05d}.parquet"); tmp = p + ".tmp"
        df.to_parquet(tmp, index=False); os.replace(tmp, p); return p
    except Exception:
        return None

def write_part_csv(df, out_dir, base, idx):
    p = os.path.join(out_dir, f"{base}_part-{idx:05d}.csv.gz"); tmp = p + ".tmp"
    with gzip.open(tmp, "wt", newline="", encoding="utf-8") as f: df.to_csv(f, index=False)
    os.replace(tmp, p); return p

# ---------- NEW: fuzzy header matching ----------
ALIASES = {
    "SANGER_MODEL_ID": ["SANGER_MODEL_ID","MODEL_ID","MODELID","SANGERID","SANGER_MODEL","SANGER MODEL ID","SIDM","MODEL"],
    "DRUG_ID":         ["DRUG_ID","DRUGID","COMPOUND_ID","CMPD_ID","COMPOUNDID"],
    "DRUG_NAME":       ["DRUG_NAME","COMPOUND","COMPOUND_NAME","DRUG","DRUGNAME","COMPOUND NAME"],
    "AUC":             ["AUC","AUC1","AUC_1","AREA_UNDER_CURVE","AREAUNDERCURVE"],
    "IC50":            ["IC50","LN_IC50","LOG_IC50","LOGIC50","IC50_UM","IC50UM","IC50_NM","IC50(NM)","IC50(UM)","IC50 U M","LN(IC50)"],
}

def _norm(s: str) -> str:
    return "".join(ch for ch in s.lower() if ch.isalnum())

def read_header_fast(path, sep):
    # Read first line and split with csv.reader to avoid heavy pandas init
    try:
        opener = (lambda: open(path, "r", encoding="utf-8", errors="replace"))
        with opener() as f:
            first = f.readline().rstrip("\n\r")
        return [c.strip() for c in next(csv.reader([first], delimiter=sep))]
    except Exception:
        # Fallback to pandas header-only if needed
        try:
            return list(pd.read_csv(path, sep=sep, nrows=0, engine="python", dtype=str).columns)
        except Exception:
            return []

def resolve_usecols(requested, header):
    if not requested: return None, {}, []
    # Build normalized header map
    hmap = {}
    for col in header:
        key = _norm(col)
        if key and key not in hmap: hmap[key] = col

    resolved, mapping, missing = [], {}, []
    for want in requested:
        want_norm = _norm(want)
        # 1) direct exact normalized match
        if want_norm in hmap:
            real = hmap[want_norm]; resolved.append(real); mapping[want] = real; continue
        # 2) alias table
        aliases = ALIASES.get(want.upper(), [want])
        found = None
        for alias in aliases:
            alias_norm = _norm(alias)
            if alias_norm in hmap: found = hmap[alias_norm]; break
        # 3) substring relaxed search (ic50 -> ln_ic50 etc.)
        if not found:
            for key, real in hmap.items():
                if want_norm in key or any(_norm(a) in key for a in aliases):
                    found = real; break
        if found:
            resolved.append(found); mapping[want] = found
        else:
            missing.append(want)
    # Dedup while preserving order
    seen = set(); deduped = []
    for c in resolved:
        if c not in seen:
            seen.add(c); deduped.append(c)
    return deduped or None, mapping, missing
# ---------- /fuzzy header matching ----------

def main():
    ap = argparse.ArgumentParser(description="Resumable CSV/TSV -> Parquet with ASCII progress and fuzzy columns")
    ap.add_argument("--in", dest="in_paths", nargs="+", required=True)
    ap.add_argument("--out", dest="out_root", required=True)
    ap.add_argument("--columns", nargs="*", default=None, help="Preferred columns; fuzzy-matched to real headers")
    ap.add_argument("--chunksize", type=int, default=20000)
    ap.add_argument("--list-columns", action="store_true", help="Print headers and exit")
    ap.add_argument("--dry-run", action="store_true", help="Resolve columns, print mapping, exit")
    args = ap.parse_args()

    os.makedirs(args.out_root, exist_ok=True)
    for src in args.in_paths:
        if not os.path.exists(src):
            print("Skip missing:", src, file=sys.stderr); continue

        base = os.path.splitext(os.path.basename(src))[0]
        out_dir = os.path.join(args.out_root, base); os.makedirs(out_dir, exist_ok=True)
        manifest = os.path.join(out_dir, "_manifest.json")

        sep = detect_sep(src)
        total_bytes, est_rows, avg_bytes = estimate_rows(src)

        # Header peek and column resolution
        header = read_header_fast(src, sep)
        usecols, mapping, missing = resolve_usecols(args.columns, header)

        if args.list_columns:
            print("Header columns:", header); return
        if args.dry_run:
            print("Resolved columns:", usecols if usecols else "(all)")
            if mapping: print("Mapping:", mapping)
            if missing: print("Missing (not found):", missing)
            return

        # Persist state (resume)
        state = {"rows_done":0, "part_idx":0, "sep":sep,
                 "requested_columns":args.columns, "resolved_columns":usecols,
                 "chunksize":args.chunksize, "total_bytes":total_bytes, "est_rows":est_rows}
        if os.path.exists(manifest):
            try:
                with open(manifest, "r", encoding="utf-8") as f: state.update(json.load(f))
            except Exception: pass

        rows_done = int(state.get("rows_done", 0))
        part_idx  = int(state.get("part_idx", 0))
        dialect = "TSV" if sep == "\t" else "CSV"
        print(f"\nConverting: {src}")
        print(f"Detected: {dialect} | est_rows~{est_rows:,} | size={human_mb(total_bytes)} | resume_rows={rows_done:,}")
        if args.columns:
            print("Requested:", args.columns)
            print("Using:", usecols if usecols else "(all)")
            if mapping: print("Resolved map:", mapping)
            if missing: print("Not found:", missing)

        header_row = 0
        skiprows = range(1, rows_done+1) if rows_done>0 else None
        last = time.time()

        try:
            for chunk in pd.read_csv(src, sep=sep, dtype=str, encoding="utf-8",
                                     engine="python", on_bad_lines="skip",
                                     usecols=usecols, chunksize=args.chunksize,
                                     header=header_row, skiprows=skiprows):
                chunk.columns = [c.strip() for c in chunk.columns]
                p = write_part_parquet(chunk, out_dir, base, part_idx) or write_part_csv(chunk, out_dir, base, part_idx)
                part_idx += 1; rows_done += len(chunk)

                if time.time() - last > 0.5:
                    print_prog("Converting", rows_done, est_rows, avg_bytes, total_bytes); last = time.time()
                state.update({"rows_done":rows_done, "part_idx":part_idx, "resolved_columns":usecols})
                atomic_write(manifest, json.dumps(state, ensure_ascii=False))

            print_prog("Converting", rows_done, est_rows, avg_bytes, total_bytes); sys.stdout.write("\nOK\n")
        except KeyboardInterrupt:
            print("\nInterrupted — resume by rerunning the same command."); return
        except Exception:
            traceback.print_exc()
            print("\nError — rerun to resume from last checkpoint."); return

if __name__ == "__main__":
    main()
