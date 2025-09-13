# File: scripts/link_patients_across_cohorts.py
# Resumable ID linker with colored % bars (per-file + overall), Windows-safe checkpoints, low-RAM streaming.
import os, sys, re, csv, time, json, gzip, argparse, traceback
from itertools import combinations
from collections import defaultdict
import pandas as pd

# --- ID headers + value fallback ---
ID_KEYS = {
    "patient","case","submitter","subject","participant","uuid","barcode",
    "sample","aliquot","portion","slide","model",
    "patient_id","sample_id","case_id","subject_id","model_id","sanger_model_id",
    "tumor_sample_barcode","matched_norm_sample_barcode","depmap_id","sanger_model","sangerid"
}
ID_COL_PAT = re.compile(r"|".join([rf"(?:^|_){re.escape(k)}(?:_|$)" for k in ID_KEYS]), re.I)
TCGA_BARCODE_RE = re.compile(r"^TCGA-[A-Z0-9]{2}-[A-Z0-9]{4}", re.I)
DEPMAP_RE       = re.compile(r"^(ACH-?\d+|DEPMAP[-_ ]?\d+)$", re.I)
SANGER_RE       = re.compile(r"^(SIDM[-_ ]?\d+|SANGER[-_ ]?\d+)$", re.I)

CSV_LIKE = {".csv", ".tsv", ".gz"}
PARQ     = {".parquet", ".pq"}

# --- tiny UI helpers ---
def _supports_color() -> bool:
    return os.name != "nt" or bool(os.environ.get("WT_SESSION") or os.environ.get("ANSICON") or os.environ.get("TERM_PROGRAM") == "vscode")
def _color(s, code, enable):  # code: '32' green, '36' cyan, '33' yellow
    return f"\x1b[{code}m{s}\x1b[0m" if enable else s
def _bar(pct, width=28):
    pct = max(0.0, min(100.0, pct))
    filled = int((pct/100.0)*width)
    return "#"*filled + "."*(width-filled)
def _human_mb(x): return f"{x/1024/1024:.1f}MB"

def _norm_id(x):
    if x is None: return ""
    s = str(x).strip()
    if not s or s.lower() in {"nan","none","null"}: return ""
    return s.upper().replace("-", "").replace("_", "").replace(" ", "")

class DSU:
    def __init__(self, parents=None): self.p = dict(parents or {})
    def find(self, x):
        if x not in self.p: self.p[x] = x
        while x != self.p[x]:
            self.p[x] = self.p[self.p[x]]; x = self.p[x]
        return x
    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra != rb: self.p[rb] = ra

def _list_tables(root):
    out = []
    for r, _, fs in os.walk(root):
        for n in fs:
            p = os.path.join(r, n); ext = os.path.splitext(n)[1].lower()
            if ext in CSV_LIKE or ext in PARQ:
                low = p.replace("\\","/").lower()
                if "/case_lists/" in low or os.path.basename(p).lower().startswith("meta_"):
                    continue
                out.append(p)
    out.sort(); return out

def _sniff_header(path):
    ext = os.path.splitext(path)[1].lower()
    if ext in PARQ:
        try: return list(pd.read_parquet(path, columns=None).columns)
        except Exception: return []
    try:
        opener = (lambda: gzip.open(path, "rt", encoding="utf-8", errors="replace")) if path.endswith(".gz") \
                 else (lambda: open(path, "r", encoding="utf-8", errors="replace"))
        with opener() as f:
            first = f.readline()
        if ":" in first and ("," not in first and "\t" not in first): return []
        sep = "\t" if first.count("\t") >= first.count(",") else ","
        return [c.strip() for c in next(csv.reader([first], delimiter=sep))]
    except Exception:
        try: return list(pd.read_csv(path, nrows=0, dtype=str, engine="python", on_bad_lines="skip").columns)
        except Exception: return []

def _candidate_cols(cols): return [c for c in (cols or []) if ID_COL_PAT.search(c or "")]

def _value_sniff_cols(path, max_rows=5000):
    sep = "\t" if path.endswith(".tsv") else ","
    try:
        df = pd.read_csv(path, sep=sep, nrows=max_rows, dtype=str, encoding="utf-8",
                         engine="python", on_bad_lines="skip")
    except Exception:
        return []
    hits, total = [], len(df)
    if total == 0: return hits
    for c in df.columns:
        s = df[c].dropna().astype(str).head(max_rows)
        if s.empty: continue
        m = sum(bool(TCGA_BARCODE_RE.search(v) or DEPMAP_RE.search(v) or SANGER_RE.search(v)) for v in s)
        if m >= 10 and (m / max(len(s),1)) >= 0.02:
            hits.append(c)
    return hits

# --- progress estimation (CSV) ---
def _estimate_rows_csv(path, sample_bytes=32*1024*1024):
    try:
        size = os.path.getsize(path)
        with open(path, "rb") as f:
            buf = f.read(min(sample_bytes, size))
        lines = max(buf.count(b"\n"), 1)
        avg = max(len(buf)/lines, 100)   # avg bytes/row
        est_rows = max(int(size/avg)-1, 1)
        return size, est_rows, avg
    except Exception:
        size = os.path.getsize(path)
        return size, 1_000_000, max(size/1_000_000, 100)

def _edges_csv(path, usecols, chunksize):
    sep = "\t" if path.endswith(".tsv") else ","
    for df in pd.read_csv(path, sep=sep, dtype=str, encoding="utf-8",
                          usecols=usecols, chunksize=chunksize,
                          engine="python", on_bad_lines="skip"):
        yield df

def _edges_parquet(path, cols):
    # not chunked; typical parquet here is modest. Show as 100% when read.
    df = pd.read_parquet(path, columns=cols)
    yield df

# --- Windows-safe replace with backoff ---
def _safe_replace(src, dst, attempts=8, base_sleep=0.25):
    for i in range(attempts):
        try: os.replace(src, dst); return
        except PermissionError: time.sleep(base_sleep * (1.6 ** i))
    try: os.remove(dst)
    except Exception: pass
    os.replace(src, dst)

def _save_state(state_dir, dsu, done_files):
    os.makedirs(state_dir, exist_ok=True)
    ptmp = os.path.join(state_dir, f"parents.{int(time.time()*1000)}.{os.getpid()}.json.gz.tmp")
    with gzip.open(ptmp, "wt", encoding="utf-8") as g: json.dump(dsu.p, g, ensure_ascii=False)
    _safe_replace(ptmp, os.path.join(state_dir, "parents.json.gz"))
    mtmp = os.path.join(state_dir, f"state.{int(time.time()*1000)}.{os.getpid()}.json.tmp")
    with open(mtmp, "w", encoding="utf-8") as f: json.dump({"done_files": sorted(list(done_files))}, f)
    _safe_replace(mtmp, os.path.join(state_dir, "state.json"))

def _load_state(state_dir):
    parents, done = {}, set()
    try:
        with gzip.open(os.path.join(state_dir, "parents.json.gz"), "rt", encoding="utf-8") as g: parents = json.load(g)
    except Exception: pass
    try:
        with open(os.path.join(state_dir, "state.json"), "r", encoding="utf-8") as f: done = set(json.load(f).get("done_files", []))
    except Exception: pass
    return parents, done

def main():
    ap = argparse.ArgumentParser(description="Resumable cross-cohort ID linker with colored progress bars.")
    ap.add_argument("--in", dest="in_dir", required=True)
    ap.add_argument("--out", dest="out_dir", required=True)
    ap.add_argument("--checkpoint", default=os.path.join("data","02_interim","_link_state"))
    ap.add_argument("--chunksize", type=int, default=100_000)
    ap.add_argument("--no-value-sniff", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--no-color", action="store_true")
    ap.add_argument("--progress-interval", type=float, default=0.6, help="Seconds between progress updates")
    args = ap.parse_args()

    color_on = _supports_color() and not args.no_color
    os.makedirs(args.out_dir, exist_ok=True)
    parents, done = _load_state(args.checkpoint)
    dsu = DSU(parents)

    files = _list_tables(args.in_dir)
    # precompute total bytes for overall bar
    sizes = {p: os.path.getsize(p) if os.path.exists(p) else 0 for p in files}
    total_bytes = sum(sizes.values()) or 1
    processed_bytes = sum(sizes[p] for p in done if p in sizes)

    total = len(files)
    print(f"Linking: files={total} from {args.in_dir}")

    choices_log = []
    for idx, path in enumerate(files, 1):
        cols = _sniff_header(path)
        idcols = _candidate_cols(cols)
        if not idcols and not args.no_value_sniff:
            vs = _value_sniff_cols(path)
            if vs: idcols = vs

        fsize = sizes.get(path, 0)
        if args.dry_run:
            choices_log.append([idx, path, ",".join(idcols) if idcols else "", _human_mb(fsize)])
            status = "NO-COLS" if not idcols else f"cols={','.join(idcols)}"
            print(f"[{idx}/{total}] {status} : {path}")
            continue

        if path in done:
            processed_pct = (processed_bytes/total_bytes)*100
            print(f"[{idx}/{total}] skip done: {path}   | overall [{_color(_bar(processed_pct), '32', color_on)}] {processed_pct:5.1f}% {_human_mb(processed_bytes)}/{_human_mb(total_bytes)}")
            continue

        if not idcols:
            print(f"[{idx}/{total}] no ID-like cols: {path}")
            done.add(path); _save_state(args.checkpoint, dsu, done); processed_bytes += fsize; continue

        # per-file progress init (CSV estimate)
        ext = os.path.splitext(path)[1].lower()
        rows_done, est_rows, avg_bytes = 0, None, None
        if ext in CSV_LIKE:
            fsize, est_rows, avg_bytes = _estimate_rows_csv(path)

        print(f"[{idx}/{total}] {path} | cols={','.join(idcols)} | size={_human_mb(fsize)}")
        edges = 0; last = time.time()

        try:
            gen = _edges_parquet(path, idcols) if ext in PARQ else _edges_csv(path, idcols, args.chunksize)
            for df in gen:
                # unions for this chunk
                for row in df.itertuples(index=False, name=None):
                    vals = [ _norm_id(v) for v in row if _norm_id(v) ]
                    for i in range(len(vals)-1):
                        a = vals[i]
                        for j in range(i+1, len(vals)):
                            b = vals[j]; dsu.union(a,b); edges += 1
                # progress update
                if ext in CSV_LIKE and est_rows:
                    rows_done += len(df)
                    est_bytes = min(int(rows_done*avg_bytes), fsize)
                    processed_now = (processed_bytes + est_bytes)
                    file_pct = min((rows_done/max(est_rows,1))*100, 99.9)
                    total_pct = (processed_now/total_bytes)*100
                    if time.time() - last > args.progress_interval:
                        sys.stdout.write("\r      file  [%s] %5.1f%%  %s/%s   overall [%s] %5.1f%%  %s/%s   edges: %s"
                                         % (_color(_bar(file_pct), "36", color_on), file_pct, _human_mb(est_bytes), _human_mb(fsize),
                                            _color(_bar(total_pct), "32", color_on), total_pct, _human_mb(processed_now), _human_mb(total_bytes), f"{edges:,}"))
                        sys.stdout.flush(); last = time.time()
            # finalize bar to 100% for this file
            processed_bytes += fsize
            total_pct = (processed_bytes/total_bytes)*100
            sys.stdout.write("\r      file  [%s] 100.0%%  %s/%s   overall [%s] %5.1f%%  %s/%s   edges: %s\n"
                             % (_color(_bar(100.0), "36", color_on), _human_mb(fsize), _human_mb(fsize),
                                _color(_bar(total_pct), "32", color_on), total_pct, _human_mb(processed_bytes), _human_mb(total_bytes), f"{edges:,}"))
        except KeyboardInterrupt:
            print("\nInterrupted — checkpointing."); _save_state(args.checkpoint, dsu, done); print("Rerun to resume."); return
        except Exception:
            traceback.print_exc()

        done.add(path); _save_state(args.checkpoint, dsu, done)

    if args.dry_run:
        out_csv = os.path.join(args.out_dir, "linker_dryrun_choices.csv")
        os.makedirs(args.out_dir, exist_ok=True)
        with open(out_csv, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f); w.writerow(["idx","path","chosen_id_columns","size"])
            w.writerows(choices_log)
        print("Wrote:", out_csv); return

    # Build clusters and outputs
    clusters = defaultdict(list)
    for node in list(dsu.p.keys()):
        clusters[dsu.find(node)].append(node)
    canon = {}
    for root, members in clusters.items():
        pick = sorted(members, key=lambda s:(len(s), s))[0]
        for m in members: canon[m] = pick

    def _write_csv(path, rows, header):
        tmp = path + ".tmp"
        with open(tmp, "w", newline="", encoding="utf-8", errors="replace") as f:
            w = csv.writer(f); w.writerow(header); w.writerows(rows)
        _safe_replace(tmp, path)

    os.makedirs(args.out_dir, exist_ok=True)
    _write_csv(os.path.join(args.out_dir, "id_map.csv"), [[k,v] for k,v in sorted(canon.items())], ["raw_id","canonical_id"])
    _write_csv(os.path.join(args.out_dir, "clusters.csv"),
               [[root, len(m), "|".join(sorted(m)[:50])] for root, m in sorted(clusters.items(), key=lambda kv:-len(kv[1]))],
               ["canonical_id","cluster_size","sample_members"])
    with open(os.path.join(args.out_dir, "report.md"), "w", encoding="utf-8") as f:
        f.write(f"# ID Linking Report\n- Files: {total}\n- Nodes: {len(dsu.p):,}\n- Clusters: {len(clusters):,}\n")
    print("OK: id_map.csv, clusters.csv, report.md")


# 1) Structured_annotated (resumes; fixes your lock issue)
# python scripts\link_patients_across_cohorts.py --in data\02_interim\structured_annotated --out data\02_interim\id_map --checkpoint data\02_interim\_link_state --chunksize 100000
# 2) Structured_annotated_v2 (same checkpoint to merge unions across both)
# python scripts\link_patients_across_cohorts.py --in data\02_interim\structured_annotated_v2 --out data\02_interim\id_map --checkpoint data\02_interim\_link_state --chunksize 100000


if __name__ == "__main__":
    main()
