#!/usr/bin/env python3
import os, re, sys, argparse, time, shutil
from pathlib import Path
from typing import List, Tuple
import pandas as pd

# ---------- Windows helpers ----------
def win_safe(p: str) -> str:
    rp = Path(p).resolve()
    return str(rp) if os.name != "nt" else ("\\\\?\\" + str(rp))

def safe_rename(src: str, dst: str, retries=5, delay=2):
    Path(dst).parent.mkdir(parents=True, exist_ok=True)
    for i in range(retries):
        try:
            os.replace(src, dst); return
        except PermissionError:
            if i == retries - 1: raise
            time.sleep(delay)
        except Exception:
            if i == retries - 1:
                shutil.move(src, dst); return
            time.sleep(delay)

# ---------- IO ----------
def read_csv_forgiving(path, sep=None):
    ext = Path(path).suffix.lower()
    if sep is None: sep = "\t" if ext in {".tsv",".txt"} else ","
    try:
        return pd.read_csv(win_safe(path), sep=sep, dtype=str, low_memory=False,
                           engine="c", on_bad_lines="skip", encoding="utf-8", errors="replace")
    except Exception:
        return pd.read_csv(win_safe(path), sep=sep, dtype=str,
                           engine="python", on_bad_lines="skip", encoding="utf-8", errors="replace")

def read_any_table(path: str) -> pd.DataFrame:
    if path.lower().endswith((".parquet",".pq")):
        return pd.read_parquet(win_safe(path))
    return read_csv_forgiving(path)

# ---------- discovery ----------
CANDIDATES = [
    r"data\02_interim\VOCAB COMPILED",
    r"data\02_interim\VOCAB COMPILED V2",
    r"data\02_interim\VOCAB_COMPILED",
    r"data\02_interim\VOCAB_COMPILED_V2",
]

def pick_dir(explicit: str|None, prefer_v2=False) -> str|None:
    if explicit:
        return explicit if Path(explicit).exists() else None
    found = []
    for d in CANDIDATES:
        if Path(d).exists():
            if prefer_v2 and ("V2" in d or d.endswith("_V2")): found.append(d)
            if not prefer_v2 and ("V2" not in d and not d.endswith("_V2")): found.append(d)
    # prefer underscored over spaced if both exist
    underscored = [d for d in found if "_" in Path(d).name]
    return underscored[0] if underscored else (found[0] if found else None)

def list_tables(root: str) -> List[str]:
    out = []
    for r, _, files in os.walk(root):
        for f in files:
            fl = f.lower()
            if fl.endswith((".parquet",".pq",".csv",".tsv",".txt")):
                out.append(os.path.join(r, f))
    return out

# ---------- standardize ----------
def std_vocab(df: pd.DataFrame, src_file: str, src_root: str) -> pd.DataFrame:
    df = df.copy()
    if "term" not in df.columns:
        df.insert(0, "term", df.iloc[:,0].astype(str))
    if "normalized" not in df.columns:
        df["normalized"] = df["term"].astype(str).str.lower().str.strip()
    else:
        df["normalized"] = df["normalized"].astype(str).str.lower().str.strip()
    df["term"] = df["term"].astype(str).str.strip()
    df["__source_file"] = src_file
    df["__source_dir"]  = src_root
    for c in df.columns:
        if df[c].dtype == "object": df[c] = df[c].astype(str)
    return df

def load_dir(dir_path: str) -> Tuple[pd.DataFrame, list]:
    files = list_tables(dir_path)
    report = []
    rows = []
    print(f"🔎 Scanning {dir_path} ... files found: {len(files)}")
    for fp in files:
        try:
            df = read_any_table(fp)
            df = std_vocab(df, os.path.basename(fp), str(Path(dir_path)))
            rows.append(df)
            report.append({"folder": dir_path, "file": fp, "rows": len(df), "status": "ok"})
        except Exception as e:
            report.append({"folder": dir_path, "file": fp, "rows": 0, "status": f"skip:{e}"})
    merged = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame(columns=["term","normalized"])
    print(f"   → loaded rows: {len(merged)} from {len(rows)} readable tables")
    return merged, report

def dedupe_pref_v2(v1: pd.DataFrame, v2: pd.DataFrame) -> pd.DataFrame:
    def add_pri(df, pri): 
        df = df.copy(); df["__pri"] = pri; df["__score"] = df.notna().sum(axis=1); return df
    allr = pd.concat([add_pri(v2,2), add_pri(v1,1)], ignore_index=True)  # V2 first
    allr["__key"] = allr["normalized"].fillna(allr["term"].str.lower().str.strip())
    allr = allr.sort_values(["__pri","__score"], ascending=[False, False], kind="stable")
    keep = allr.drop_duplicates(subset="__key", keep="first").drop(columns=["__pri","__score","__key"], errors="ignore")
    if "term" not in keep.columns: keep["term"] = keep["normalized"]
    if "normalized" not in keep.columns: keep["normalized"] = keep["term"].str.lower().str.strip()
    cols = ["term","normalized"] + [c for c in keep.columns if c not in {"term","normalized"}]
    return keep[cols].reset_index(drop=True)

def write_final(df: pd.DataFrame, out_parquet: str) -> str:
    tmp = out_parquet + ".tmp"
    try:
        df.to_parquet(win_safe(tmp), index=False); safe_rename(tmp, out_parquet); return out_parquet
    except Exception:
        out_csv = str(Path(out_parquet).with_suffix(".csv"))
        df.to_csv(out_csv + ".tmp", index=False); safe_rename(out_csv + ".tmp", out_csv); return out_csv

def main():
    ap = argparse.ArgumentParser(description="Merge VOCAB folders (v1 + v2) into canonical VOCAB_COMPILED")
    ap.add_argument("--v1", help="Path to v1 (e.g., data\\02_interim\\VOCAB_COMPILED)")
    ap.add_argument("--v2", help="Path to v2 (e.g., data\\02_interim\\VOCAB_COMPILED_V2)")
    ap.add_argument("--out-dir", default=r"data\02_interim\VOCAB_COMPILED")
    ap.add_argument("--archive-old", action="store_true")
    ap.add_argument("--delete-old", action="store_true")
    args = ap.parse_args()

    v1 = pick_dir(args.v1, prefer_v2=False)
    v2 = pick_dir(args.v2, prefer_v2=True)
    print(f"v1 dir → {v1}")
    print(f"v2 dir → {v2}")
    if not v1 and not v2:
        sys.exit("❌ No vocab directories found. Check names/paths.")
    out_dir = Path(args.out_dir); out_dir.mkdir(parents=True, exist_ok=True)
    out_parquet = str(out_dir / "vocabulary_enriched.parquet")

    v1_df, rep1 = (pd.DataFrame(columns=["term","normalized"]), []) if not v1 else load_dir(v1)
    v2_df, rep2 = (pd.DataFrame(columns=["term","normalized"]), []) if not v2 else load_dir(v2)

    if len(v1_df)==0 and len(v2_df)==0:
        sys.exit("❌ Found directories but no readable vocab files (parquet/csv/tsv/txt).")

    merged = dedupe_pref_v2(v1_df, v2_df)
    artifact = write_final(merged, out_parquet)

    # write report
    rep_path = out_dir / "vocabulary_merge.report.csv"
    pd.DataFrame([*rep1, *rep2, {"folder":"RESULT","file":artifact,"rows":len(merged),"status":"merged"}]).to_csv(rep_path, index=False)
    print(f"✅ Merge complete → {artifact}")
    print(f"📑 Report → {rep_path}")

    # archive/delete v2 to prevent “two folders” problem
    if v2 and Path(v2).exists():
        if args.delete_old:
            try: shutil.rmtree(v2); print(f"🧹 Deleted V2: {v2}")
            except Exception as e: print(f"⚠️ Could not delete V2: {e}")
        elif args.archive_old:
            stamp = time.strftime("%Y%m%d")
            arch = out_dir.parent / "ARCHIVE" / f"{Path(v2).name}_{stamp}"
            try:
                arch.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(v2, arch); print(f"📦 Archived V2 → {arch}")
            except Exception as e:
                print(f"⚠️ Could not archive V2: {e}")

if __name__ == "__main__":
    main()
