#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import annotations
import argparse, os, glob, re, json, sqlite3
from pathlib import Path
from typing import List, Optional

import polars as pl
from rich.console import Console
from rich.progress import Progress, BarColumn, TimeElapsedColumn, TimeRemainingColumn, TextColumn

console = Console()

DEFAULT_ID_CANDIDATES = [
    "sample_id","model_id","tumor_sample_barcode","tsb","patient_id",
    "case_id","depmap_id","sample","model","cell_id","cell_line","specimen_id"
]

def sanitize_prefix(path: str) -> str:
    stem = Path(path).stem
    stem = re.sub(r"_annotated$", "", stem)
    return re.sub(r"[^A-Za-z0-9]+", "_", stem).strip("_").lower()

def discover_inputs(in_dir: Optional[str]) -> List[str]:
    if not in_dir: return []
    pq = sorted(set(glob.glob(os.path.join(in_dir, "**", "*.parquet"), recursive=True)))
    csv = sorted(set(glob.glob(os.path.join(in_dir, "**", "*.csv"), recursive=True)))
    return pq + csv

def choose_sources(old_dir: Optional[str], v2_dir: Optional[str]) -> List[str]:
    chosen = {}
    for base, tag in ((v2_dir, "v2"), (old_dir, "old")):
        if not base: continue
        for p in discover_inputs(base):
            key = Path(p).with_suffix("").name.lower()
            if tag == "v2" or key not in chosen:
                chosen[key] = p
    return sorted(chosen.values())

def load_lazy(path: str) -> pl.LazyFrame:
    return pl.scan_parquet(path) if path.endswith(".parquet") else pl.scan_csv(path, infer_schema_length=5000)

def pick_id_column(cols_lower: List[str], candidates: List[str]) -> Optional[str]:
    lut = {c.lower(): c for c in cols_lower}
    for k in candidates:
        if k.lower() in lut: return k.lower()
    return None

def mk_conn(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("""CREATE TABLE IF NOT EXISTS features(
        sample_id TEXT NOT NULL,
        feature   TEXT NOT NULL,
        value_num REAL,
        value_txt TEXT,
        source    TEXT NOT NULL
    )""")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_feat_sample ON features(sample_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_feat_name   ON features(feature)")
    return conn

def insert_rows(conn: sqlite3.Connection, rows, batch_size: int):
    cur = conn.cursor()
    buf = []
    for r in rows:
        buf.append(r)
        if len(buf) >= batch_size:
            cur.executemany("INSERT INTO features VALUES (?,?,?,?,?)", buf)
            conn.commit(); buf.clear()
    if buf:
        cur.executemany("INSERT INTO features VALUES (?,?,?,?,?)", buf)
        conn.commit()

def chunk(lst: List[str], n: int):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def main():
    ap = argparse.ArgumentParser(description="Low-RAM feature store (column-batch streaming).")
    ap.add_argument("--in-old", default="", help="older structured_annotated dir (optional)")
    ap.add_argument("--in-v2",  default="", help="new structured_annotated_v2 dir (preferred)")
    ap.add_argument("--out",    required=True, help="output folder (e.g., data/04_model_ready)")
    ap.add_argument("--db-name", default="feature_store.sqlite")
    ap.add_argument("--id-candidates", nargs="+", default=DEFAULT_ID_CANDIDATES)
    ap.add_argument("--max-wide-cols", type=int, default=1200, help="skip files wider than this")
    ap.add_argument("--cols-per-batch", type=int, default=64, help="process this many columns at a time")
    ap.add_argument("--batch-insert", type=int, default=15000, help="SQLite insert batch size")
    ap.add_argument("--topk", type=int, default=400, help="materialize top-K prevalent features to a small wide table")
    ap.add_argument("--csv", choices=["flat","none"], default="flat")
    ap.add_argument("--threads", type=int, default=4, help="POLARS_MAX_THREADS limit")
    args = ap.parse_args()

    os.environ.setdefault("POLARS_MAX_THREADS", str(args.threads))
    os.makedirs(args.out, exist_ok=True)
    db_path = os.path.join(args.out, args.db_name)
    if os.path.exists(db_path): os.remove(db_path)
    conn = mk_conn(db_path)

    sources = choose_sources(args.in_old or None, args.in_v2 or None)
    if not sources:
        console.print("[red]No inputs discovered.[/red]"); return

    bar = BarColumn(bar_width=None)
    cols = [TextColumn("[progress.description]{task.description}"), bar, TimeElapsedColumn(), TimeRemainingColumn()]
    manifest = {"ingested": [], "skipped": []}

    with Progress(*cols, console=console) as prog:
        t = prog.add_task("[bold cyan]Building feature store (column-batch streaming)", total=len(sources))
        for path in sources:
            prefix = sanitize_prefix(path)
            try:
                lf = load_lazy(path)
                head = lf.fetch(25)
                if head.is_empty():
                    manifest["skipped"].append({"file": path, "reason": "empty"}); prog.advance(t); continue

                # Lower-case names once on the lazy frame
                rename_map = {c: c.lower() for c in head.columns}
                lf = lf.rename(rename_map)
                head = lf.fetch(25)  # refresh

                cols_all = list(head.columns)
                if len(cols_all) > args.max_wide_cols:
                    manifest["skipped"].append({"file": path, "reason": f"ultra_wide({len(cols_all)})"})
                    prog.advance(t); continue

                # detect id col
                id_col = pick_id_column(cols_all, args.id_candidates)
                if not id_col:
                    manifest["skipped"].append({"file": path, "reason": "no_id_column"})
                    prog.advance(t); continue

                feature_cols = [c for c in cols_all if c != id_col]
                if not feature_cols:
                    manifest["skipped"].append({"file": path, "reason": "no_features"}); prog.advance(t); continue

                # Process in small column batches
                total_rows_written = 0
                for cols_batch in chunk(feature_cols, args.cols_per_batch):
                    # aggregate within file per batch
                    # build per-column aggs based on sampled dtypes
                    sample = lf.select([id_col] + cols_batch).fetch(10)
                    aggs = []
                    for c, dt in zip(sample.columns, sample.dtypes):
                        if c == id_col: continue
                        if dt.is_numeric():
                            aggs.append(pl.col(c).cast(pl.Float64).mean().alias(c))
                        elif dt == pl.Boolean:
                            aggs.append(pl.col(c).any().alias(c))
                        else:
                            aggs.append(pl.col(c).cast(pl.Utf8).alias(c))
                    agg_lf = lf.select([id_col] + cols_batch).group_by(id_col).agg(aggs)
                    df = agg_lf.collect(streaming=True)  # small: (#ids, ≤cols_per_batch+1)

                    # prefix, flatten lists to text
                    df = df.rename({c: f"{prefix}__{c}" for c in df.columns if c != id_col})
                    if any(dt.is_list() for dt in df.dtypes):
                        fx = []
                        for c, dt in zip(df.columns, df.dtypes):
                            if c == id_col: continue
                            if dt.is_list():
                                fx.append(pl.col(c).cast(pl.List(pl.Utf8)).list.join("|").alias(c))
                        if fx: df = df.with_columns(fx)

                    # separate num vs text and melt to long
                    num_cols, txt_cols = [], []
                    for c, dt in zip(df.columns, df.dtypes):
                        if c == id_col: continue
                        (num_cols if dt.is_numeric() else txt_cols).append(c)

                    if num_cols:
                        m = df.select([id_col] + num_cols).melt(id_vars=id_col, variable_name="feature", value_name="vnum")
                        rows = ((sid, feat, float(val) if val is not None else None, None, prefix)
                                for sid, feat, val in m.iter_rows())
                        insert_rows(conn, rows, args.batch_insert)
                        total_rows_written += m.height
                    if txt_cols:
                        m = df.select([id_col] + txt_cols).melt(id_vars=id_col, variable_name="feature", value_name="vtxt")
                        rows = ((sid, feat, None, (val if val is not None else None), prefix)
                                for sid, feat, val in m.iter_rows())
                        insert_rows(conn, rows, args.batch_insert)
                        total_rows_written += m.height

                manifest["ingested"].append({"file": path, "rows_long": int(total_rows_written), "prefix": prefix})

            except Exception as e:
                manifest["skipped"].append({"file": path, "reason": f"error:{e}"})
            finally:
                prog.advance(t)

    # Optional: small wide view of top-K prevalent features
    if args.topk and args.topk > 0:
        console.print("[green]Materializing top-K wide view…[/green]")
        cur = conn.cursor()
        cur.execute("""
            SELECT feature, COUNT(DISTINCT sample_id) AS n
            FROM features
            WHERE value_num IS NOT NULL OR (value_txt IS NOT NULL AND value_txt <> '')
            GROUP BY feature ORDER BY n DESC LIMIT ?
        """, (args.topk,))
        feats = [r[0] for r in cur.fetchall()]
        if feats:
            q = f"""
              SELECT sample_id, feature,
                     value_num AS vnum,
                     CASE WHEN value_num IS NULL THEN value_txt ELSE NULL END AS vtxt
              FROM features WHERE feature IN ({",".join("?" for _ in feats)})
            """
            cur.execute(q, feats); rows = cur.fetchall()
            if rows:
                df = pl.from_records(rows, schema=["sample_id","feature","vnum","vtxt"])
                num = (df.filter(pl.col("vnum").is_not_null())
                         .pivot(values="vnum", index="sample_id", columns="feature").fill_null(None))
                txt = (df.filter(pl.col("vnum").is_null())
                         .pivot(values="vtxt", index="sample_id", columns="feature").fill_null(None))
                wide = num.join(txt, on="sample_id", how="outer")
                out_parq = os.path.join(args.out, "master_structured_topK.parquet")
                wide.write_parquet(out_parq)
                if args.csv != "none":
                    wide.write_csv(os.path.join(args.out, "master_structured_topK.csv"))

    with open(os.path.join(args.out, "feature_manifest.json"), "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)

    try: conn.execute("VACUUM;")
    except Exception: pass
    conn.close()
    console.rule("[bold green]Feature store built (column-batch streaming)")

if __name__ == "__main__":
    main()
