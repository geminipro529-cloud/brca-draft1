#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Low-memory master builder: writes a disk-backed feature store (SQLite) instead of one giant wide frame.

What it does
------------
- Discovers structured outputs in two trees (--in-old, --in-v2); v2 wins on stem collisions.
- For each file:
  * loads lazily (Polars scan), aggregates duplicate rows by sample_id,
  * prefixes non-id columns with file stem,
  * flattens list columns to pipe-joined strings,
  * SKIPS ultra-wide quantitative matrices (width > --max-wide-cols) to avoid OOM,
  * streams rows into SQLite table: features(sample_id, feature, value_num, value_txt, source)
- Creates indexes on (sample_id) and (feature).
- Optionally materializes a small wide Parquet with top-K features by prevalence.

Why this won’t crash
--------------------
- Work is file-by-file; nothing accumulates in RAM.
- SQLite is append-only here; commits happen in batches.
- No DataFrame-wide outer joins.

Outputs
-------
- data/04_model_ready/feature_store.sqlite
- data/04_model_ready/feature_manifest.json  (what was ingested/skipped)
- (optional) data/04_model_ready/master_structured_topK.parquet/.csv

"""

from __future__ import annotations
import argparse, os, glob, re, json, sqlite3, math
from pathlib import Path
from typing import List, Tuple, Optional

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

def discover_inputs(in_dir: Optional[str]) -> Tuple[List[str], List[str]]:
    if not in_dir: return [], []
    pq = sorted(set(glob.glob(os.path.join(in_dir, "**", "*.parquet"), recursive=True)))
    csv = sorted(set(glob.glob(os.path.join(in_dir, "**", "*.csv"), recursive=True)))
    return pq, csv

def choose_sources(old_dir: Optional[str], v2_dir: Optional[str]) -> List[str]:
    """v2 wins when stems collide; return list of paths (parquet first)."""
    chosen = {}
    for base, tag in ((v2_dir, "v2"), (old_dir, "old")):
        if not base: continue
        pq, csv = discover_inputs(base)
        for p in pq + csv:
            key = Path(p).with_suffix("").name.lower()
            # only add if not already claimed by v2
            if key not in chosen:
                chosen[key] = p if tag == "v2" else chosen.get(key, p)
    # prefer parquet if both parquet and csv exist with same stem (handled by dict already)
    return sorted(chosen.values())

def pick_id_column(df: pl.DataFrame, candidates: List[str]) -> Optional[str]:
    cols = {c.lower(): c for c in df.columns}
    for k in candidates:
        if k.lower() in cols:
            return cols[k.lower()]
    return None

def load_lazy(path: str) -> pl.LazyFrame:
    if path.endswith(".parquet"):
        return pl.scan_parquet(path)
    # CSV: let Polars infer; avoid reading everything at once
    return pl.scan_csv(path, infer_schema_length=5000)

def is_ultra_wide(df_head: pl.DataFrame, max_cols: int) -> bool:
    return df_head.width > max_cols

def agg_by_id_lazy(lf: pl.LazyFrame, id_col: str) -> pl.LazyFrame:
    # Build aggregations dynamically from schema
    schema = lf.fetch(50).schema
    aggs = []
    for c, dt in schema.items():
        if c == id_col: 
            continue
        if dt.is_numeric():
            aggs.append(pl.col(c).cast(pl.Float64).mean().alias(c))
        elif dt == pl.Boolean:
            aggs.append(pl.col(c).any().alias(c))
        else:
            # flatten lists to text; ensure Utf8 for melt later
            aggs.append(pl.col(c).cast(pl.Utf8).alias(c))
    return lf.group_by(id_col).agg(aggs)

def mk_conn(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS features (
            sample_id TEXT NOT NULL,
            feature   TEXT NOT NULL,
            value_num REAL,
            value_txt TEXT,
            source    TEXT NOT NULL
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_feat_sample ON features(sample_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_feat_name   ON features(feature)")
    return conn

def insert_rows(conn: sqlite3.Connection, rows: List[tuple]):
    conn.executemany("INSERT INTO features(sample_id, feature, value_num, value_txt, source) VALUES (?,?,?,?,?)", rows)

def to_batches(seq, size=10000):
    buf = []
    for x in seq:
        buf.append(x)
        if len(buf) >= size:
            yield buf
            buf = []
    if buf:
        yield buf

def main():
    ap = argparse.ArgumentParser(description="Build disk-backed feature store (low memory).")
    ap.add_argument("--in-old", default="", help="older structured_annotated dir (optional)")
    ap.add_argument("--in-v2",  default="", help="new structured_annotated_v2 dir (preferred)")
    ap.add_argument("--out",    required=True, help="output folder (e.g., data/04_model_ready)")
    ap.add_argument("--db-name", default="feature_store.sqlite", help="sqlite filename in --out")
    ap.add_argument("--id-candidates", nargs="+", default=DEFAULT_ID_CANDIDATES)
    ap.add_argument("--max-wide-cols", type=int, default=2000, help="skip files wider than this (quantitative lane)")
    ap.add_argument("--batch-insert",  type=int, default=20000, help="sqlite insert batch size")
    ap.add_argument("--topk", type=int, default=500, help="materialize top-K prevalent features to a small wide table")
    ap.add_argument("--csv", choices=["flat","none"], default="flat")
    args = ap.parse_args()

    os.makedirs(args.out, exist_ok=True)
    db_path = os.path.join(args.out, args.db_name)
    if os.path.exists(db_path):
        console.print(f"[yellow]Overwriting existing DB: {db_path}[/yellow]")
        os.remove(db_path)
    conn = mk_conn(db_path)

    sources = choose_sources(args.in_old or None, args.in_v2 or None)
    if not sources:
        console.print("[red]No inputs discovered.[/red]"); return

    bar = BarColumn(bar_width=None)
    cols = [TextColumn("[progress.description]{task.description}"), bar, TimeElapsedColumn(), TimeRemainingColumn()]
    manifest = {"ingested": [], "skipped": []}

    with Progress(*cols, console=console) as prog:
        t = prog.add_task("[bold cyan]Building feature store (streaming)", total=len(sources))
        for path in sources:
            prefix = sanitize_prefix(path)
            try:
                lf = load_lazy(path)
                head = lf.fetch(50)  # tiny sample to inspect schema & width
                if head.is_empty():
                    manifest["skipped"].append({"file": path, "reason": "empty"})
                    prog.advance(t); continue

                # Skip ultra-wide gene/protein matrices -> quantitative lane
                if is_ultra_wide(head, args.max_wide_cols):
                    manifest["skipped"].append({"file": path, "reason": f"ultra_wide({head.width})"})
                    prog.advance(t); continue

                # pick id column and aggregate
                # normalize column names to lowercase for id detection
                lf = lf.with_columns([pl.col(c).alias(c.lower()) for c in head.columns])
                head = lf.fetch(5)
                id_col = pick_id_column(head, args.id_candidates)
                if not id_col:
                    manifest["skipped"].append({"file": path, "reason": "no_id_column"})
                    prog.advance(t); continue

                # aggregate by id; ensure non-id columns are prefixed
                agg_lf = agg_by_id_lazy(lf, id_col)
                # collect in small row batches to avoid RAM spikes
                # We collect to a DataFrame per file (already aggregated -> #rows ~ #samples)
                df = agg_lf.collect(streaming=True)

                # prefix non-id features
                rename = {c: f"{prefix}__{c}" for c in df.columns if c != id_col}
                df = df.rename(rename)

                # flatten lists to text
                exprs = []
                for c, dt in zip(df.columns, df.dtypes):
                    if c == id_col: 
                        continue
                    if dt == pl.List(pl.Utf8) or dt == pl.List(pl.Float64) or dt == pl.List(pl.Int64):
                        exprs.append(pl.col(c).cast(pl.List(pl.Utf8)).list.join("|").alias(c))
                if exprs:
                    df = df.with_columns(exprs)

                # write to SQLite in long (sample_id, feature, value_num/value_txt, source)
                num_cols, txt_cols = [], []
                for c, dt in zip(df.columns, df.dtypes):
                    if c == id_col: continue
                    if dt.is_numeric():
                        num_cols.append(c)
                    else:
                        txt_cols.append(c)

                # numeric block
                if num_cols:
                    melt = df.select([id_col] + num_cols).melt(id_vars=id_col, variable_name="feature", value_name="valnum")
                    # prepare rows
                    rows = ((sid, feat, float(val) if val is not None else None, None, prefix)
                            for sid, feat, val in melt.iter_rows())
                    for batch in to_batches(rows, size=args.batch_insert):
                        insert_rows(conn, batch)
                        conn.commit()

                # text block
                if txt_cols:
                    melt = df.select([id_col] + txt_cols).melt(id_vars=id_col, variable_name="feature", value_name="valtxt")
                    rows = ((sid, feat, None, (val if val is not None else None), prefix)
                            for sid, feat, val in melt.iter_rows())
                    for batch in to_batches(rows, size=args.batch_insert):
                        insert_rows(conn, batch)
                        conn.commit()

                manifest["ingested"].append({"file": path, "rows": int(df.height), "cols": int(df.width)})
            except Exception as e:
                manifest["skipped"].append({"file": path, "reason": f"error:{e}"})
            finally:
                prog.advance(t)

    # Materialize a small top-K wide view (optional)
    if args.topk and args.topk > 0:
        console.print("[green]Materializing top-K wide view…[/green]")
        cur = conn.cursor()
        # Prevalence = #samples with non-null value (num or text)
        cur.execute("""
            SELECT feature, COUNT(DISTINCT sample_id) AS n
            FROM features
            WHERE value_num IS NOT NULL OR (value_txt IS NOT NULL AND value_txt <> '')
            GROUP BY feature
            ORDER BY n DESC
            LIMIT ?
        """, (args.topk,))
        feats = [r[0] for r in cur.fetchall()]
        if feats:
            # Pull as Polars frame, then pivot
            # We try to cast numeric first; otherwise keep text.
            q = f"""
                SELECT sample_id, feature,
                       CASE WHEN value_num IS NOT NULL THEN value_num ELSE NULL END AS vnum,
                       CASE WHEN value_num IS NULL THEN value_txt ELSE NULL END AS vtxt
                FROM features
                WHERE feature IN ({",".join("?" for _ in feats)})
            """
            cur.execute(q, feats)
            rows = cur.fetchall()
            if rows:
                df = pl.from_records(rows, schema=["sample_id","feature","vnum","vtxt"])
                # prefer numeric
                num = (df.filter(pl.col("vnum").is_not_null())
                         .pivot(values="vnum", index="sample_id", columns="feature").fill_null(None))
                txt = (df.filter(pl.col("vnum").is_null())
                         .pivot(values="vtxt", index="sample_id", columns="feature").fill_null(None))
                wide = num.join(txt, on="sample_id", how="outer")
                out_parq = os.path.join(args.out, "master_structured_topK.parquet")
                wide.write_parquet(out_parq)
                if args.csv != "none":
                    # ensure text columns are safe for CSV
                    flat = wide
                    for c, dt in zip(flat.columns, flat.dtypes):
                        if dt == pl.List(pl.Utf8):
                            flat = flat.with_columns(pl.col(c).list.join("|").alias(c))
                    flat.write_csv(os.path.join(args.out, "master_structured_topK.csv"))

    # Write manifest
    with open(os.path.join(args.out, "feature_manifest.json"), "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)

    # Vacuum to compact
    try:
        conn.execute("VACUUM;")
    except Exception:
        pass
    conn.close()
    console.rule("[bold green]Feature store built (low memory)")
    
if __name__ == "__main__":
    main()
