
"""
Collapse sentence-level relations (subject, predicate, object) into weighted edges.

Inputs
- --relations : relations.parquet/csv from extract_unstructured_from_pdf.py
  (expects columns: doc_relpath, subject, predicate, object, negated, confidence, section[, qualifiers_json])

Outputs
- edges.parquet (+ .csv if --csv flat)
- edges_report.md (counts + basic stats)

Weights
- --weight doc|sent|tfidf  (default: doc)
  doc   = #unique documents supporting the edge
  sent  = #sentences supporting the edge
  tfidf = doc_count * mean_conf * log(1 + total_docs/(1+doc_count))   (simple IDF)
"""

from __future__ import annotations
import argparse, os, math
import polars as pl

def maybe_read(p: str) -> pl.DataFrame:
    if not os.path.exists(p): return pl.DataFrame({"_":[]})
    return pl.read_parquet(p) if p.lower().endswith(".parquet") else pl.read_csv(p)

def main():
    ap = argparse.ArgumentParser(description="Collapse unstructured relations to weighted edges.")
    ap.add_argument("--relations", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--weight", choices=["doc","sent","tfidf"], default="doc")
    ap.add_argument("--min-conf", type=float, default=0.0)
    ap.add_argument("--min-docs", type=int, default=1)
    ap.add_argument("--csv", choices=["flat","none"], default="flat")
    args = ap.parse_args()

    os.makedirs(args.out, exist_ok=True)
    R = maybe_read(args.relations)
    if R.is_empty():
        print("[WARN] No relations to collapse."); return

    # required columns check (be forgiving on types)
    need = {"subject","predicate","object"}
    missing = [c for c in need if c not in R.columns]
    if missing:
        raise SystemExit(f"relations file missing columns: {missing}")

    # normalization
    cols = {c.lower(): c for c in R.columns}
    def has(c): return c in R.columns
    # some columns are optional
    conf_col = "confidence" if "confidence" in R.columns else None
    neg_col  = "negated" if "negated" in R.columns else None
    doc_col  = "doc_relpath" if "doc_relpath" in R.columns else None
    sec_col  = "section" if "section" in R.columns else None

    # filter by confidence if present
    if conf_col:
        R = R.filter(pl.col(conf_col).cast(pl.Float64) >= args.min_conf)

    # group to edges
    gb = ["subject","predicate","object"]
    aggs = [
        pl.len().alias("sentence_count"),
    ]
    if doc_col:
        aggs.append(pl.col(doc_col).n_unique().alias("doc_count"))
    else:
        aggs.append(pl.lit(None).alias("doc_count"))
    if conf_col:
        aggs.append(pl.col(conf_col).cast(pl.Float64).mean().alias("mean_conf"))
    else:
        aggs.append(pl.lit(None).alias("mean_conf"))
    if neg_col:
        aggs.append(pl.col(neg_col).cast(pl.Int8).mean().alias("neg_frac"))
    else:
        aggs.append(pl.lit(None).alias("neg_frac"))
    if sec_col:
        aggs.append(pl.col(sec_col).unique().alias("sections"))
    else:
        aggs.append(pl.lit(None).alias("sections"))

    E = R.group_by(gb).agg(aggs)

    # compute weight
    total_docs = int(R.select(pl.col(doc_col)).n_unique()) if doc_col else None
    if args.weight == "doc":
        E = E.with_columns(pl.col("doc_count").fill_null(0).alias("weight"))
    elif args.weight == "sent":
        E = E.with_columns(pl.col("sentence_count").alias("weight"))
    else:
        # tfidf-like
        if doc_col:
            E = E.with_columns([
                pl.when(pl.col("doc_count").is_null()).then(0).otherwise(pl.col("doc_count")).alias("__dc"),
                pl.when(pl.col("mean_conf").is_null()).then(0.7).otherwise(pl.col("mean_conf")).alias("__mc"),
            ])
            E = E.with_columns( (pl.col("__dc") * pl.col("__mc") *
                                 pl.lit(math.log1p((total_docs or 1) / (1 + 1))))  # small positive idf if total_docs tiny
                               .alias("weight") ).drop(["__dc","__mc"])
        else:
            E = E.with_columns(pl.col("sentence_count").alias("weight"))

    # minimal filters
    E = E.filter(pl.col("subject").is_not_null() & pl.col("object").is_not_null() & (pl.col("subject") != pl.col("object")))
    if args.min_docs > 1 and "doc_count" in E.columns:
        E = E.filter(pl.col("doc_count") >= args.min_docs)

    # write
    out_parq = os.path.join(args.out, "edges.parquet")
    E.write_parquet(out_parq)
    if args.csv != "none":
        E_out = E.with_columns([
            pl.col("sections").cast(pl.List(pl.Utf8)).list.join("|").alias("sections")
        ])
        E_out.write_csv(os.path.join(args.out, "edges.csv"))

    # quick report
    with open(os.path.join(args.out, "edges_report.md"), "w", encoding="utf-8") as f:
        f.write("# Collapsed edges report\n\n")
        f.write(f"- Unique edges: **{E.height}**\n")
        if "doc_count" in E.columns and not E.select("doc_count").is_empty():
            f.write(f"- Median doc_count: **{E.select(pl.col('doc_count')).median()}**\n")
        if "mean_conf" in E.columns and not E.select('mean_conf').is_empty():
            f.write(f"- Mean confidence (avg across edges): **{float(E.select(pl.col('mean_conf')).mean().item()):.3f}**\n")

    print(f"[OK] Wrote edges to {out_parq}")

if __name__ == "__main__":
    main()
