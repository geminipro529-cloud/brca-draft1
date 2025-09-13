
from __future__ import annotations
import argparse, os
import polars as pl
from rich.console import Console

console = Console()

def load_vocab(vp: str) -> set[str]:
    v = pl.read_parquet(vp) if vp.lower().endswith(".parquet") else pl.read_csv(vp)
    col = "canonical" if "canonical" in v.columns else "term"
    return set([str(x).lower() for x in v[col].drop_nulls().to_list()])

def maybe_read(p: str) -> pl.DataFrame:
    if not os.path.exists(p): return pl.DataFrame({"_": []})
    return pl.read_parquet(p) if p.lower().endswith(".parquet") else pl.read_csv(p)

def main():
    ap = argparse.ArgumentParser(description="Report vocabulary coverage & gaps across outputs.")
    ap.add_argument("--vocab", required=True, help="vocabulary_enriched.parquet/csv")
    ap.add_argument("--structured-dir", required=True, help="data/02_interim/structured_annotated_v2 (or merged)")
    ap.add_argument("--unstruct-entities", required=True, help="entities.parquet from unstructured extractor")
    ap.add_argument("--unstruct-relations", required=True, help="relations.parquet from unstructured extractor")
    ap.add_argument("--out", required=True, help="output folder for reports")
    args = ap.parse_args()

    os.makedirs(args.out, exist_ok=True)
    vocab = load_vocab(args.vocab)

    # ---------- UNSTRUCTURED ----------
    E = maybe_read(args.unstruct_entities)
    R = maybe_read(args.unstruct_relations)

    un_known = pl.DataFrame({"ent_norm": []})
    if not E.is_empty():
        # Unknown entities = missing in vocab OR ent_type unknown
        en = E.select([
            pl.col("ent_norm").cast(pl.Utf8).str.to_lowercase().alias("ent_norm"),
            pl.col("ent_type").cast(pl.Utf8).fill_null("unknown").alias("ent_type")
        ]).drop_nulls()
        un_known = (en
            .with_columns(pl.when(pl.col("ent_norm").is_in(list(vocab))).then(pl.lit(0)).otherwise(pl.lit(1)).alias("__miss"))
            .filter((pl.col("__miss")==1) | (pl.col("ent_type")=="unknown"))
            .group_by(["ent_norm","ent_type"]).len().rename({"len":"n"})
            .sort(["n"], descending=True)
        )

    # ---------- STRUCTURED ----------
    # Pull any normalized_terms pipe-joined from CSV outputs if present
    import glob
    struct_csvs = glob.glob(os.path.join(args.structured_dir, "**", "*.csv"), recursive=True)
    S = []
    for p in struct_csvs:
        try:
            df = pl.read_csv(p)
            if "normalized_terms" in df.columns:
                S.append(df.select(pl.col("normalized_terms").cast(pl.Utf8)))
        except Exception:
            pass
    struct_miss = pl.DataFrame({"term": [], "n": []})
    if S:
        s = pl.concat(S, how="diagonal_relaxed")
        terms = (s.select(pl.col("normalized_terms").cast(pl.Utf8))
                   .drop_nulls()
                   .with_columns(pl.col("normalized_terms").str.split("|"))
                   .explode("normalized_terms")
                   .drop_nulls()
                   .with_columns(pl.col("normalized_terms").str.to_lowercase().alias("t"))
                   .group_by("t").len().rename({"t":"term","len":"n"})
                   .sort("n", descending=True)
               )
        struct_miss = terms.filter(~pl.col("term").is_in(list(vocab)))

    # ---------- suggest term_type heuristic ----------
    def guess_type(term: str) -> str:
        low = term.lower()
        if any(x in low for x in (" pathway"," signaling","signalling")) or low.startswith("pi3k"):
            return "pathway"
        if any(x in low for x in (" carcinoma"," cancer"," tumor"," tumour"," neoplasm"," syndrome")):
            return "disease"
        if any(x in low for x in (" inhibitor"," antibody"," drug"," therapeutic"," therapy")):
            return "drug"
        # crude gene-ish hint
        if low.isupper() and len(low) <= 6:
            return "gene"
        return "unknown"

    # combine gaps and propose types
    gaps = []
    if not un_known.is_empty():
        for t, tt, n in zip(un_known["ent_norm"].to_list(), un_known["ent_type"].to_list(), un_known["n"].to_list()):
            gaps.append({"candidate": t, "suggested_type": guess_type(t) if tt=="unknown" else tt, "count": int(n), "source": "unstructured"})
    if not struct_miss.is_empty():
        for t, n in zip(struct_miss["term"].to_list(), struct_miss["n"].to_list()):
            gaps.append({"candidate": t, "suggested_type": guess_type(t), "count": int(n), "source": "structured"})
    G = pl.DataFrame(gaps).group_by(["candidate","suggested_type"]).agg([
        pl.col("count").sum().alias("total_count"),
        pl.col("source").unique().alias("sources"),
    ]).sort("total_count", descending=True)

    out_csv = os.path.join(args.out, "vocab_gaps.csv")
    G.with_columns(pl.col("sources").list.join("|").alias("sources")).write_csv(out_csv)

    with open(os.path.join(args.out, "vocab_coverage_report.md"), "w", encoding="utf-8") as f:
        f.write("# Vocabulary coverage\n\n")
        f.write(f"- Unstructured entities total: **{E.height if not E.is_empty() else 0}**\n")
        f.write(f"- Relations total: **{R.height if not R.is_empty() else 0}**\n")
        if not G.is_empty():
            f.write(f"- Gap candidates: **{G.height}** → see vocab_gaps.csv\n")

    console.rule("[bold green]Vocab gap analysis complete")

if __name__ == "__main__":
    main()
