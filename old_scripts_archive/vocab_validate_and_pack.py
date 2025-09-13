
from __future__ import annotations
import argparse, os, json, unicodedata, re
import polars as pl
from rich.console import Console

console = Console()

GREEK = {
    "α":"alpha", "β":"beta", "γ":"gamma", "δ":"delta", "ε":"epsilon",
    "κ":"kappa", "λ":"lambda", "μ":"mu", "ν":"nu", "π":"pi", "ρ":"rho",
    "σ":"sigma", "τ":"tau", "φ":"phi", "χ":"chi", "ψ":"psi", "ω":"omega"
}

DEFAULT_STOP = {
    "the","and","of","to","in","for","with","by","on","as","at","or","an",
    "patient","sample","case","unknown","na","n/a"
}

def uclean(s: str) -> str:
    if s is None: return ""
    s = unicodedata.normalize("NFKC", str(s))
    s = re.sub(r"\s+", " ", s.strip())
    return s

def greek_fold(s: str) -> str:
    return "".join(GREEK.get(ch, ch) for ch in s)

def surface_variants(t: str) -> list[str]:
    # minimal variants to improve recall without exploding FP
    c = t
    outs = {c}
    outs.add(c.replace("-", " "))
    outs.add(c.replace(" ", "-"))
    outs.add(greek_fold(c))
    outs.add(greek_fold(c).replace("-", " "))
    return sorted({o for o in outs if len(o) >= 3})

def main():
    ap = argparse.ArgumentParser(description="Validate & pack vocabulary for matching.")
    ap.add_argument("--in", dest="in_path", required=True, help="vocabulary.csv or .parquet")
    ap.add_argument("--outdir", required=True, help="output dir (e.g., data/02_interim/VOCAB_COMPILED)")
    ap.add_argument("--add-stop", nargs="*", default=[], help="extra stopwords to drop")
    args = ap.parse_args()

    os.makedirs(args.outdir, exist_ok=True)
    ext = os.path.splitext(args.in_path)[1].lower()
    v = pl.read_parquet(args.in_path) if ext == ".parquet" else pl.read_csv(args.in_path)

    # normalize columns
    if "term" not in v.columns:
        v = v.rename({v.columns[0]: "term"})
    if "term_type" not in v.columns:
        v = v.with_columns(pl.lit("unknown").alias("term_type"))
    if "canonical" not in v.columns:
        v = v.with_columns(pl.lit(None).alias("canonical"))

    # clean + lowercase helper columns
    v = v.with_columns([
        pl.col("term").cast(pl.Utf8).map_elements(uclean, return_dtype=pl.Utf8).alias("term"),
        pl.col("canonical").cast(pl.Utf8).map_elements(uclean, return_dtype=pl.Utf8).alias("canonical"),
        pl.col("term_type").cast(pl.Utf8).map_elements(uclean, return_dtype=pl.Utf8).alias("term_type"),
    ])
    v = v.filter(pl.col("term").str.len_bytes() >= 3)

    # fill canonical
    v = v.with_columns(pl.when(pl.col("canonical").is_null() | (pl.col("canonical")==""))
                       .then(pl.col("term")).otherwise(pl.col("canonical")).alias("canonical"))

    # stopwords
    stops = {s.lower() for s in DEFAULT_STOP | set(args.add_stop)}
    v = v.with_columns(pl.col("term").str.to_lowercase().alias("__lc"))
    v = v.filter(~pl.col("__lc").is_in(list(stops))).drop("__lc")

    # deduplicate by (canonical, term_type)
    v = v.unique(subset=["canonical","term_type"], keep="first")

    # create minimal variant list for matching
    rows = []
    for term, canon, ttype in zip(v["term"].to_list(), v["canonical"].to_list(), v["term_type"].to_list()):
        for s in surface_variants(term):
            rows.append({"surface": s, "canonical": canon, "term_type": ttype})
    pack = pl.DataFrame(rows).unique(subset=["surface","term_type"])

    # write enriched vocab + pack
    v.write_parquet(os.path.join(args.outdir, "vocabulary_enriched.parquet"))
    v.write_csv(os.path.join(args.outdir, "vocabulary_enriched.csv"))

    pack_path = os.path.join(args.outdir, "vocab_keywords.json")
    with open(pack_path, "w", encoding="utf-8") as f:
        json.dump(pack.to_dicts(), f, ensure_ascii=False, indent=2)

    # tiny report
    by_t = v.group_by("term_type").len().rename({"len":"n"}).sort("n", descending=True)
    with open(os.path.join(args.outdir, "vocab_report.md"), "w", encoding="utf-8") as f:
        f.write("# Vocabulary report\n\n")
        f.write(f"- Terms: **{v.height}**\n")
        f.write(f"- Match surfaces: **{pack.height}**\n\n")
        if not by_t.is_empty():
            f.write("## Counts by term_type\n\n")
            f.write(by_t.to_pandas().to_markdown(index=False))

    console.rule("[bold green]Vocabulary validated & packed")

if __name__ == "__main__":
    main()
