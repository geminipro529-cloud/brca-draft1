#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Annotate structured datasets with compiled vocabulary (robust Polars version)
- Recurses all subfolders and supports .csv/.tsv/.txt/.parquet
- Auto-detects text-like columns when your --text-cols don't exist
- Substring matching via FlashText; safe regex fallback if FlashText missing
- Writes Parquet (authoritative, list columns OK) + flattened CSV (joins lists with '|')
- Adds source_relpath/source_dir/source_file for provenance
"""

from __future__ import annotations
import argparse, os, re, glob, sys
from pathlib import Path
from typing import Dict, List, Tuple

import polars as pl
from rich.console import Console
from rich.progress import Progress, BarColumn, TimeElapsedColumn, TimeRemainingColumn, TextColumn

console = Console()

# ----------------------- Matchers -----------------------

try:
    from flashtext import KeywordProcessor
except Exception:
    KeywordProcessor = None

class FlashMatcher:
    def __init__(self, terms: List[str], canon: Dict[str, str]):
        if KeywordProcessor is None:
            raise RuntimeError("flashtext not installed")
        self.raw = KeywordProcessor(case_sensitive=False)
        self.norm = KeywordProcessor(case_sensitive=False)
        for kp in (self.raw, self.norm):
            kp.add_non_word_boundary("-"); kp.add_non_word_boundary("/")
        for t in terms:
            if not isinstance(t, str) or not t:
                continue
            self.raw.add_keyword(t)
            self.norm.add_keyword(t, canon.get(t.lower(), t))

    def extract(self, s: str) -> Tuple[List[str], List[str]]:
        if not s:
            return [], []
        return self.raw.extract_keywords(s), self.norm.extract_keywords(s)

class RegexChunkMatcher:
    def __init__(self, terms: List[str], canon: Dict[str, str], chunk: int = 50):
        safe = [re.escape(t) for t in terms if isinstance(t, str) and t]
        safe.sort(key=len, reverse=True)
        self._pats = [
            re.compile(r"(?i)\b(" + "|".join(safe[i:i + chunk]) + r")\b")
            for i in range(0, len(safe), chunk)
        ] if safe else []
        self._canon = canon

    def extract(self, s: str) -> Tuple[List[str], List[str]]:
        if not s or not self._pats:
            return [], []
        raw: List[str] = []
        for p in self._pats:
            raw.extend(p.findall(s))
        # unique preserve order
        seen = set()
        raw_u = [x for x in raw if not (x in seen or seen.add(x))]
        norm = [self._canon.get(x.lower(), x) for x in raw_u]
        return raw_u, norm

def build_matcher(terms: List[str], canon: Dict[str, str]):
    try:
        if KeywordProcessor is None:
            raise RuntimeError
        return FlashMatcher(terms, canon)
    except Exception:
        return RegexChunkMatcher(terms, canon)

# ----------------------- Vocab -----------------------

def load_vocab(vocab_path: Path) -> Tuple[List[str], Dict[str, str]]:
    if vocab_path.is_dir():
        pq = vocab_path / "vocabulary.parquet"
        cs = vocab_path / "vocabulary.csv"
        if pq.exists():
            vocab_path = pq
        elif cs.exists():
            vocab_path = cs
        else:
            raise FileNotFoundError(f"No vocab file in {vocab_path}")

    if not vocab_path.exists():
        raise FileNotFoundError(f"Vocab not found: {vocab_path}")

    if vocab_path.suffix.lower() == ".parquet":
        v = pl.read_parquet(vocab_path)
    elif vocab_path.suffix.lower() == ".csv":
        v = pl.read_csv(vocab_path)
    else:
        raise ValueError(f"Unsupported vocab format: {vocab_path.suffix}")

    if "term" not in v.columns:
        # assume first column is term
        v = v.rename({v.columns[0]: "term"})

    v = (v.select(
            pl.col("term").cast(pl.Utf8).str.strip().alias("term"),
            (pl.col("canonical").cast(pl.Utf8) if "canonical" in v.columns else pl.lit(None).alias("canonical"))
        )
        .drop_nulls(subset=["term"])
        .unique(subset=["term"], keep="first")
    )

    terms = v["term"].to_list()
    canon_vals = v["canonical"].fill_null(v["term"]).to_list()
    canon = { (t.lower() if isinstance(t, str) else t): c for t, c in zip(terms, canon_vals) }

    console.log(f"Loaded vocab: {len(terms)} terms from {vocab_path}")
    return terms, canon

# ----------------------- IO helpers -----------------------

def relpath_unix(p: str, root: str) -> str:
    try:
        rp = os.path.relpath(p, root)
    except ValueError:
        rp = p
    return rp.replace("\\", "/")

def discover_files(in_root: str) -> List[str]:
    pats = ("**/*.csv", "**/*.tsv", "**/*.txt", "**/*.parquet")
    files: List[str] = []
    for pat in pats:
        files.extend(glob.glob(os.path.join(in_root, pat), recursive=True))
    return sorted(set(files))

def read_head_for_text_cols(path: str, user_text_cols: List[str]) -> List[str]:
    """
    Peek a small chunk to infer reasonable text-like columns if user_text_cols don't exist.
    Strategy:
      - if any of user_text_cols present -> use their intersection
      - else take columns inferred as Utf8 in a small read (n_rows=200)
      - else fallback to ALL columns cast to Utf8
    """
    ext = os.path.splitext(path)[1].lower()
    try:
        if ext == ".parquet":
            head = pl.read_parquet(path, n_rows=200)
        elif ext in (".tsv", ".txt"):
            head = pl.read_csv(path, separator="\t", n_rows=200, ignore_errors=True)
        else:
            head = pl.read_csv(path, n_rows=200, ignore_errors=True)
    except Exception:
        return []  # we'll handle later

    cols = head.columns
    sel = [c for c in user_text_cols if c in cols]
    if sel:
        return sel

    utf8_cols = [c for c, dt in zip(head.columns, head.dtypes) if dt == pl.Utf8]
    if utf8_cols:
        return utf8_cols

    return list(cols)  # fallback: everything

def scan_lazy(path: str) -> pl.LazyFrame | None:
    ext = os.path.splitext(path)[1].lower()
    try:
        if ext == ".parquet":
            return pl.scan_parquet(path)
        elif ext in (".tsv", ".txt"):
            return pl.scan_csv(path, separator="\t", ignore_errors=True)
        elif ext in (".csv",):
            return pl.scan_csv(path, ignore_errors=True)
        else:
            return None
    except Exception as e:
        console.log(f"[yellow]Skip {path}[/yellow]: {e}")
        return None

# ----------------------- Annotation -----------------------

def build_blob_expr(cols: List[str]) -> pl.Expr:
    return pl.concat_str([pl.col(c).cast(pl.Utf8) for c in cols], separator=" ", ignore_nulls=True).alias("__blob")

def annotate_file(path: str, matcher, out_dir: str, in_root: str, user_text_cols: List[str]) -> None:
    rel = relpath_unix(path, in_root)
    src_dir = os.path.dirname(rel)
    src_file = os.path.basename(rel)
    base = os.path.splitext(src_file)[0]

    dest_dir = os.path.join(out_dir, src_dir.replace("/", os.sep)) if src_dir else out_dir
    os.makedirs(dest_dir, exist_ok=True)
    out_base = os.path.join(dest_dir, base)

    lf = scan_lazy(path)
    if lf is None:
        # write a stub for coverage
        stub = pl.DataFrame({
            "mentions": [[]],
            "normalized_terms": [[]],
            "source_relpath": [rel],
            "source_dir": [src_dir],
            "source_file": [src_file],
        })
        stub.write_parquet(out_base + ".parquet")
        stub.with_columns(
            pl.lit("").alias("mentions"),
            pl.lit("").alias("normalized_terms"),
        ).write_csv(out_base + ".csv")
        console.log(f"[yellow]Wrote stub (unreadable):[/yellow] {rel}")
        return

    # infer text columns
    cols = read_head_for_text_cols(path, user_text_cols)
    if not cols:
        # still try to collect small sample to know columns
        try:
            head = lf.fetch(50)
            cols = head.columns
        except Exception:
            cols = []
    if not cols:
        console.log(f"[yellow]No columns detected:[/yellow] {rel}")
        # write empty but valid outputs
        empty = lf.select([]).collect()
        empty = empty.with_columns([
            pl.lit(rel).alias("source_relpath"),
            pl.lit(src_dir).alias("source_dir"),
            pl.lit(src_file).alias("source_file"),
            pl.lit([]).alias("mentions"),
            pl.lit([]).alias("normalized_terms"),
        ])
        empty.write_parquet(out_base + ".parquet")
        empty.select([
            pl.col("source_relpath"), pl.col("source_dir"), pl.col("source_file"),
            pl.lit("").alias("mentions"), pl.lit("").alias("normalized_terms")
        ]).write_csv(out_base + ".csv")
        return

    # Build blob and extract mentions using map_elements (no deprecated .apply)
    lf2 = lf.with_columns([build_blob_expr(cols)])

    def _extract_raw(s: str) -> List[str]:
        r, _ = matcher.extract(s or "")
        # header supplement: also scan headers (helps sparse tables)
        return list(dict.fromkeys(r))

    def _extract_norm(s: str) -> List[str]:
        _, n = matcher.extract(s or "")
        return list(dict.fromkeys(n))

    lf2 = lf2.with_columns([
        pl.col("__blob").map_elements(_extract_raw, return_dtype=pl.List(pl.Utf8)).alias("mentions"),
        pl.col("__blob").map_elements(_extract_norm, return_dtype=pl.List(pl.Utf8)).alias("normalized_terms"),
        pl.lit(rel).alias("source_relpath"),
        pl.lit(src_dir).alias("source_dir"),
        pl.lit(src_file).alias("source_file"),
    ]).drop(["__blob"])

    # Collect once
    df = lf2.collect(streaming=True)

    # Write Parquet (authoritative)
    df.write_parquet(out_base + ".parquet")

    # Flatten for CSV
    flat = df.with_columns([
        pl.when(pl.col("mentions").is_null()).then(pl.lit("")).otherwise(pl.col("mentions").cast(pl.List(pl.Utf8)).list.join("|")).alias("mentions"),
        pl.when(pl.col("normalized_terms").is_null()).then(pl.lit("")).otherwise(pl.col("normalized_terms").cast(pl.List(pl.Utf8)).list.join("|")).alias("normalized_terms"),
    ])
    flat.write_csv(out_base + ".csv")

    console.log(f"[green]Annotated:[/green] {rel}")

# ----------------------- CLI -----------------------

def main():
    ap = argparse.ArgumentParser(description="Annotate structured files with compiled vocabulary (robust).")
    ap.add_argument("--in", dest="in_dir", required=True, help="Folder with structured inputs")
    ap.add_argument("--vocab", dest="vocab_path", required=True, help="Compiled vocabulary (CSV/Parquet or folder)")
    ap.add_argument("--out", dest="out_dir", required=True, help="Output folder")
    ap.add_argument("--text-cols", nargs="+", default=["term", "symbol", "name", "label", "code"],
                    help="Candidate text columns (fallback to auto-detect if absent)")
    args = ap.parse_args()

    in_root = os.path.abspath(args.in_dir)
    out_dir = os.path.abspath(args.out_dir)
    os.makedirs(out_dir, exist_ok=True)

    terms, canon = load_vocab(Path(args.vocab_path))
    matcher = build_matcher(terms, canon)

    files = discover_files(in_root)
    if not files:
        console.print(f"[yellow]No supported files under {in_root}[/yellow]")
        sys.exit(0)

    bar = BarColumn(bar_width=None)
    cols = [TextColumn("[progress.description]{task.description}"), bar, TimeElapsedColumn(), TimeRemainingColumn()]
    with Progress(*cols, console=console) as prog:
        t = prog.add_task("[bold cyan]Annotating structured files", total=len(files))
        for f in files:
            annotate_file(f, matcher, out_dir, in_root, args.text_cols)
            prog.advance(t)

    console.rule("[bold green]Structured annotation complete")

if __name__ == "__main__":
    main()
