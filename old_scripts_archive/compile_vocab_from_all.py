# scripts/compile_vocab_from_all.py
# Fully refactored for maximum compatibility & traceability + Rich progress.

from __future__ import annotations

import argparse
import csv
import gzip
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

import pandas as pd
import yaml
from rich.console import Console
from rich.progress import Progress, BarColumn, TimeElapsedColumn, TimeRemainingColumn, TextColumn, TaskID

try:
    import polars as pl  # optional acceleration
except Exception:
    pl = None  # keep optional

# ---------------------------- setup ----------------------------
console = Console()
JSONLike = Union[dict, list, str, int, float, bool, None]


@dataclass
class Opts:
    engine: str = "pandas"         # "pandas" | "polars"
    chunksize: int = 200_000
    row_limit: int = 0
    sample_frac: float = 1.0
    verbose_skips: bool = False
    dedupe_scope: str = "by-source"  # "by-source" | "global"


# ------------------------ config helpers -----------------------
def load_config(p: Path) -> dict:
    with p.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    if not isinstance(data, dict):
        raise ValueError("config.yaml must be a dict at top level.")
    return data


def cfg_get(d: dict, path: List[str], default=None):
    cur = d
    for k in path:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


def as_list_str(x, default=None) -> List[str]:
    if isinstance(x, (list, tuple)):
        return [str(i) for i in x]
    return default or []


def normalize_term(t: str) -> str:
    # keep visible characters; normalize whitespace
    s = "".join(ch for ch in str(t).strip() if ch.isprintable())
    return " ".join(s.split())


def rel_unix(path: Path, root: Path) -> str:
    try:
        rp = os.path.relpath(str(path), str(root))
    except ValueError:
        rp = str(path)
    return rp.replace("\\", "/")


def split_dir_file(relpath: str) -> Tuple[str, str]:
    d = os.path.dirname(relpath)
    b = os.path.basename(relpath)
    return d, b


# ------------------------ column inference ---------------------
def infer_candidates(df: pd.DataFrame, candidates: List[str]) -> List[str]:
    if df is None or df.empty:
        return []
    cols = [str(c).lower() for c in df.columns]
    hits = [c for c in candidates if c in cols]
    if hits:
        return hits
    return [cols[0]] if cols else []


# ------------------------ pandas readers -----------------------
def pandas_stream_terms(
    path: Path,
    sep: Optional[str],
    term_cols: List[str],
    min_len: int,
    chunksize: int,
    sample_frac: float,
    row_limit: int,
) -> pd.DataFrame:
    rows: List[Dict[str, str]] = []
    total = 0
    for chunk in pd.read_csv(
        path,
        sep=sep,
        dtype=str,
        encoding="utf-8",
        on_bad_lines="skip",
        engine="python",
        chunksize=chunksize,
    ):
        chunk.columns = [str(c).lower() for c in chunk.columns]
        cols = infer_candidates(chunk, term_cols)
        if not cols:
            continue
        if 0 < sample_frac < 1.0:
            chunk = chunk.sample(frac=sample_frac, random_state=42)
        for _, rec in chunk.iterrows():
            for c in cols:
                v = rec.get(c)
                if pd.isna(v) or not str(v).strip():
                    continue
                t = normalize_term(str(v))
                if len(t) < min_len:
                    continue
                rows.append({"term": t})
                total += 1
                if row_limit and total >= row_limit:
                    return pd.DataFrame(rows)
    return pd.DataFrame(rows)


def pandas_stream_terms_gz(
    path: Path,
    sep: Optional[str],
    term_cols: List[str],
    min_len: int,
    chunksize: int,
    sample_frac: float,
    row_limit: int,
) -> pd.DataFrame:
    rows: List[Dict[str, str]] = []
    total = 0
    with gzip.open(path, "rt", encoding="utf-8", errors="ignore") as fh:
        for chunk in pd.read_csv(
            fh,
            sep=sep,
            dtype=str,
            on_bad_lines="skip",
            engine="python",
            chunksize=chunksize,
        ):
            chunk.columns = [str(c).lower() for c in chunk.columns]
            cols = infer_candidates(chunk, term_cols)
            if not cols:
                continue
            if 0 < sample_frac < 1.0:
                chunk = chunk.sample(frac=sample_frac, random_state=42)
            for _, rec in chunk.iterrows():
                for c in cols:
                    v = rec.get(c)
                    if pd.isna(v) or not str(v).strip():
                        continue
                    t = normalize_term(str(v))
                    if len(t) < min_len:
                        continue
                    rows.append({"term": t})
                    total += 1
                    if row_limit and total >= row_limit:
                        return pd.DataFrame(rows)
    return pd.DataFrame(rows)


# ------------------------ polars reader ------------------------
def polars_terms_scan_csv(
    path: Path,
    sep: Optional[str],
    term_cols: List[str],
    min_len: int,
    sample_frac: float,
    row_limit: int,
) -> pd.DataFrame:
    if pl is None:
        return pd.DataFrame(columns=["term"])
    try:
        head = pl.read_csv(
            path,
            n_rows=100,
            sep=(sep or ","),
            has_header=True,
            infer_schema_length=1000,
            ignore_errors=True,
        )
    except Exception:
        return pd.DataFrame(columns=["term"])
    sel: List[str] = []
    cand_lower = [c.lower() for c in term_cols]
    for c in head.columns:
        if c.lower() in cand_lower:
            sel.append(c)
    if not sel:
        sel = head.columns[:1]
    lf = pl.scan_csv(
        path,
        sep=(sep or ","),
        has_header=True,
        infer_schema_length=1000,
        ignore_errors=True,
    ).select(sel)
    if 0 < sample_frac < 1.0:
        stride = max(1, int(1.0 / sample_frac))
        lf = lf.with_row_count().filter((pl.col("row_nr") % stride) == 0).drop("row_nr")
    if row_limit and row_limit > 0:
        lf = lf.limit(row_limit)
    df = lf.collect(streaming=True)
    if len(sel) > 1:
        df = df.select(pl.coalesce(sel).alias("term"))
    else:
        df = df.rename({sel[0]: "term"})
    df = df.with_columns(pl.col("term").cast(pl.Utf8).str.strip())
    df = df.filter(pl.col("term").is_not_null() & (pl.col("term").str.lengths() >= min_len))
    return df.to_pandas(use_pyarrow_extension_array=True)


# -------------------- specialty parsers ------------------------
def parse_gct(path: Path, term_cols: List[str], min_len: int) -> pd.DataFrame:
    if str(path).lower().endswith(".gct.gz"):
        with gzip.open(path, "rt", encoding="utf-8", errors="ignore") as fh:
            lines = fh.read().splitlines()
    else:
        lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
    if len(lines) < 3:
        return pd.DataFrame(columns=["term"])
    header = lines[2].split("\t")
    data = [ln.split("\t") for ln in lines[3:]]
    df = pd.DataFrame(data, columns=header)
    df.columns = [c.lower() for c in df.columns]
    cols = infer_candidates(df, term_cols)
    rows: List[Dict[str, str]] = []
    for _, rec in df.iterrows():
        for c in cols:
            v = rec.get(c)
            if pd.isna(v) or not str(v).strip():
                continue
            t = normalize_term(str(v))
            if len(t) < min_len:
                continue
            rows.append({"term": t})
    return pd.DataFrame(rows)


def parse_json_like(path: Path, min_len: int) -> pd.DataFrame:
    try:
        obj = json.loads(path.read_text(encoding="utf-8", errors="ignore"))
    except Exception:
        return pd.DataFrame(columns=["term"])
    rows: List[Dict[str, str]] = []

    def walk(o: Any):
        if isinstance(o, dict):
            for v in o.values():
                walk(v)
        elif isinstance(o, list):
            for v in o:
                walk(v)
        elif isinstance(o, str):
            s = normalize_term(o)
            if len(s) >= min_len:
                rows.append({"term": s})

    walk(obj)
    return pd.DataFrame(rows)


# ---------------------- typing & filtering ---------------------
def guess_term_type(name: str, type_hints: Dict[str, List[str]]) -> str:
    low = name.lower()
    for t, hints in type_hints.items():
        for h in hints:
            if h.lower() in low:
                return t
    return ""


# ------------------------- core compile ------------------------
def compile_from_file(
    path: Path,
    term_cols: List[str],
    min_len: int,
    opts: Opts,
) -> pd.DataFrame:
    """Return DataFrame with at least 'term' column (no path columns here; we enrich later)."""
    n = path.name.lower()
    try:
        # compressed csv/tsv/txt
        if n.endswith(".csv.gz"):
            return (
                polars_terms_scan_csv(path, ",", term_cols, min_len, opts.sample_frac, opts.row_limit)
                if opts.engine == "polars" and pl is not None
                else pandas_stream_terms_gz(path, ",", term_cols, min_len, opts.chunksize, opts.sample_frac, opts.row_limit)
            )
        if n.endswith(".tsv.gz"):
            return (
                polars_terms_scan_csv(path, "\t", term_cols, min_len, opts.sample_frac, opts.row_limit)
                if opts.engine == "polars" and pl is not None
                else pandas_stream_terms_gz(path, "\t", term_cols, min_len, opts.chunksize, opts.sample_frac, opts.row_limit)
            )
        if n.endswith(".txt.gz"):
            return (
                polars_terms_scan_csv(path, None, term_cols, min_len, opts.sample_frac, opts.row_limit)
                if opts.engine == "polars" and pl is not None
                else pandas_stream_terms_gz(path, None, term_cols, min_len, opts.chunksize, opts.sample_frac, opts.row_limit)
            )

        # plain csv/tsv/txt
        if path.suffix.lower() == ".csv":
            return (
                polars_terms_scan_csv(path, ",", term_cols, min_len, opts.sample_frac, opts.row_limit)
                if opts.engine == "polars" and pl is not None
                else pandas_stream_terms(path, ",", term_cols, min_len, opts.chunksize, opts.sample_frac, opts.row_limit)
            )
        if path.suffix.lower() == ".tsv":
            return (
                polars_terms_scan_csv(path, "\t", term_cols, min_len, opts.sample_frac, opts.row_limit)
                if opts.engine == "polars" and pl is not None
                else pandas_stream_terms(path, "\t", term_cols, min_len, opts.chunksize, opts.sample_frac, opts.row_limit)
            )
        if path.suffix.lower() == ".txt":
            return (
                polars_terms_scan_csv(path, None, term_cols, min_len, opts.sample_frac, opts.row_limit)
                if opts.engine == "polars" and pl is not None
                else pandas_stream_terms(path, None, term_cols, min_len, opts.chunksize, opts.sample_frac, opts.row_limit)
            )

        # specialty
        if n.endswith(".gct") or n.endswith(".gct.gz"):
            return parse_gct(path, term_cols, min_len)

        if path.suffix.lower() == ".seg":
            df = pd.read_csv(path, sep="\t", dtype=str, on_bad_lines="skip", engine="python")
            df.columns = [c.lower() for c in df.columns]
            cols = infer_candidates(df, term_cols)
            rows = []
            for _, rec in df.iterrows():
                for c in cols:
                    v = rec.get(c)
                    if pd.isna(v) or not str(v).strip():
                        continue
                    t = normalize_term(str(v))
                    if len(t) < min_len:
                        continue
                    rows.append({"term": t})
            return pd.DataFrame(rows)

        if path.suffix.lower() == ".json":
            return parse_json_like(path, min_len)

        if path.suffix.lower() in (".yaml", ".yml"):
            try:
                y = yaml.safe_load(path.read_text(encoding="utf-8", errors="ignore"))
            except Exception:
                return pd.DataFrame(columns=["term"])
            rows: List[Dict[str, str]] = []

            def walk(o: Any):
                if isinstance(o, dict):
                    for v in o.values():
                        walk(v)
                elif isinstance(o, list):
                    for v in o:
                        walk(v)
                elif isinstance(o, str):
                    s = normalize_term(o)
                    if len(s) >= min_len:
                        rows.append({"term": s})

            walk(y)
            return pd.DataFrame(rows)

        # unknown/binary
        return pd.DataFrame(columns=["term"])
    except Exception as e:
        if opts.verbose_skips:
            console.print(f"[yellow]Skip {path}:[/yellow] {e}")
        return pd.DataFrame(columns=["term"])


# --------------------------- main ------------------------------
def main() -> None:
    ap = argparse.ArgumentParser(description="Compile vocabulary from VOCAB + STRUCTURED + DICTIONARY.")
    ap.add_argument("--engine", choices=["pandas", "polars"], default="pandas")
    ap.add_argument("--chunksize", type=int, default=200_000)
    ap.add_argument("--row-limit", type=int, default=0)
    ap.add_argument("--sample-frac", type=float, default=1.0)
    ap.add_argument("--verbose-skips", action="store_true")
    ap.add_argument("--dedupe-scope", choices=["global", "by-source"], default="by-source",
                    help="global=dedupe by (term_lc, term_type); by-source=dedupe by (term_lc, term_type, source_relpath)")
    ap.add_argument("--write-parquet", action="store_true", help="Also write Parquet alongside CSV.")
    ap.add_argument("--no-sqlite", action="store_true", help="Skip writing SQLite artifact.")
    args = ap.parse_args()

    # Resolve project root and config first
    root = Path(__file__).resolve().parents[1]
    cfg = load_config(root / "config.yaml")

    # Output paths (created up front)
    out_dir = (root / "data/02_interim/VOCAB_COMPILED").resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    out_csv = out_dir / "vocabulary.csv"
    out_parq = out_dir / "vocabulary.parquet"
    out_db = out_dir / "vocabulary.db"
    out_md = out_dir / "vocab_report.md"

    # Discoverable roots
    raw_root = Path(root / (cfg_get(cfg, ["paths", "raw_root"], "data/01_raw") or "data/01_raw")).resolve()
    vocab_dir = Path(root / (cfg_get(cfg, ["paths", "raw_vocab_dir"], "data/01_raw/VOCAB") or "data/01_raw/VOCAB")).resolve()
    struct_dir = Path(root / (cfg_get(cfg, ["paths", "raw_structured_dir"], "data/01_raw/STRUCTURED") or "data/01_raw/STRUCTURED")).resolve()
    dict_dir = Path(root / (cfg_get(cfg, ["paths", "dict_dir"], "data/01_raw/DICTIONARY") or "data/01_raw/DICTIONARY")).resolve()

    # Term column candidates
    term_cols = [c.lower() for c in as_list_str(cfg_get(cfg, ["parsing", "preferred_term_columns"], []))]
    if not term_cols:
        term_cols = ["term", "symbol", "gene", "hgnc_symbol", "name", "label", "code"]

    min_len = int(cfg_get(cfg, ["qualitative", "min_term_len"], 3) or 3)
    stopwords = {s.lower() for s in as_list_str(cfg_get(cfg, ["vocab", "entity_stopwords"], []))}
    type_hints: Dict[str, List[str]] = {}
    th = cfg_get(cfg, ["vocab", "type_hints"], {}) or {}
    if isinstance(th, dict):
        for k, v in th.items():
            type_hints[str(k)] = as_list_str(v, [])

    # Options
    opts = Opts(
        engine=("polars" if (args.engine == "polars" and pl is not None) else "pandas"),
        chunksize=args.chunksize,
        row_limit=args.row_limit,
        sample_frac=args.sample_frac,
        verbose_skips=bool(args.verbose_skips),
        dedupe_scope=args.dedupe_scope,
    )
    if args.engine == "polars" and pl is None:
        console.print("[yellow]polars not installed; falling back to pandas.[/yellow]")

    # Enumerate files with their root labels
    sources: List[Tuple[str, Path, Path]] = []
    if vocab_dir.exists():  # (label, base, path)
        for p in vocab_dir.rglob("*.*"):
            if p.is_file():
                sources.append(("VOCAB", vocab_dir, p))
    if struct_dir.exists():
        for p in struct_dir.rglob("*.*"):
            if p.is_file():
                sources.append(("STRUCTURED", struct_dir, p))
    if dict_dir.exists():
        for p in dict_dir.rglob("*.*"):
            if p.is_file():
                sources.append(("DICTIONARY", dict_dir, p))

    if not sources:
        console.print("[red]No files found in VOCAB/STRUCTURED/DICTIONARY.[/red]")
        # Write empty artifacts to keep pipeline happy
        pd.DataFrame(columns=["term", "term_type", "source_root", "source_relpath", "source_dir", "source_file", "source_abs"]).to_csv(out_csv, index=False, encoding="utf-8")
        out_md.write_text("# Vocabulary Report (All Sources)\n\n- Total terms: **0**\n- Files scanned: **0**\n", encoding="utf-8")
        console.print(f"[green]CSV:[/green] {out_csv}")
        console.print(f"[green]Report:[/green] {out_md}")
        return

    # -------------------------- progress UI --------------------------
    bar = BarColumn(bar_width=None)
    text = TextColumn("[progress.description]{task.description}")
    count = TextColumn("{task.completed}/{task.total}")
    elapsed = TimeElapsedColumn()
    remain = TimeRemainingColumn()

    with Progress(text, bar, count, "•", elapsed, "eta", remain, console=console) as prog:
        t_scan: TaskID = prog.add_task("[bold cyan]Scanning files", total=len(sources))
        t_extract: TaskID = prog.add_task("[bold cyan]Extracting terms", total=len(sources))
        t_post: TaskID = prog.add_task("[bold cyan]Post-processing", total=3)
        t_write: TaskID = prog.add_task("[bold cyan]Writing artifacts", total=4 if not args.no_sqlite else 3)

        # quick scan tick
        for _ in sources:
            prog.advance(t_scan)

        # Extract
        dfs: List[pd.DataFrame] = []
        for label, base, p in sources:
            df = compile_from_file(p, term_cols, min_len, opts)
            if df is not None and not df.empty:
                # attach provenance columns
                rel = rel_unix(p, base)
                d, b = split_dir_file(rel)
                df = df.assign(
                    term=df["term"].astype(str).str.strip(),
                    source_root=label,
                    source_relpath=rel,
                    source_dir=d,
                    source_file=b,
                    source_abs=str(p),
                )
                # light typing & classification by filename hints
                ttype = guess_term_type(p.name, type_hints)
                df["term_type"] = ttype
                dfs.append(df)
            prog.advance(t_extract)

        # No terms?
        if not dfs:
            console.print("[yellow]No terms extracted from scanned files.[/yellow]")
            empty_cols = ["term", "term_type", "source_root", "source_relpath", "source_dir", "source_file", "source_abs"]
            pd.DataFrame(columns=empty_cols).to_csv(out_csv, index=False, encoding="utf-8")
            out_md.write_text("# Vocabulary Report (All Sources)\n\n- Total terms: **0**\n", encoding="utf-8")
            console.print(f"[green]CSV:[/green] {out_csv}")
            console.print(f"[green]Report:[/green] {out_md}")
            return

        # Concatenate & clean
        vocab = pd.concat(dfs, ignore_index=True)
        prog.advance(t_post)

        vocab["term"] = vocab["term"].astype(str).str.strip()
        vocab["term_lc"] = vocab["term"].str.lower()

        # Stopwords
        if stopwords:
            vocab = vocab[~vocab["term_lc"].isin(stopwords)].reset_index(drop=True)
        prog.advance(t_post)

        # Dedupe
        if opts.dedupe_scope == "global":
            subset = ["term_lc", "term_type"]
        else:  # by-source
            subset = ["term_lc", "term_type", "source_root", "source_relpath"]
        vocab = vocab.drop_duplicates(subset=subset).reset_index(drop=True)
        prog.advance(t_post)

        # Final minimal order
        final_cols = [
            "term", "term_type",
            "source_root", "source_relpath", "source_dir", "source_file", "source_abs",
        ]
        final = vocab[final_cols].copy()

        # Write CSV
        final.to_csv(out_csv, index=False, encoding="utf-8")
        console.log(f"[green]CSV:[/green] {out_csv}")
        prog.advance(t_write)

        # Write Parquet (optional)
        if args.write_parquet:
            try:
                if pl is not None:
                    pl.from_pandas(final).write_parquet(str(out_parq))
                else:
                    # try via pandas/pyarrow if available
                    final.to_parquet(out_parq, index=False)  # noqa
                console.log(f"[green]Parquet:[/green] {out_parq}")
            except Exception as e:
                console.log(f"[yellow]Parquet skipped:[/yellow] {e}")
            prog.advance(t_write)

        # SQLite (optional)
        if not args.no_sqlite:
            try:
                import sqlite3
                conn = sqlite3.connect(str(out_db))
                cur = conn.cursor()
                cur.execute("DROP TABLE IF EXISTS vocabulary")
                cur.execute("CREATE TABLE vocabulary (term TEXT, term_type TEXT, source_root TEXT, source_relpath TEXT, source_dir TEXT, source_file TEXT, source_abs TEXT)")
                cur.executemany(
                    "INSERT INTO vocabulary(term, term_type, source_root, source_relpath, source_dir, source_file, source_abs) VALUES (?,?,?,?,?,?,?)",
                    final.itertuples(index=False, name=None),
                )
                cur.execute("CREATE INDEX IF NOT EXISTS idx_term ON vocabulary(term)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_term_type ON vocabulary(term_type)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_rel ON vocabulary(source_root, source_relpath)")
                conn.commit()
                conn.close()
                console.log(f"[green]SQLite:[/green] {out_db}")
            except Exception as e:
                console.log(f"[yellow]SQLite skipped:[/yellow] {e}")
            prog.advance(t_write)

        # Report
        try:
            by_t = final.groupby("term_type", dropna=False).size().reset_index(name="n").sort_values("n", ascending=False)
            report = [
                "# Vocabulary Report (All Sources)",
                "",
                f"- Total terms: **{len(final)}**",
                f"- Files scanned: **{len(sources)}**",
                f"- Dedupe scope: **{opts.dedupe_scope}**",
                "",
                "## Counts by term_type",
            ]
            try:
                table_md = by_t.to_markdown(index=False)
            except Exception:
                # fallback if tabulate isn't present
                table_md = "term_type | n\n---|---\n" + "\n".join(f"{r['term_type']} | {r['n']}" for _, r in by_t.iterrows())
            report.append("")
            report.append(table_md)
            out_md.write_text("\n".join(report), encoding="utf-8")
            console.log(f"[green]Report:[/green] {out_md}")
        except Exception as e:
            console.log(f"[yellow]Report skipped:[/yellow] {e}")
        prog.advance(t_write)

    console.rule("[bold green]Done")


if __name__ == "__main__":
    main()
