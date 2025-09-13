# -*- coding: utf-8 -*-
"""
Qualitative extractor from PDF journals (maximum compatibility).

Features
--------
- PDF text extraction:
    * Prefers PyMuPDF (fitz) if available; falls back to pdfminer.six.
- Vocabulary-driven entity detection:
    * FlashText if available (fast, case-insensitive, multi-word) with hyphen/slash in-word support.
    * Fallback: small-chunk regex matcher (avoids oversized patterns).
- Sectionization & sentence splitting:
    * Heuristics for ABSTRACT/INTRO/METHODS/RESULTS/DISCUSSION/CONCLUSION.
    * Conservative sentence splitter (no external downloads).
- Relation harvesting:
    * Verb patterns: inhibits/activates/binds/phosphorylates/induces/synergizes/antagonizes/knockdown/overexpression/mutation/associated with/mediates/targets.
    * Numeric qualifiers: IC50/EC50/Ki/Kd/GI50 (with units), HR/OR + CI, p-values, dosing (mg/kg, μM/nM).
    * Confidence score heuristic by pattern strength + presence of metrics.
- Provenance columns:
    * doc_relpath, doc_dir, doc_file, doc_abs, section, page spans.
- Outputs:
    * Mentions table (long), Relations table (long), Parquet + CSV(flat).
"""

from __future__ import annotations
import argparse, os, re, sys, json, glob
from dataclasses import dataclass
from typing import List, Dict, Tuple, Optional, Iterable

import polars as pl
from rich.console import Console
from rich.progress import Progress, BarColumn, TimeElapsedColumn, TimeRemainingColumn, TextColumn

console = Console()

# ------------------- Optional deps -------------------
try:
    import fitz  # PyMuPDF
except Exception:
    fitz = None

try:
    from pdfminer.high_level import extract_text as pdfminer_extract_text
except Exception:
    pdfminer_extract_text = None

try:
    from flashtext import KeywordProcessor
except Exception:
    KeywordProcessor = None

# ------------------- Utils & setup -------------------

def safe_mkdir(p: str) -> None:
    os.makedirs(p, exist_ok=True)

def relpath_unix(p: str, root: str) -> str:
    try:
        rp = os.path.relpath(p, root)
    except ValueError:
        rp = p
    return rp.replace("\\", "/")

def unique_preserve_order(items: Iterable[str]) -> List[str]:
    seen = set(); out = []
    for x in items:
        if x is None: continue
        if x not in seen:
            seen.add(x); out.append(x)
    return out

# ------------------- Vocab loading -------------------

def load_vocab(vocab_path: str) -> Tuple[pl.DataFrame, Dict[str, str], Dict[str, List[str]]]:
    ext = os.path.splitext(vocab_path)[1].lower()
    if ext == ".parquet":
        v = pl.read_parquet(vocab_path)
    elif ext == ".csv":
        v = pl.read_csv(vocab_path)
    else:
        raise ValueError(f"Unsupported vocab: {ext}")
    # normalize columns
    need = {"term"}
    if not need.issubset(set(v.columns)): raise ValueError("Vocab must include 'term'.")
    if "term_type" not in v.columns: v = v.with_columns(pl.lit("unknown").alias("term_type"))
    if "canonical" not in v.columns: v = v.with_columns(pl.lit(None).alias("canonical"))

    v = (
        v.select(
            pl.col("term").cast(pl.Utf8),
            pl.col("term_type").cast(pl.Utf8),
            pl.col("canonical").cast(pl.Utf8),
        )
        .drop_nulls(subset=["term"])
        .unique(subset=["term"], keep="first")
    )
    # canonical map (case-insensitive key)
    terms = v["term"].to_list()
    canon_vals = v["canonical"].fill_null(v["term"]).to_list()
    canon_map = { (t.lower() if isinstance(t,str) else t): c for t,c in zip(terms, canon_vals) }

    by_type_df = (
        v.group_by(["term_type"])
         .agg(pl.col("term").unique().alias("terms"))
         .drop_nulls(subset=["term_type"])
    )
    by_type = dict(zip(by_type_df["term_type"].to_list(), by_type_df["terms"].to_list()))
    return v, canon_map, by_type

# ------------------- Entity matchers -------------------

class FlashMatcher:
    def __init__(self, terms: List[str], canon_map: Dict[str,str]) -> None:
        if KeywordProcessor is None:
            raise RuntimeError("flashtext not available")
        self.raw = KeywordProcessor(case_sensitive=False)
        self.norm = KeywordProcessor(case_sensitive=False)
        for kp in (self.raw, self.norm):
            kp.add_non_word_boundary("-"); kp.add_non_word_boundary("/")
        for t in terms:
            if not isinstance(t,str) or not t: continue
            self.raw.add_keyword(t)
            self.norm.add_keyword(t, canon_map.get(t.lower(), t))
    def extract_raw(self, s: str|None)->List[str]:
        if not isinstance(s,str) or not s: return []
        return self.raw.extract_keywords(s)
    def extract_norm(self, s: str|None)->List[str]:
        if not isinstance(s,str) or not s: return []
        return self.norm.extract_keywords(s)

class RegexChunkMatcher:
    def __init__(self, terms: List[str], canon_map: Dict[str,str], chunk_size:int=50)->None:
        safe = [re.escape(t) for t in terms if isinstance(t,str) and t]
        safe.sort(key=len, reverse=True)
        self._pats = [re.compile(r"(?i)\b(" + "|".join(safe[i:i+chunk_size]) + r")\b") for i in range(0,len(safe),chunk_size)] if safe else []
        self._canon = canon_map
    def extract_raw(self, s: str|None)->List[str]:
        if not isinstance(s,str) or not s or not self._pats: return []
        out=[]; [out.extend(p.findall(s)) for p in self._pats]
        return out
    def extract_norm(self, s: str|None)->List[str]:
        raw = self.extract_raw(s)
        return [ self._canon.get(str(x).lower(), str(x)) for x in raw ]

def build_matcher(terms: List[str], canon_map: Dict[str,str]):
    try:
        if KeywordProcessor is None: raise RuntimeError
        return FlashMatcher(terms, canon_map)
    except Exception:
        return RegexChunkMatcher(terms, canon_map)

# ------------------- PDF text extraction -------------------

def extract_text_pymupdf(pdf_path: str, max_pages:int=0) -> Tuple[str, List[Tuple[int,int]]]:
    """Return full text and list of page start/end char offsets."""
    if fitz is None: raise RuntimeError("PyMuPDF not available")
    doc = fitz.open(pdf_path)
    texts=[]; offsets=[]; pos=0
    N = len(doc)
    for i, page in enumerate(doc):
        if max_pages and i>=max_pages: break
        t = page.get_text("text")  # basic, robust
        if not isinstance(t,str): t=str(t)
        texts.append(t)
        offsets.append((i, pos, pos+len(t)))
        pos += len(t)
    doc.close()
    return "".join(texts), offsets

def extract_text_pdfminer(pdf_path: str, max_pages:int=0) -> Tuple[str, List[Tuple[int,int]]]:
    if pdfminer_extract_text is None: raise RuntimeError("pdfminer.six not available")
    # pdfminer doesn't give easy offsets per page; we recompute by splitting on \f
    text = pdfminer_extract_text(pdf_path, maxpages=max_pages if max_pages>0 else None) or ""
    parts = text.split("\f")
    offsets=[]; pos=0
    out=[]
    for i, part in enumerate(parts):
        out.append(part)
        offsets.append((i, pos, pos+len(part)))
        pos+=len(part)
    return "".join(out), offsets

def extract_pdf_text(pdf_path: str, max_pages:int=0) -> Tuple[str, List[Tuple[int,int]]]:
    last_err=None
    if fitz is not None:
        try: return extract_text_pymupdf(pdf_path, max_pages)
        except Exception as e: last_err=e
    if pdfminer_extract_text is not None:
        try: return extract_text_pdfminer(pdf_path, max_pages)
        except Exception as e: last_err=e
    raise RuntimeError(f"No PDF backend available (install PyMuPDF or pdfminer.six). Last error: {last_err}")

# ------------------- Cleaning/Sectioning/Sentences -------------------

SECTION_HEADS = [
    r"ABSTRACT", r"BACKGROUND", r"INTRODUCTION", r"METHODS?", r"MATERIALS AND METHODS",
    r"RESULTS", r"DISCUSSION", r"CONCLUSION", r"CONCLUSIONS", r"ACKNOWLEDGMENTS?", r"REFERENCES"
]
SECTION_RE = re.compile(rf"(?m)^\s*({'|'.join(SECTION_HEADS)})\s*$", re.IGNORECASE)

def clean_text(s: str) -> str:
    if not s: return ""
    # de-hyphenate across line breaks: "acti-\nvation" -> "activation"
    s = re.sub(r"(\w)-\n(\w)", r"\1\2", s)
    # normalize newlines/whitespace
    s = s.replace("\r","")
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\n{2,}", "\n\n", s)
    return s

def split_sections(text: str) -> List[Tuple[str,str]]:
    """
    Returns list of (section_name, section_text), section_name may be 'UNKNOWN' if no headings matched.
    """
    if not text: return [("UNKNOWN", "")]
    hits = list(SECTION_RE.finditer(text))
    if not hits:
        return [("UNKNOWN", text)]
    out=[]
    for i, m in enumerate(hits):
        name = m.group(1).upper()
        start = m.end()
        end = hits[i+1].start() if i+1<len(hits) else len(text)
        out.append((name, text[start:end].strip()))
    # include preface before first section as INTRO/UNKNOWN
    pre_start = hits[0].start()
    if pre_start>0:
        out = [("PREFACE", text[:pre_start].strip())] + out
    return out

SENT_SPLIT_RE = re.compile(
    r"(?<!\b[A-Z]\.)(?<!\b[A-Z][a-z]\.)(?<!\bet al)(?<!\bFig)(?<!\bRef)"
    r"(?<=[\.\?\!])\s+(?=[A-Z])"
)

def split_sentences(section_text: str) -> List[str]:
    if not section_text: return []
    # protect common abbreviations crudely above; then split
    parts = re.split(SENT_SPLIT_RE, section_text.strip())
    # prune very short fragments
    return [p.strip() for p in parts if len(p.strip())>=20]

# ------------------- Relation patterns -------------------

REL_PATTERNS = [
    # predicate, compiled regex (case-insensitive)
    ("inhibits",       re.compile(r"\b(inhibit(?:s|ed|ion)?|block(?:s|ed|ade)?)\b", re.I)),
    ("activates",      re.compile(r"\b(activate(?:s|d|ion)?)\b", re.I)),
    ("binds",          re.compile(r"\b(bind(?:s|ing)?)\b", re.I)),
    ("phosphorylates", re.compile(r"\b(phosphorylat(?:es|ed|ion))\b", re.I)),
    ("induces",        re.compile(r"\b(induce(?:s|d|ment)?)\b", re.I)),
    ("suppresses",     re.compile(r"\b(suppress(?:es|ed|ion)?)\b", re.I)),
    ("sensitizes",     re.compile(r"\b(sensiti[sz]e(?:s|d|ation)?)\b", re.I)),
    ("synergizes",     re.compile(r"\b(synerg(?:y|istic|izes?))\b", re.I)),
    ("antagonizes",    re.compile(r"\b(antagonis(?:m|t|es?))\b", re.I)),
    ("targets",        re.compile(r"\b(target(?:s|ed|ing)?)\b", re.I)),
    ("associated_with",re.compile(r"\b(associat(?:ed|ion)\s+with)\b", re.I)),
    ("mutation",       re.compile(r"\b(mutant|mutation|mutations|mutated)\b", re.I)),
    ("overexpression", re.compile(r"\b(over[- ]?express(?:ion|ed))\b", re.I)),
    ("knockdown",      re.compile(r"\b(knock[- ]?down|silenc(?:e|ing)|shRNA)\b", re.I)),
    ("pathway_activation", re.compile(r"\b(pathway|cascade).{0,12}(activat(?:e|ion|ed))\b", re.I)),
]

NUMERIC_PATTERNS = [
    # key, regex to capture (value, unit?) or (lhs, value, unit)
    ("IC50", re.compile(r"\bIC(?:50)\s*(?:=|:|of)?\s*(\d+(?:\.\d+)?)\s*(pM|nM|µM|uM|mM)?\b", re.I)),
    ("EC50", re.compile(r"\bEC(?:50)\s*(?:=|:|of)?\s*(\d+(?:\.\d+)?)\s*(pM|nM|µM|uM|mM)?\b", re.I)),
    ("GI50", re.compile(r"\bGI(?:50)\s*(?:=|:|of)?\s*(\d+(?:\.\d+)?)\s*(pM|nM|µM|uM|mM)?\b", re.I)),
    ("Kd",   re.compile(r"\bK[di]\s*(?:=|:)?\s*(\d+(?:\.\d+)?)\s*(pM|nM|µM|uM|mM)?\b", re.I)),
    ("Ki",   re.compile(r"\bKi\s*(?:=|:)?\s*(\d+(?:\.\d+)?)\s*(pM|nM|µM|uM|mM)?\b", re.I)),
    ("HR",   re.compile(r"\bHR\s*(?:=|:)?\s*(\d+(?:\.\d+)?)\b", re.I)),
    ("OR",   re.compile(r"\bOR\s*(?:=|:)?\s*(\d+(?:\.\d+)?)\b", re.I)),
    ("CI95", re.compile(r"\b95%?\s*CI\s*(?:=|:)?\s*\(?\s*(\d+(?:\.\d+)?)[,\-–]\s*(\d+(?:\.\d+)?)\s*\)?", re.I)),
    ("p",    re.compile(r"\bp\s*(?:=|<|≤)\s*(\d+(?:\.\d+)?(?:e-?\d+)?)\b", re.I)),
    ("dose", re.compile(r"\b(\d+(?:\.\d+)?)\s*(mg/kg|mg/m2|ug/mL|µg/mL|μg/mL|mg|µM|uM|nM)\b", re.I)),
]

# Mapping of vocab term_type values into coarse roles (heuristic)
ROLE_MAP = {
    "drug": "drug", "compound":"drug", "inhibitor":"drug", "antibody":"drug", "therapy":"drug", "treatment":"drug",
    "gene":"gene", "symbol":"gene", "protein":"gene", "target":"gene", "biomarker":"gene",
    "pathway":"pathway", "reactome":"pathway", "go":"pathway", "kegg":"pathway", "cascade":"pathway",
    "disease":"disease", "cancer":"disease", "phenotype":"disease",
}

def coarse_role(term: str, term_type: str) -> str:
    if term_type:
        t = term_type.lower()
        for k,v in ROLE_MAP.items():
            if k in t: return v
    # fallback heuristics
    if "/" in term or "-" in term: return "gene"  # e.g., HER2/neu, TNF-alpha
    return "unknown"

# ------------------- Extraction core -------------------

def sentence_mentions(sentence: str, matcher) -> Tuple[List[str], List[str]]:
    raw = unique_preserve_order(matcher.extract_raw(sentence))
    norm = unique_preserve_order(matcher.extract_norm(sentence))
    return raw, norm

def pick_predicate(sentence: str) -> Tuple[str, str]:
    # returns (predicate, pattern_id)
    for pred, rx in REL_PATTERNS:
        if rx.search(sentence):
            return pred, rx.pattern
    return "", ""

def extract_numeric_qualifiers(sentence: str) -> Dict[str, str]:
    out = {}
    for key, rx in NUMERIC_PATTERNS:
        m = rx.search(sentence)
        if not m: continue
        try:
            if key in ("CI95",):
                out[key] = f"{m.group(1)}-{m.group(2)}"
            elif key in ("dose",):
                out[key] = f"{m.group(1)} {m.group(2)}"
            else:
                val = m.group(1); unit = (m.group(2) or "").replace("µ","u")
                out[key] = f"{val}{(' '+unit) if unit else ''}".strip()
        except Exception:
            continue
    return out

def pairwise_relations(entities: List[Tuple[str,str]], predicate: str) -> List[Tuple[Tuple[str,str],Tuple[str,str]]]:
    """
    Build (subject, object) pairs based on coarse roles.
    entities: list of (canonical, role)
    Strategy:
      - If predicate implies action (inhibits/activates/targets), prefer (drug -> gene/pathway/disease).
      - If mutation/overexpression, prefer (gene -> disease/pathway).
      - Otherwise, produce all pairwise combos among distinct entities (conservative).
    """
    if not entities: return []
    drug = [e for e in entities if e[1]=="drug"]
    gene = [e for e in entities if e[1]=="gene"]
    pway = [e for e in entities if e[1]=="pathway"]
    dis  = [e for e in entities if e[1]=="disease"]

    pairs=[]
    if predicate in ("inhibits","activates","binds","phosphorylates","targets","sensitizes","suppresses","induces","synergizes","antagonizes"):
        for s in drug:
            for o in gene+pway+dis:
                if s[0]!=o[0]: pairs.append((s,o))
    elif predicate in ("mutation","overexpression","knockdown","associated_with","pathway_activation"):
        for s in gene:
            for o in dis+pway:
                if s[0]!=o[0]: pairs.append((s,o))
    # fallback if nothing found
    if not pairs and len(entities)>=2:
        for i in range(len(entities)):
            for j in range(len(entities)):
                if i==j: continue
                if entities[i][0]!=entities[j][0]:
                    pairs.append((entities[i], entities[j]))
    return pairs

def confidence_score(predicate: str, qualifiers: Dict[str,str], entity_count:int) -> float:
    base = 0.4
    strong = {"inhibits","activates","binds","phosphorylates","targets","synergizes","antagonizes"}
    medium = {"suppresses","induces","sensitizes","pathway_activation"}
    weak = {"associated_with","mutation","overexpression","knockdown"}
    if predicate in strong: base = 0.9
    elif predicate in medium: base = 0.7
    elif predicate in weak: base = 0.6
    if any(k in qualifiers for k in ("IC50","EC50","GI50","Ki","Kd","HR","OR","p")):
        base += 0.05
    if entity_count>=3:
        base += 0.05
    return max(0.0, min(1.0, base))

# ------------------- Main pipeline per PDF -------------------

def process_pdf(pdf_path: str, in_root: str, matcher, type_index: Dict[str,str],
                add_path_cols: bool, preserve_tree: bool, out_dir: str, csv_mode: str,
                max_pages:int=0) -> Tuple[List[Dict], List[Dict]]:
    """Return (mentions_rows, relations_rows)"""
    text, offsets = extract_pdf_text(pdf_path, max_pages=max_pages)
    text = clean_text(text)
    sections = split_sections(text)

    rel_pdf = relpath_unix(pdf_path, in_root)
    doc_dir = os.path.dirname(rel_pdf); doc_file = os.path.basename(rel_pdf)

    mentions_rows=[]; relations_rows=[]
    sent_id=0

    for sec_name, sec_text in sections:
        if not sec_text: continue
        sentences = split_sentences(sec_text)
        for sent in sentences:
            sent_id += 1
            raw, norm = sentence_mentions(sent, matcher)
            if not norm:  # no entities
                continue
            # mentions rows (one row per sentence; entities as pipe-joined in CSV later)
            roles = [ coarse_role(n, type_index.get(n.lower(),"")) for n in norm ]
            mentions_rows.append({
                "doc_relpath": rel_pdf, "doc_dir": doc_dir, "doc_file": doc_file, "doc_abs": os.path.abspath(pdf_path),
                "section": sec_name, "sentence_id": sent_id, "sentence": sent,
                "mentions_raw": raw, "mentions_canonical": norm, "mention_roles": roles,
            })
            # relations
            predicate, pattern_id = pick_predicate(sent)
            qualifiers = extract_numeric_qualifiers(sent)

            # assemble entity tuples (canonical, role) unique
            ent = unique_preserve_order(norm)
            ent_tuples = [(e, coarse_role(e, type_index.get(e.lower(),""))) for e in ent]
            pairs = pairwise_relations(ent_tuples, predicate)
            if not pairs:  # still record contextual co-mention
                continue
            conf = confidence_score(predicate, qualifiers, len(ent_tuples))
            for (s_term, s_role), (o_term, o_role) in pairs:
                relations_rows.append({
                    "doc_relpath": rel_pdf, "doc_dir": doc_dir, "doc_file": doc_file, "doc_abs": os.path.abspath(pdf_path),
                    "section": sec_name, "sentence_id": sent_id, "sentence": sent,
                    "subject": s_term, "subject_role": s_role,
                    "predicate": predicate, "pattern_id": pattern_id,
                    "object": o_term, "object_role": o_role,
                    "qualifiers_json": json.dumps(qualifiers) if qualifiers else "",
                    "confidence": conf,
                    "all_mentions": ent,  # for parquet
                })

    return mentions_rows, relations_rows

# ------------------- Write helpers -------------------

def write_tables(mentions_rows: List[Dict], relations_rows: List[Dict], out_dir: str, csv_mode:str):
    safe_mkdir(out_dir)
    m_df = pl.DataFrame(mentions_rows) if mentions_rows else pl.DataFrame({"doc_relpath":[]})
    r_df = pl.DataFrame(relations_rows) if relations_rows else pl.DataFrame({"doc_relpath":[]})

    # Parquet (authoritative)
    m_parq = os.path.join(out_dir, "pdf_mentions.parquet"); m_df.write_parquet(m_parq)
    r_parq = os.path.join(out_dir, "pdf_relations.parquet"); r_df.write_parquet(r_parq)

    # CSV flattened (pipe-join list cols)
    if csv_mode.lower() != "none":
        if "mentions_raw" in m_df.columns:
            m_df = m_df.with_columns(
                pl.col("mentions_raw").cast(pl.List(pl.Utf8)).list.join("|").alias("mentions_raw"),
                pl.col("mentions_canonical").cast(pl.List(pl.Utf8)).list.join("|").alias("mentions_canonical"),
                pl.col("mention_roles").cast(pl.List(pl.Utf8)).list.join("|").alias("mention_roles"),
            )
        m_csv = os.path.join(out_dir, "pdf_mentions.csv"); m_df.write_csv(m_csv)

        if "all_mentions" in r_df.columns:
            r_df = r_df.with_columns(pl.col("all_mentions").cast(pl.List(pl.Utf8)).list.join("|").alias("all_mentions"))
        r_csv = os.path.join(out_dir, "pdf_relations.csv"); r_df.write_csv(r_csv)

# ------------------- CLI -------------------

def main():
    ap = argparse.ArgumentParser(description="Extract qualitative entities/relations from PDF journals.")
    ap.add_argument("--in", dest="in_dir", required=True, help="Folder with PDFs")
    ap.add_argument("--vocab", required=True, help="Vocabulary CSV/Parquet with term, [term_type], [canonical]")
    ap.add_argument("--out", dest="out_dir", required=True, help="Output folder for extracted tables")
    ap.add_argument("--csv", choices=["flat","none"], default="flat", help="CSV writing mode (flat joins list columns)")
    ap.add_argument("--preserve-tree", action="store_true", help="Mirror input folder structure under --out (per-PDF parquet).")
    ap.add_argument("--add-path-cols", action="store_true", help="(Default) already included; kept for symmetry.")
    ap.add_argument("--max-pages", type=int, default=0, help="Limit pages per PDF (0 = all) to speed up dev runs.")
    args = ap.parse_args()

    safe_mkdir(args.out_dir)

    # Load vocab
    _, canon_map, by_type = load_vocab(args.vocab)
    all_terms = sorted({t for arr in by_type.values() for t in arr})
    # term -> type index (for roles)
    type_index = {}
    for ttype, arr in by_type.items():
        for t in arr:
            type_index[t.lower()] = ttype

    # Build matcher
    matcher = build_matcher(all_terms, canon_map)

    # Gather PDFs
    pdfs = sorted(set(glob.glob(os.path.join(args.in_dir, "**", "*.pdf"), recursive=True)))
    if not pdfs:
        console.print(f"[red]No PDFs under {args.in_dir}[/red]")
        sys.exit(1)

    # Progress
    prog_cols = [TextColumn("[progress.description]{task.description}"), BarColumn(bar_width=None), TimeElapsedColumn(), TimeRemainingColumn()]
    mentions_all=[]; relations_all=[]

    with Progress(*prog_cols, console=console) as prog:
        t = prog.add_task("[bold cyan]Extracting PDFs", total=len(pdfs))

        for pdf_path in pdfs:
            try:
                m_rows, r_rows = process_pdf(
                    pdf_path=pdf_path, in_root=args.in_dir, matcher=matcher, type_index=type_index,
                    add_path_cols=True, preserve_tree=args.preserve_tree, out_dir=args.out_dir, csv_mode=args.csv,
                    max_pages=args.max_pages
                )
                mentions_all.extend(m_rows)
                relations_all.extend(r_rows)

                # Optional: per-PDF outputs (preserve tree)
                if args.preserve_tree:
                    rel = relpath_unix(pdf_path, args.in_dir)
                    dest_dir = os.path.join(args.out_dir, os.path.dirname(rel).replace("/", os.sep))
                    safe_mkdir(dest_dir)
                    base = os.path.splitext(os.path.basename(rel))[0]
                    if m_rows:
                        pl.DataFrame(m_rows).write_parquet(os.path.join(dest_dir, f"{base}__mentions.parquet"))
                    if r_rows:
                        pl.DataFrame(r_rows).write_parquet(os.path.join(dest_dir, f"{base}__relations.parquet"))

            except Exception as e:
                console.log(f"[yellow]Skip {pdf_path}[/yellow]: {e}")

            prog.advance(t)

    # Write aggregate tables
    write_tables(mentions_all, relations_all, args.out_dir, args.csv)

    # Summary
    console.rule("[bold green]Done")
    console.log(f"Mentions rows: {len(mentions_all)}")
    console.log(f"Relations rows: {len(relations_all)}")

if __name__ == "__main__":
    main()
