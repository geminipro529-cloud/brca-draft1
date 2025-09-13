#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Advanced biomedical unstructured extractor
- Inputs: PDFs (.pdf) and/or plain text (.txt) under --in
- NER ensemble with graceful fallbacks:
    * SciSpaCy (en_core_sci_sm / en_ner_bc5cdr_md) + AbbreviationDetector (if available)
    * HuggingFace transformers pipeline NER (e.g., dslim/bert-base-NER, kamalkraj/BioBERT)
    * FlashText vocab keyword matcher (always available; uses your vocabulary.csv)
- Relation extraction:
    * Predicate patterns (inhibits/activates/targets/binds/resistant_to/sensitizes/associated_with)
    * Simple negation handling
    * Numeric qualifiers (IC50, p-values)
    * Section-aware confidence (ABSTRACT/RESULTS weighted higher)
- Normalization:
    * map to canonical terms from your vocab
    * map to term_type (drug/gene/pathway/disease/…)
- Outputs (always written):
    * entities.parquet (+ CSV flattened)
    * relations.parquet (+ CSV flattened)
    * doc_index.csv (what was processed)
    * run_report.md (quick counts)

Parquet is the source of truth; CSVs are for eyeballing.
"""

from __future__ import annotations
import argparse, os, re, glob, sys, json
from pathlib import Path
from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass

import polars as pl
from rich.console import Console
from rich.progress import Progress, BarColumn, TimeElapsedColumn, TimeRemainingColumn, TextColumn

console = Console()

# -------------------- Optional deps (all graceful) --------------------
try:
    import fitz  # PyMuPDF
except Exception:
    fitz = None

try:
    from pdfminer.high_level import extract_text as pdfminer_extract_text
except Exception:
    pdfminer_extract_text = None

try:
    import spacy
    from spacy.tokens import Doc
    _HAS_SPACY = True
except Exception:
    spacy = None
    Doc = None
    _HAS_SPACY = False

try:
    from scispacy.abbreviation import AbbreviationDetector  # type: ignore
    _HAS_ABBR = True
except Exception:
    _HAS_ABBR = False

try:
    from transformers import pipeline  # type: ignore
    _HAS_TXF = True
except Exception:
    _HAS_TXF = False

try:
    from flashtext import KeywordProcessor  # type: ignore
    _HAS_FT = True
except Exception:
    _HAS_FT = False

# -------------------- Regex + utils --------------------
SECTION_RE = re.compile(r"(?mi)^\s*(ABSTRACT|BACKGROUND|INTRODUCTION|METHODS?|RESULTS|DISCUSSION|CONCLUSI(?:ON|ONS)|REFERENCES|ACKNOWLEDGEMENTS?)\s*$")
SENT_SPLIT_RE = re.compile(r"(?<!\b[A-Z]\.)(?<!\b[A-Z][a-z]\.)(?<!\bet al)(?<=[.!?])\s+(?=[A-Z])")
NEG_RE = re.compile(r"\b(no|not|lack(?:ing)?|fails?\s+to|did\s+not|without)\b", re.I)

REL = [
    ("inhibits", re.compile(r"\b(inhibit(?:s|ed|ion)?|block(?:s|ed|ade)?)\b", re.I)),
    ("activates", re.compile(r"\b(activate(?:s|d|ion)?)\b", re.I)),
    ("targets", re.compile(r"\b(target(?:s|ed|ing)?)\b", re.I)),
    ("binds", re.compile(r"\b(bind(?:s|ing)?)\b", re.I)),
    ("resistant_to", re.compile(r"\b(resistan(?:t|ce)\s+to)\b", re.I)),
    ("sensitizes", re.compile(r"\b(sensiti[sz]e(?:s|d|ation)?)\b", re.I)),
    ("associated_with", re.compile(r"\b(associat(?:ed|ion)\s+with)\b", re.I)),
]
NUM = [
    ("IC50", re.compile(r"\bIC50\s*(?:=|:)?\s*(\d+(?:\.\d+)?)\s*(pM|nM|uM|µM|mM)?\b", re.I)),
    ("p",    re.compile(r"\bp\s*(?:=|<|≤)\s*(\d+(?:\.\d+)?(?:e-?\d+)?)\b", re.I)),
]

ROLE_MAP = {
    "drug": "drug", "compound": "drug", "inhibitor": "drug", "therapy": "drug", "antibody": "drug",
    "gene": "gene", "protein": "gene", "target": "gene", "biomarker": "gene",
    "pathway": "pathway", "reactome": "pathway", "go": "pathway", "kegg": "pathway",
    "disease": "disease", "cancer": "disease", "phenotype": "disease",
}

SEC_WEIGHT = {
    "ABSTRACT": 1.0, "RESULTS": 1.0,
    "INTRODUCTION": 0.8, "DISCUSSION": 0.8,
    "METHODS": 0.5, "METHOD": 0.5,
    "CONCLUSION": 0.9, "CONCLUSIONS": 0.9,
    "BACKGROUND": 0.7,
    "REFERENCES": 0.1, "ACKNOWLEDGEMENTS": 0.1,
    "UNKNOWN": 0.7, "PREFACE": 0.7,
}

# -------------------- Data classes --------------------
@dataclass
class Ent:
    text: str
    label: str
    start: int
    end: int
    score: float

# -------------------- PDF/TXT text --------------------
def clean_text(s: str) -> str:
    s = re.sub(r"(\w)-\n(\w)", r"\1\2", s)
    s = s.replace("\r", "")
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\n{2,}", "\n\n", s)
    return s

def pdf_to_text(path: str, max_pages: int = 0) -> str:
    last = None
    if fitz is not None:
        try:
            doc = fitz.open(path)
            out = []
            for i, p in enumerate(doc):
                if max_pages and i >= max_pages:
                    break
                out.append(p.get_text("text") or "")
            doc.close()
            if out:
                return "".join(out)
        except Exception as e:
            last = e
    if pdfminer_extract_text is not None:
        try:
            return pdfminer_extract_text(path, maxpages=(max_pages or None)) or ""
        except Exception as e:
            last = e
    raise RuntimeError(f"Could not extract text from PDF; install PyMuPDF or pdfminer.six. Last: {last}")

def load_text_file(path: str) -> str:
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as fh:
            return fh.read()
    except Exception:
        return ""

def split_sections(raw: str) -> List[Tuple[str, str]]:
    text = clean_text(raw)
    hits = list(SECTION_RE.finditer(text))
    out = []
    if not hits:
        return [("UNKNOWN", text)]
    if hits[0].start() > 0:
        out.append(("PREFACE", text[:hits[0].start()]))
    for i, m in enumerate(hits):
        name = m.group(1).upper()
        start = m.end()
        end = hits[i + 1].start() if i + 1 < len(hits) else len(text)
        out.append((name, text[start:end]))
    return out

def split_sentences(s: str) -> List[str]:
    parts = re.split(SENT_SPLIT_RE, s.strip())
    return [p.strip() for p in parts if len(p.strip()) >= 20]

# -------------------- Vocabulary --------------------
def load_vocab(vpath: str) -> tuple[list[str], dict[str, str], dict[str, str]]:
    ext = os.path.splitext(vpath)[1].lower()
    v = pl.read_parquet(vpath) if ext == ".parquet" else pl.read_csv(vpath)
    if "term" not in v.columns:
        v = v.rename({v.columns[0]: "term"})
    if "canonical" not in v.columns:
        v = v.with_columns(pl.lit(None).alias("canonical"))
    if "term_type" not in v.columns:
        v = v.with_columns(pl.lit("unknown").alias("term_type"))
    v = (v.select(
            pl.col("term").cast(pl.Utf8).str.strip().alias("term"),
            pl.col("canonical").cast(pl.Utf8),
            pl.col("term_type").cast(pl.Utf8),
        ).drop_nulls(subset=["term"]).unique(subset=["term"], keep="first"))
    terms = v["term"].to_list()
    canon_vals = v["canonical"].fill_null(v["term"]).to_list()
    canon = {(t.lower() if isinstance(t, str) else t): c for t, c in zip(terms, canon_vals)}
    type_map = {}
    for t, tt in zip(v["term"].to_list(), v["term_type"].to_list()):
        if isinstance(t, str):
            type_map[t.lower()] = tt or "unknown"
    return terms, canon, type_map

def role_for(norm_term: str, type_map: dict[str, str]) -> str:
    tt = type_map.get(norm_term.lower(), "").lower()
    for k, v in ROLE_MAP.items():
        if k in tt:
            return v
    return "unknown"

# -------------------- NER Backends --------------------
class NerBackend:
    name = "base"
    def extract(self, text: str) -> list[Ent]:
        return []

# SciSpaCy (preferred if available)
class SciSpacyBackend(NerBackend):
    def __init__(self, model_name: str = "en_core_sci_sm", use_abbrev: bool = True):
        if not _HAS_SPACY:
            raise RuntimeError("spaCy not installed")
        try:
            self.nlp = spacy.load(model_name)
        except Exception as e:
            raise RuntimeError(f"Could not load spaCy model '{model_name}': {e}")
        if use_abbrev and _HAS_ABBR:
            self.nlp.add_pipe("abbreviation_detector")
        self.name = f"scispacy:{model_name}"

    def extract(self, text: str) -> list[Ent]:
        out: list[Ent] = []
        doc = self.nlp(text)
        for e in doc.ents:
            if not e.text or len(e.text) < 3:
                continue
            out.append(Ent(e.text, e.label_, int(e.start_char), int(e.end_char), 0.85))
        # expand abbreviations if present
        if _HAS_ABBR and "abbreviation_detector" in self.nlp.pipe_names:
            try:
                ad = self.nlp.get_pipe("abbreviation_detector")
                for abrv in ad(doc):
                    lf = str(abrv._.long_form) if hasattr(abrv._, "long_form") else None
                    if lf:
                        out.append(Ent(lf, "ABBR_LONGFORM", int(abrv.start_char), int(abrv.end_char), 0.75))
            except Exception:
                pass
        return out

# Transformers NER
class HFTransformersBackend(NerBackend):
    def __init__(self, model_name: str = "dslim/bert-base-NER"):
        if not _HAS_TXF:
            raise RuntimeError("transformers not installed")
        self.pipe = pipeline("token-classification", model=model_name, aggregation_strategy="simple")
        self.name = f"hf:{model_name}"

    def extract(self, text: str) -> list[Ent]:
        out: list[Ent] = []
        try:
            for r in self.pipe(text):
                out.append(Ent(r["word"], r.get("entity_group", "ENT"), int(r["start"]), int(r["end"]), float(r.get("score", 0.8))))
        except Exception:
            pass
        return out

# FlashText Vocab (keyword) – always available (fallback to regex-lite if lib missing)
class VocabKeywordBackend(NerBackend):
    def __init__(self, terms: list[str]):
        self.name = "vocab_flashtext" if _HAS_FT else "vocab_regex"
        self.terms = [t for t in terms if isinstance(t, str) and t]
        if _HAS_FT:
            self.kp = KeywordProcessor(case_sensitive=False)
            for t in self.terms:
                self.kp.add_keyword(t)
        else:
            safe = [re.escape(t) for t in self.terms]
            safe.sort(key=len, reverse=True)
            # chunk to avoid huge regex
            self.pats = [re.compile(r"(?i)\b(" + "|".join(safe[i:i + 64]) + r")\b") for i in range(0, len(safe), 64)]

    def extract(self, text: str) -> list[Ent]:
        out: list[Ent] = []
        if not text:
            return out
        if _HAS_FT:
            for m in self.kp.extract_keywords(text, span_info=True):
                k, s, e = m  # type: ignore
                out.append(Ent(k, "VOCAB", int(s), int(e), 0.7))
        else:
            for p in self.pats:
                for m in p.finditer(text):
                    out.append(Ent(m.group(0), "VOCAB", int(m.start()), int(m.end()), 0.6))
        return out

# -------------------- Ensemble + merge --------------------
def dedup_entities(ents: list[Ent]) -> list[Ent]:
    # prefer longer spans and higher scores
    ents = sorted(ents, key=lambda e: (e.start, -(e.end - e.start), -e.score))
    out: list[Ent] = []
    last_end = -1
    for e in ents:
        if e.start >= last_end:
            out.append(e)
            last_end = e.end
    return out

def normalize_entity(txt: str, canon: dict[str, str]) -> str:
    return canon.get(txt.lower(), txt)

# -------------------- Qualifiers / confidence --------------------
def extract_qualifiers(sent: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for k, rx in NUM:
        m = rx.search(sent)
        if not m:
            continue
        if k == "IC50":
            out[k] = f"{m.group(1)}{(' ' + (m.group(2) or '')).strip()}"
        else:
            out[k] = m.group(1)
    return out

def sec_weight(section: str) -> float:
    return SEC_WEIGHT.get(section.upper(), 0.7)

def rel_confidence(predicate: str, q: dict[str, str], n_mentions: int, section: str, neg: bool) -> float:
    base = {
        "inhibits": 0.9, "activates": 0.9, "targets": 0.85, "binds": 0.8,
        "resistant_to": 0.7, "sensitizes": 0.7, "associated_with": 0.6,
    }.get(predicate, 0.5)
    if any(k in q for k in ("IC50", "p")):
        base += 0.05
    if n_mentions >= 3:
        base += 0.05
    base *= sec_weight(section)
    if neg:
        base -= 0.3
    return max(0.0, min(1.0, base))

# -------------------- Main processing --------------------
def build_backends(prefer_bc5cdr: bool, use_hf: bool, vocab_terms: list[str]) -> list[NerBackend]:
    backends: list[NerBackend] = []
    # SciSpaCy first (if available)
    if _HAS_SPACY:
        tried = []
        if prefer_bc5cdr:
            tried = ["en_ner_bc5cdr_md", "en_core_sci_md", "en_core_sci_sm"]
        else:
            tried = ["en_core_sci_sm", "en_core_sci_md", "en_ner_bc5cdr_md"]
        for m in tried:
            try:
                backends.append(SciSpacyBackend(m))
                break
            except Exception:
                continue
    # Transformers next (optional)
    if use_hf and _HAS_TXF:
        for model in ("dslim/bert-base-NER",):
            try:
                backends.append(HFTransformersBackend(model))
                break
            except Exception:
                continue
    # Vocab keyword fallback (always)
    backends.append(VocabKeywordBackend(vocab_terms))
    return backends

def process_sentence(s: str, backends: list[NerBackend], canon: dict[str, str], type_map: dict[str, str]) -> list[dict]:
    ents: list[Ent] = []
    for be in backends:
        try:
            ents.extend(be.extract(s))
        except Exception:
            continue
    ents = dedup_entities(ents)
    rows = []
    for e in ents:
        norm = normalize_entity(e.text, canon)
        ttype = type_map.get(norm.lower(), "unknown")
        rows.append({
            "ent_text": e.text, "ent_norm": norm, "ent_type": ttype,
            "start": e.start, "end": e.end, "score": e.score
        })
    return rows

def find_predicate(sent: str) -> str:
    for pr, rx in REL:
        if rx.search(sent):
            return pr
    return ""

def is_negated(sent: str) -> bool:
    return bool(NEG_RE.search(sent))

def assemble_relations(sent: str, section: str, ents_norm_rows: list[dict], type_map: dict[str, str]) -> list[dict]:
    # group canon mentions by role
    drugs = [r["ent_norm"] for r in ents_norm_rows if role_for(r["ent_norm"], type_map) == "drug"]
    targets = [r["ent_norm"] for r in ents_norm_rows if role_for(r["ent_norm"], type_map) in ("gene", "pathway", "disease")]
    if not drugs or not targets:
        return []
    pred = find_predicate(sent)
    if not pred:
        return []
    neg = is_negated(sent)
    q = extract_qualifiers(sent)
    conf = rel_confidence(pred, q, len(ents_norm_rows), section, neg)
    out = []
    for d in dict.fromkeys(drugs):
        for t in dict.fromkeys(targets):
            if d == t:
                continue
            out.append({
                "subject": d, "subject_role": "drug",
                "predicate": pred,
                "object": t, "object_role": role_for(t, type_map),
                "negated": neg,
                "qualifiers_json": json.dumps(q) if q else "",
                "confidence": conf,
            })
    return out

def main():
    ap = argparse.ArgumentParser(description="Advanced biomedical unstructured extractor (ensemble NER + relations).")
    ap.add_argument("--in", dest="in_dir", required=True, help="Folder with PDFs and/or .txt")
    ap.add_argument("--vocab", required=True, help="Vocabulary CSV/Parquet (with term, term_type[, canonical])")
    ap.add_argument("--out", dest="out_dir", required=True, help="Output folder")
    ap.add_argument("--prefer-bc5cdr", action="store_true", help="Prefer SciSpaCy chemical/disease model if available")
    ap.add_argument("--use-hf", action="store_true", help="Enable HuggingFace NER pipeline if installed")
    ap.add_argument("--max-pages", type=int, default=0, help="Max pages per PDF (0 = all)")
    ap.add_argument("--csv", choices=["flat", "none"], default="flat")
    args = ap.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)

    terms, canon, type_map = load_vocab(args.vocab)
    backends = build_backends(args.prefer_bc5cdr, args.use_hf, terms)
    console.log(f"NER backends: {[b.name for b in backends]}")

    pdfs = sorted(set(glob.glob(os.path.join(args.in_dir, "**", "*.pdf"), recursive=True)))
    txts = sorted(set(glob.glob(os.path.join(args.in_dir, "**", "*.txt"), recursive=True)))
    files = pdfs + txts
    if not files:
        console.print(f"[red]No PDFs or TXTs under {args.in_dir}[/red]")
        sys.exit(1)

    bar = BarColumn(bar_width=None)
    cols = [TextColumn("[progress.description]{task.description}"), bar, TimeElapsedColumn(), TimeRemainingColumn()]
    ENT_ROWS: list[dict] = []
    REL_ROWS: list[dict] = []
    DOC_ROWS: list[dict] = []

    with Progress(*cols, console=console) as prog:
        t = prog.add_task("[bold cyan]Extracting unstructured", total=len(files))
        for path in files:
            try:
                if path.lower().endswith(".pdf"):
                    raw = pdf_to_text(path, args.max_pages)
                else:
                    raw = load_text_file(path)
                if not raw.strip():
                    prog.advance(t); continue

                rel = os.path.relpath(path, args.in_dir).replace("\\", "/")
                ddir = os.path.dirname(rel); dfile = os.path.basename(rel); dabs = os.path.abspath(path)

                sections = split_sections(raw)
                sent_id = 0
                for sec_name, body in sections:
                    # Skip references/acks in relation extraction (but still capture entities)
                    sents = split_sentences(body)
                    for s in sents:
                        sent_id += 1
                        ents_rows = process_sentence(s, backends, canon, type_map)
                        # entities
                        for r in ents_rows:
                            ENT_ROWS.append({
                                "doc_relpath": rel, "doc_dir": ddir, "doc_file": dfile, "doc_abs": dabs,
                                "section": sec_name, "sentence_id": sent_id, "sentence": s,
                                **r
                            })
                        # relations (skip in References/Acknowledgements)
                        if sec_name.upper() in ("REFERENCES", "ACKNOWLEDGEMENTS"):
                            continue
                        REL_ROWS.extend([
                            {
                                "doc_relpath": rel, "doc_dir": ddir, "doc_file": dfile, "doc_abs": dabs,
                                "section": sec_name, "sentence_id": sent_id, "sentence": s,
                                **rel_row
                            } for rel_row in assemble_relations(s, sec_name, ents_rows, type_map)
                        ])

                DOC_ROWS.append({"doc_relpath": rel, "doc_dir": ddir, "doc_file": dfile, "doc_abs": dabs})
            except Exception as e:
                console.log(f"[yellow]Skip {path}[/yellow]: {e}")
            prog.advance(t)

    # ---- write outputs ----
    ent_df = pl.DataFrame(ENT_ROWS) if ENT_ROWS else pl.DataFrame({"doc_relpath": []})
    rel_df = pl.DataFrame(REL_ROWS) if REL_ROWS else pl.DataFrame({"doc_relpath": []})
    doc_df = pl.DataFrame(DOC_ROWS) if DOC_ROWS else pl.DataFrame({"doc_relpath": []})

    ent_parq = os.path.join(args.out_dir, "entities.parquet")
    rel_parq = os.path.join(args.out_dir, "relations.parquet")
    ent_df.write_parquet(ent_parq)
    rel_df.write_parquet(rel_parq)
    doc_df.write_csv(os.path.join(args.out_dir, "doc_index.csv"))

    if args.csv != "none":
        if not ent_df.is_empty():
            ent_df.select([
                pl.col("doc_relpath"), pl.col("doc_dir"), pl.col("doc_file"), pl.col("section"),
                pl.col("sentence_id"),
                pl.col("ent_text"), pl.col("ent_norm"), pl.col("ent_type"),
                pl.col("score")
            ]).write_csv(os.path.join(args.out_dir, "entities.csv"))
        if not rel_df.is_empty():
            rel_df.select([
                pl.col("doc_relpath"), pl.col("doc_dir"), pl.col("doc_file"), pl.col("section"),
                pl.col("sentence_id"),
                pl.col("subject"), pl.col("predicate"), pl.col("object"),
                pl.col("subject_role"), pl.col("object_role"),
                pl.col("negated"), pl.col("confidence"), pl.col("qualifiers_json")
            ]).write_csv(os.path.join(args.out_dir, "relations.csv"))

    with open(os.path.join(args.out_dir, "run_report.md"), "w", encoding="utf-8") as f:
        f.write("# Unstructured extraction report\n\n")
        f.write(f"- Documents processed: **{doc_df.height}**\n")
        f.write(f"- Entity rows: **{ent_df.height}**\n")
        f.write(f"- Relation rows: **{rel_df.height}**\n")
        if not ent_df.is_empty():
            f.write(f"- Unique normalized entities: **{ent_df['ent_norm'].n_unique()}**\n")
        if not rel_df.is_empty():
            f.write(f"- Unique edges: **{rel_df.select(['subject','predicate','object']).unique().height}**\n")

    console.rule("[bold green]Unstructured extraction complete")

if __name__ == "__main__":
    main()
