#!/usr/bin/env python3
import argparse, csv, json
from pathlib import Path

BC_KEYWORDS = {
  "breast","mammary","er+","er-","estrogen receptor","progesterone receptor","pr+","pr-",
  "her2","erbb2","her-2","her2+","her2-","luminal a","luminal b","triple-negative","tnbc",
  "basal-like","ductal","lobular","brca1","brca2","ar+","androgen receptor","ki-67","ki67"
}
BC_NORM_KEYS = {"breast cancer","er","pr","her2","erbb2","tnbc","luminal a","luminal b","brca1","brca2"}

def text_has_bc(t: str) -> bool:
    return bool(t) and any(k in t.casefold() for k in BC_KEYWORDS)

def record_is_bc(rec: dict) -> bool:
    if rec.get("normalized","").casefold() in BC_NORM_KEYS: return True
    return any(text_has_bc(rec.get(k,"")) for k in ("snippet","sentence_text","prev_sentence","next_sentence","doc_path"))

def main(src_dir: Path, dst_dir: Path):
    docs_dir = src_dir / "docs"
    dst_dir.mkdir(parents=True, exist_ok=True)
    out_bc = dst_dir / "bc_annotations.jsonl"
    out_idx = dst_dir / "bc_doc_index.csv"
    out_topk = dst_dir / "bc_unknown_terms_topk.csv"
    src_idx = src_dir / "doc_index.csv"
    unk_jsonl = src_dir / "unknown_counts.jsonl"

    kept = 0
    bc_docs = set()
    with out_bc.open("w", encoding="utf-8") as fout:
        for jf in docs_dir.rglob("*.jsonl"):
            with jf.open("r", encoding="utf-8") as f:
                for line in f:
                    try: rec = json.loads(line)
                    except: continue
                    if record_is_bc(rec):
                        fout.write(json.dumps(rec, ensure_ascii=False) + "\n")
                        bc_docs.add(rec.get("doc_path",""))
                        kept += 1

    if src_idx.exists():
        with src_idx.open("r", encoding="utf-8") as fin, out_idx.open("w", encoding="utf-8", newline="") as fout:
            rd, wr = csv.reader(fin), csv.writer(fout)
            hdr = next(rd); wr.writerow(hdr)
            dp = hdr.index("doc_path")
            for row in rd:
                if row[dp] in bc_docs: wr.writerow(row)

    counts = {}
    if unk_jsonl.exists():
        with unk_jsonl.open("r", encoding="utf-8") as f:
            for line in f:
                try: rec = json.loads(line)
                except: continue
                if rec.get("doc_path","") in bc_docs:
                    for term, cnt in rec.get("counts", []):
                        counts[term] = counts.get(term, 0) + int(cnt)
    items = sorted(counts.items(), key=lambda x: -x[1])[:20000]
    with out_topk.open("w", encoding="utf-8", newline="") as f:
        wr = csv.writer(f); wr.writerow(["term","count"]); wr.writerows(items)

    print(f"[DONE] kept {kept} BC annotations")
    print(f"[OUT]  {out_bc}")
    print(f"[OUT]  {out_idx}")
    print(f"[OUT]  {out_topk}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--infile", required=True, help="unstructured output dir from extractor")
    ap.add_argument("--out", required=True, help="destination for BC-only outputs")
    a = ap.parse_args()
    main(Path(a.infile), Path(a.out))
