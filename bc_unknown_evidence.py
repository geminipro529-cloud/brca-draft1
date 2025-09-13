#!/usr/bin/env python3
import argparse, csv, json
from pathlib import Path

def main(bc_ann_path: Path, topk_csv: Path, out_csv: Path, max_examples: int):
    # load top terms (smallish)
    terms = []
    with topk_csv.open("r", encoding="utf-8") as f:
        rd = csv.DictReader(f)
        for r in rd:
            terms.append((r["term"], int(r["count"])))
    terms = [t for t,_ in terms[:2000]]  # cap

    want = {t: [] for t in terms}
    need = set(terms)

    def maybe_add(term, rec):
        arr = want[term]
        if len(arr) < max_examples:
            arr.append((rec.get("doc_path",""), rec.get("snippet","") or rec.get("sentence_text","")))
            if len(arr) >= max_examples:
                need.discard(term)

    with bc_ann_path.open("r", encoding="utf-8") as f:
        for line in f:
            rec = json.loads(line)
            text = (rec.get("snippet") or rec.get("sentence_text") or "").casefold()
            if not text: continue
            for t in list(need):
                if t in text:
                    maybe_add(t, rec)
            if not need: break

    with out_csv.open("w", encoding="utf-8", newline="") as f:
        wr = csv.writer(f)
        wr.writerow(["term","example_doc","example_snippet"])
        for t, exs in want.items():
            for doc, snip in exs:
                wr.writerow([t, doc, snip])

    print(f"[OUT] {out_csv}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--bc-annotations", required=True)
    ap.add_argument("--topk", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--max-examples", type=int, default=5)
    a = ap.parse_args()
    main(Path(a.bc_annotations), Path(a.topk), Path(a.out), a.max_examples)
