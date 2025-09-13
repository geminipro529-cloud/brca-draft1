#!/usr/bin/env python3
# make_seeds_from_gaps.py — converts gap_report.json into seed templates in data/00_urls/
import json, argparse
from pathlib import Path

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--gap", default="data/_validation/gap_report.json")
    ap.add_argument("--outdir", default="data/00_urls")
    args = ap.parse_args()

    outdir = Path(args.outdir); outdir.mkdir(parents=True, exist_ok=True)
    seeds = {"brca.txt": [], "brca_nanomedicine.txt": [], "nanomedicine.txt": []}

    data = json.loads(Path(args.gap).read_text(encoding="utf-8"))
    gaps = data.get("gaps", [])
    for g in gaps:
        key = g.get("dataset","").lower()
        if "brca" in key and any(t in key for t in ("nano","nanomed")):
            seeds["brca_nanomedicine.txt"].append(f"# NEED: {g.get('msg') or g.get('ids')}")
        elif "brca" in key:
            seeds["brca.txt"].append(f"# NEED: {g.get('msg') or g.get('ids')}")
        else:
            seeds["nanomedicine.txt"].append(f"# NEED: {g.get('msg') or g.get('ids')}")

    # Trust domains (headers) to guide manual fills; safe defaults (no placeholders for URLs you don't have)
    header = [
        "# Add one URL/DOI per line (ftp/http/s3/gs).",
        "# Prefer primary sources: GEO/ArrayExpress/GDC/PRIDE/Zenodo/figshare.",
        "# Examples of trusted domains to consider (remove if not used):",
        "## geo.ncbi.nlm.nih.gov / ftp.ncbi.nlm.nih.gov",
        "## portal.gdc.cancer.gov / api.gdc.cancer.gov",
        "## www.ebi.ac.uk/arrayexpress / www.ebi.ac.uk/pride",
        "## zenodo.org / figshare.com",
        "## osf.io / dataverse.org",
        "",
    ]
    for name, lines in seeds.items():
        p = outdir / name
        content = "\n".join(header + lines) + ("\n" if lines or header else "")
        p.write_text(content, encoding="utf-8")
        print(f"[OK] Wrote {p}")

if __name__ == "__main__":
    main()
