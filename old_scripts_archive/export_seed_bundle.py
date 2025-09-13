#!/usr/bin/env python3
# export_seed_bundle.py — bundle all seed links/crawlers/DOIs into neat exports
# Outputs:
#   data/_validation/export_brca_nanomedicine.txt
#   data/_validation/export_nanomedicine.txt
#   data/_validation/export_all_seeds.txt

import os, re

SEED_DIR = os.path.join("data","00_urls")
DYN = [
    os.path.join("data","01_raw","NANOMEDICINE","_progress","crawl_discovered.txt"),
    os.path.join("data","01_raw","NANOMEDICINE","_source_finder","next_seeds.txt"),
]
OUT_DIR = os.path.join("data","_validation")

URL_RE = re.compile(r'\b(?:https?|ftp)://[^\s"\'<>()]+', re.I)
DOI_RE = re.compile(r'10\.\d{4,9}/[^\s"\'<>]+', re.I)

def read_lines(path):
    try:
        with open(path,"r",encoding="utf-8",errors="ignore") as f:
            for line in f:
                line=line.strip()
                if not line or line.startswith("#"): continue
                yield line
    except FileNotFoundError:
        return

def harvest():
    urls, dois = set(), set()
    # seeds under data/00_urls
    for r,_,fs in os.walk(SEED_DIR):
        for fn in fs:
            if os.path.splitext(fn.lower())[1] in (".txt",".csv",".lst",".tsv",".urls"):
                for line in read_lines(os.path.join(r,fn)):
                    for m in URL_RE.findall(line): urls.add(m)
                    for m in DOI_RE.findall(line): dois.add(m.strip(").,;]"))
    # dynamic discoveries
    for p in DYN:
        for line in read_lines(p):
            for m in URL_RE.findall(line): urls.add(m)
    return sorted(urls), sorted(dois)

def write(path, items):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path,"w",encoding="utf-8") as f:
        for x in items: f.write(x+"\n")

def main():
    urls, dois = harvest()
    # filters
    def is_brca(s):   s=s.lower(); return ("breast" in s or "brca" in s or "mammary" in s)
    def is_nano(s):   s=s.lower(); return ("nano" in s or "liposome" in s or "exosome" in s or "lipid nanoparticle" in s or "lnp" in s or "nanoparticle" in s)

    all_items = urls + dois
    nm_items  = [x for x in all_items if is_nano(x)]
    brca_nm   = [x for x in nm_items   if is_brca(x)]

    write(os.path.join(OUT_DIR, "export_all_seeds.txt"),       all_items)
    write(os.path.join(OUT_DIR, "export_nanomedicine.txt"),    nm_items)
    write(os.path.join(OUT_DIR, "export_brca_nanomedicine.txt"), brca_nm)

    print(f"Wrote: {OUT_DIR}\\export_all_seeds.txt "
          f"({len(all_items)} items), export_nanomedicine.txt ({len(nm_items)}), "
          f"export_brca_nanomedicine.txt ({len(brca_nm)})")

if __name__=="__main__": main()
