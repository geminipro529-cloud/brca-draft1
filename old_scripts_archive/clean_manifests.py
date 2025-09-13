# File: scripts/clean_manifests.py
# Purpose: Merge multiple annotated manifests into a unified clean URL list.
# Usage:
#   python scripts\clean_manifests.py --in-dir data\00_urls\brca --out data\00_urls\brca\clean_brca_all.txt --csv data\00_urls\brca\clean_brca_all.csv

import os, re, csv, argparse

URL_RX = re.compile(r'(?i)\b(?:https?|ftp)://[^\s<>"\'\)\]]+')

def parse_manifest(path):
    """Extract (url,note,sourcefile) from one manifest file."""
    urls=[]
    with open(path,"r",encoding="utf-8",errors="ignore") as f:
        for line in f:
            line=line.strip()
            if not line or line.startswith("#"): continue
            note=""
            if "#" in line:
                url_part,note=line.split("#",1)
                line=url_part.strip(); note=note.strip()
            matches=URL_RX.findall(line)
            for u in matches:
                if u.startswith(("http://","https://","ftp://")):
                    urls.append((u,note,os.path.basename(path)))
    return urls

def clean_manifests(in_dir, out_txt, out_csv=None):
    all_urls=[]
    for root,_,files in os.walk(in_dir):
        for f in files:
            if f.lower().endswith((".txt",".csv",".tsv")):
                all_urls.extend(parse_manifest(os.path.join(root,f)))
    # Deduplicate by URL
    seen=set(); cleaned=[]
    for u,n,src in all_urls:
        if u not in seen:
            seen.add(u); cleaned.append((u,n,src))
    # Write TXT (one URL per line)
    os.makedirs(os.path.dirname(out_txt), exist_ok=True)
    with open(out_txt,"w",encoding="utf-8") as f:
        for u,_,_ in cleaned: f.write(u+"\n")
    # Optional CSV
    if out_csv:
        with open(out_csv,"w",encoding="utf-8",newline="") as f:
            w=csv.writer(f); w.writerow(["url","note","source_file"]); w.writerows(cleaned)
    print(f"[OK] {len(cleaned)} unique URLs → {out_txt}" + (f" and {out_csv}" if out_csv else ""))

if __name__=="__main__":
    ap=argparse.ArgumentParser()
    ap.add_argument("--in-dir", required=True, help="Directory with manifest files")
    ap.add_argument("--out", required=True, help="Output clean TXT")
    ap.add_argument("--csv", help="Optional CSV with notes+source")
    a=ap.parse_args()
    clean_manifests(a.in_dir,a.out,a.csv)
