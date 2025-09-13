# === install & run the neutral ID reader ===
Set-Location D:\breast_cancer_project_root
New-Item -ItemType Directory -Force -Path scripts, data\02_interim\diagnostics | Out-Null

@'
#!/usr/bin/env python3
# scripts/id_reader.py — generic ID inventory (GEO/SRA/ENA/EGA/ArrayExpress/PRIDE/TCGA/CPTAC/…)
# Minimal deps: pandas (pyarrow optional). Windows-safe; uses os/os.path.

import os, sys, argparse, glob, tempfile, json, time, random, re
import concurrent.futures as cf
import pandas as pd

try:
    from rich.console import Console
    from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn
    RICH=True; console=Console()
except Exception:
    RICH=False

NS_PAT = {
    "GSE":   re.compile(r"\bGSE\d+\b", re.I),
    "GSM":   re.compile(r"\bGSM\d+\b", re.I),
    "SRR":   re.compile(r"\bSRR\d+\b", re.I),
    "SRX":   re.compile(r"\bSRX\d+\b", re.I),
    "SRS":   re.compile(r"\bSRS\d+\b", re.I),
    "PRJNA": re.compile(r"\bPRJNA\d+\b", re.I),
    "SAMN":  re.compile(r"\bSAMN\d+\b", re.I),
    "ERR":   re.compile(r"\bERR\d+\b", re.I),
    "ERX":   re.compile(r"\bERX\d+\b", re.I),
    "ERP":   re.compile(r"\bERP\d+\b", re.I),
    "EGAD":  re.compile(r"\bEGAD\d+\b", re.I),
    "EGAS":  re.compile(r"\bEGAS\d+\b", re.I),
    "EGAN":  re.compile(r"\bEGAN\d+\b", re.I),
    "E-MTAB":re.compile(r"\bE-MTAB-\d+\b", re.I),
    "E-GEOD":re.compile(r"\bE-GEOD-\d+\b", re.I),
    "E-MEXP":re.compile(r"\bE-MEXP-\d+\b", re.I),
    "PXD":   re.compile(r"\bPXD\d{5,7}\b", re.I),
    "TCGA_PAT":  re.compile(r"\bTCGA-[A-Z0-9]{2}-[A-Z0-9]{4}\b", re.I),
    "TCGA_SAMP": re.compile(r"\bTCGA-[A-Z0-9]{2}-[A-Z0-9]{4}-\d{2}[A-Z]\b", re.I),
    "CPTAC":     re.compile(r"\bCPTAC[-_ ]?\d+\b", re.I),
    "UUID":      re.compile(r"\b[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[089ab][0-9a-f]{3}-[0-9a-f]{12}\b", re.I),
}

BREAST = { "breast cancer","brca","tnbc","triple-negative","her2","er+","pr+","luminal","basal",
           "mda-mb-231","mcf-7","t47d","bt-474","sk-br-3","4t1","pdx","xenograft" }
NANO   = { "nanoparticle","nanomedicine","liposome","lnp","exosome","micelle","dendrimer","plga","peg",
           "spion","iron oxide","gold nanoparticle","aunp","silver nanoparticle","agnp","quantum dot","qd",
           "graphene","mof","silica","sio2","chitosan","albumin","nanocapsule","biodistribution","pk",
           "zeta potential","hydrodynamic diameter","pdi" }

GENE_COLS = {
    "gene","hugo_symbol","ensembl","gene_symbol","entrez","feature","id","symbol","geneid",
    "gene_name","hgnc_symbol","ensembl_gene_id","entrez_id"
}
ID_CAND_COLS = [
    "sample","sample_id","sample_barcode","tumor_sample_barcode","aliquot","aliquot_id","aliquot_barcode",
    "submitter_id","case_id","case_submitter_id","patient_id","subject_id","participant_id","barcode",
    "gsm","gse","srr","srx","srs","err","erx","erp","prjna","samn","pxd","accession","run_accession"
]

def iter_paths(spec):
    if not spec: return []
    out=[]
    for tok in str(spec).split(";"):
        s = tok.strip().replace("“", '"').replace("”", '"').strip('"').strip("'")
        if not s: continue
        s = os.path.expandvars(os.path.expanduser(s))
        if os.path.isdir(s):
            for ext in ("*.csv","*.tsv","*.parquet"):
                out+=glob.glob(os.path.join(s,"**",ext), recursive=True)
        elif any(ch in s for ch in "*?[]"):
            out+=glob.glob(s, recursive=True)
        elif os.path.exists(s):
            out.append(s)
    seen=set(); out2=[]
    for p in out:
        rp=os.path.normpath(os.path.abspath(p))
        if os.path.exists(rp) and rp not in seen:
            seen.add(rp); out2.append(rp)
    return sorted(out2)

def csv_header(path):
    try: return list(pd.read_csv(path, sep=None, engine="python", nrows=0).columns)
    except Exception: return []

def csv_sample(path, n=800):
    try: return pd.read_csv(path, sep=None, engine="python", nrows=n, dtype="string")
    except Exception: return pd.DataFrame()

def csv_col(path, col):
    try:
        return pd.read_csv(path, sep=None, engine="python", usecols=[col], dtype="string")[col].astype(str).tolist()
    except Exception: return []

def pq_columns(path):
    try:
        import pyarrow.parquet as pq
        return list(pq.ParquetFile(path).schema.names)
    except Exception:
        try: return list(pd.read_parquet(path, engine=None).head(0).columns)
        except Exception: return []

def pq_col(path, col):
    try: return pd.read_parquet(path, columns=[col], engine=None)[col].astype(str).tolist()
    except Exception:
        try: return pd.read_parquet(path, engine=None)[col].astype(str).tolist()
        except Exception: return []

def header_tags(cols):
    text=" ".join(map(str, cols)).lower()
    tags=[]
    if any(w in text for w in BREAST): tags.append("breast")
    if any(w in text for w in NANO):   tags.append("nano")
    return ";".join(tags)

def scan_values(series):
    counts={k:0 for k in NS_PAT}; examples={k:[] for k in NS_PAT}
    for v in series.fillna("").astype(str).tolist():
        for ns,pat in NS_PAT.items():
            if pat.search(v):
                counts[ns]+=1
                if len(examples[ns])<5 and v not in examples[ns]:
                    examples[ns].append(v)
    counts={k:v for k,v in counts.items() if v>0}
    examples={k:v for k,v in examples.items() if v}
    return counts, examples

def analyze_file(path):
    ext=os.path.splitext(path)[1].lower()
    row={"file":path,"ext":ext or "","size":os.path.getsize(path) if os.path.exists(path) else 0,
         "mode":"none","header_tags":"","id_columns":"","namespaces":"","ns_counts":"","examples":""}
    try:
        if ext in (".csv",".tsv",".txt"):
            cols=csv_header(path)
            if not cols: return row
            row["header_tags"]=header_tags(cols)
            # wide header heuristic
            non_gene=[c for c in cols if str(c).lower() not in GENE_COLS]
            wide=[c for c in non_gene if any(p.search(str(c)) for p in NS_PAT.values())]
            # value scan on likely columns or a capped set
            pick=[c for c in cols if str(c).lower() in ID_CAND_COLS] or cols[:min(12,len(cols))]
            v_counts={}; v_examples={}
            df=csv_sample(path, n=800)
            if not df.empty:
                for c in pick:
                    if c not in df.columns: continue
                    cnts, ex = scan_values(df[c])
                    for ns,v in cnts.items(): v_counts[ns]=v_counts.get(ns,0)+v
                    for ns,vals in ex.items():
                        v_examples.setdefault(ns,[])
                        for x in vals:
                            if len(v_examples[ns])<5 and x not in v_examples[ns]: v_examples[ns].append(x)
            ns_union=set(v_counts.keys())
            row["mode"]="wide_header" if len(wide)>=3 else ("value_scan" if v_counts else "none")
            row["id_columns"]=";".join([str(c) for c in pick])
            row["namespaces"]=";".join(sorted(ns_union))
            row["ns_counts"]=";".join(f"{k}={v_counts.get(k,0)}" for k in sorted(ns_union)) if v_counts else ""
            row["examples"]=";".join(f"{k}:"+"|".join(v_examples.get(k,[])) for k in sorted(v_examples)) if v_examples else ""
            return row

        if ext==".parquet":
            cols=pq_columns(path)
            if not cols: return row
            row["header_tags"]=header_tags(cols)
            non_gene=[c for c in cols if str(c).lower() not in GENE_COLS]
            wide=[c for c in non_gene if any(p.search(str(c)) for p in NS_PAT.values())]
            pick=[c for c in cols if str(c).lower() in ID_CAND_COLS][:12]
            v_counts={}; v_examples={}
            for c in pick:
                s=pd.Series(pq_col(path, c), dtype="string")
                cnts, ex = scan_values(s)
                for ns,v in cnts.items(): v_counts[ns]=v_counts.get(ns,0)+v
                for ns,vals in ex.items():
                    v_examples.setdefault(ns,[])
                    for x in vals:
                        if len(v_examples[ns])<5 and x not in v_examples[ns]: v_examples[ns].append(x)
            ns_union=set(v_counts.keys())
            row["mode"]="wide_header" if len(wide)>=3 else ("value_scan" if v_counts else "none")
            row["id_columns"]=";".join([str(c) for c in pick])
            row["namespaces"]=";".join(sorted(ns_union))
            row["ns_counts"]=";".join(f"{k}={v_counts.get(k,0)}" for k in sorted(ns_union)) if v_counts else ""
            row["examples"]=";".join(f"{k}:"+"|".join(v_examples.get(k,[])) for k in sorted(v_examples)) if v_examples else ""
            return row

        return row
    except Exception as e:
        row["mode"]=f"error:{e.__class__.__name__}"
        return row

def atomic_write_csv(df, out_path):
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    fd,tmp=tempfile.mkstemp(prefix=".tmp_idinv_", suffix=".csv", dir=os.path.dirname(out_path) or ".")
    os.close(fd)
    try:
        df.to_csv(tmp, index=False)
        os.replace(tmp, out_path)
    except Exception:
        try: os.remove(tmp)
        except Exception: pass
        raise

def main():
    ap=argparse.ArgumentParser(description="Inventory IDs/accessions across datasets (GEO/SRA/ENA/EGA/ArrayExpress/PRIDE/TCGA/CPTAC/…).")
    ap.add_argument("--inputs", required=True, help="Dir/glob/;-list (e.g., \"data\\**\\*.csv;data\\**\\*.parquet\")")
    ap.add_argument("--workers", type=int, default=12, help="Parallel file readers")
    ap.add_argument("--out", default="data/02_interim/diagnostics/id_inventory.csv", help="CSV output path")
    args=ap.parse_args()

    files=iter_paths(args.inputs)
    if not files:
        print("[ERR ] No files found. Check --inputs."); sys.exit(2)

    rows=[]
    if RICH:
        with Progress(TextColumn("[bold]scan"), BarColumn(), TextColumn("{task.completed}/{task.total}"), TimeRemainingColumn(), console=console, transient=True) as prog:
            t=prog.add_task("inventory", total=len(files))
            w=max(1,args.workers); stride=max(1,len(files)//w)
            batches=[files[i:i+stride] for i in range(0,len(files),stride)]
            with cf.ThreadPoolExecutor(max_workers=w) as ex:
                results=ex.map(lambda b: [analyze_file(p) for p in b], batches)
                for b, rlist in zip(batches, results):
                    rows.extend(rlist)
                    prog.advance(t, advance=len(b))
    else:
        with cf.ThreadPoolExecutor(max_workers=max(1,args.workers)) as ex:
            for r in ex.map(analyze_file, files):
                rows.append(r)

    df=pd.DataFrame(rows, columns=["file","ext","size","mode","header_tags","id_columns","namespaces","ns_counts","examples"])
    atomic_write_csv(df, args.out)
    print(f"[DONE] ID inventory \u2192 {args.out}  files={len(df)}")
    ns_all=[]
    for x in df["namespaces"].dropna():
        ns_all.extend([t for t in x.split(";") if t])
    ns_counts=pd.Series(ns_all).value_counts().to_dict() if ns_all else {}
    print("[SUMMARY] files-with-ns:", json.dumps(ns_counts, separators=(",",":")) or "{}")

if __name__=="__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[WARN] Interrupted."); sys.exit(130)
'@ | Out-File -FilePath scripts\id_reader.py -Encoding utf8 -Force

