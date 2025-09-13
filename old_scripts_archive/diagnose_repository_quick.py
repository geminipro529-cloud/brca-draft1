#!/usr/bin/env python3
# scripts/diagnose_repository_quick.py
# Purpose: fast (≤5 min) repo scan → CSVs + recommendations + SVG treemap (stdlib only)
# Usage (PowerShell, from project root):
#   python scripts\diagnose_repository_quick.py --root data --out data\02_interim\diagnostics --time-budget-sec 300

import os, sys, csv, json, time, argparse, math, traceback
from collections import defaultdict, Counter
from typing import Dict, Any, List, Tuple

# --------- defaults ----------
DEFAULT_TIME_BUDGET = 300   # seconds (≤ 5 mins)
DEFAULT_MAX_FILES   = 200000
DEFAULT_SVG_W, DEFAULT_SVG_H = 2000, 1200
# ----------------------------

def eprint(s): print(s, file=sys.stderr, flush=True)

def ensure_dir(p: str):
    if p:
        os.makedirs(p, exist_ok=True)

def atomic_write_text(path: str, text: str, encoding="utf-8"):
    tmp = path + ".part"
    with open(tmp, "w", encoding=encoding, newline="") as f:
        f.write(text)
    if os.path.exists(path): os.replace(tmp, path)
    else: os.rename(tmp, path)

def atomic_write_csv(path: str, header: List[str], rows: List[List[Any]]):
    tmp = path + ".part"
    ensure_dir(os.path.dirname(path))
    with open(tmp, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)
    if os.path.exists(path): os.replace(tmp, path)
    else: os.rename(tmp, path)

def ext_of(name: str) -> str:
    base = name.lower()
    if base.endswith(".tar.gz"): return ".tar.gz"
    if base.endswith(".csv.gz"): return ".csv.gz"
    if base.endswith(".tsv.gz"): return ".tsv.gz"
    return os.path.splitext(base)[1] or "<noext>"

def human_bytes(n: int) -> str:
    if n < 1024: return f"{n} B"
    for u in ("KB","MB","GB","TB","PB"):
        n/=1024.0
        if n<1024: return f"{n:.1f} {u}"
    return f"{n:.1f} EB"

def walk_fast(root: str, time_budget: int, max_files: int) -> Tuple[List[Dict[str,Any]], Dict[str, Any]]:
    t0 = time.time()
    inv: List[Dict[str,Any]] = []
    stats = dict(files=0, dirs=0, bytes=0, truncated=False, elapsed=0.0)
    for dirpath, dirnames, filenames in os.walk(root):
        stats["dirs"] += 1
        # quick exit if time budget exceeded
        if time.time() - t0 > time_budget:
            stats["truncated"] = True
            break
        # iterate files
        for fn in filenames:
            full = os.path.join(dirpath, fn)
            try:
                st = os.stat(full, follow_symlinks=False)
                size = int(st.st_size)
            except Exception:
                size = -1
            rel = os.path.relpath(full, root)
            ext = ext_of(fn)
            inv.append(dict(rel=rel, dir=os.path.dirname(rel), name=fn, ext=ext, size=size))
            stats["files"] += 1
            if size > 0: stats["bytes"] += size
            if stats["files"] >= max_files or (time.time() - t0) > time_budget:
                stats["truncated"] = True
                break
        if stats["truncated"]:
            break
    stats["elapsed"] = round(time.time() - t0, 2)
    return inv, stats

def summarize(inv: List[Dict[str,Any]]) -> Dict[str, Any]:
    ext_counts = Counter()
    ext_bytes = Counter()
    dirs_bytes = Counter()
    largest = []
    for row in inv:
        ext = row["ext"]
        sz  = max(0, row["size"])
        ext_counts[ext] += 1
        ext_bytes[ext]  += sz
        dirs_bytes[row["dir"]] += sz
        if sz>0:
            largest.append((sz, row["rel"]))
    largest.sort(reverse=True)
    return dict(
        by_ext_counts=dict(ext_counts),
        by_ext_bytes=dict((k, int(v)) for k,v in ext_bytes.items()),
        by_dir_bytes=dict((k, int(v)) for k,v in dirs_bytes.items()),
        largest=largest[:200]  # top 200
    )

# -------- Simple slice-and-dice treemap to SVG (stdlib) ----------
def layout_treemap(items: List[Tuple[str,int]], x: float, y: float, w: float, h: float, horizontal: bool=True):
    # items = list of (label, value>0), returns list of (x,y,w,h,label,value)
    total = sum(v for _,v in items) or 1
    rects = []
    offset = 0.0
    for label, v in items:
        frac = float(v)/total
        if horizontal:
            ww = w*frac
            rects.append((x+offset, y, ww, h, label, v))
            offset += ww
        else:
            hh = h*frac
            rects.append((x, y+offset, w, hh, label, v))
            offset += hh
    return rects

def sanitize_label(s: str) -> str:
    # keep SVG clean
    return s.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

def svg_rect(x,y,w,h,fill,stroke,text=None,small=False):
    r = f'<rect x="{x:.2f}" y="{y:.2f}" width="{w:.2f}" height="{h:.2f}" fill="{fill}" stroke="{stroke}" stroke-width="0.8"/>'
    if text and w>40 and h>16:
        size = 10 if small else 12
        tx = x + 3
        ty = y + 12
        t = sanitize_label(text)
        r += f'<text x="{tx:.2f}" y="{ty:.2f}" font-size="{size}" fill="#111">{t}</text>'
    return r

def color_for(label: str) -> str:
    # deterministic pastel color from label
    h = (abs(hash(label)) % 360)
    return f"hsl({h},60%,80%)"

def make_svg_treemap(root: str, by_dir_bytes: Dict[str,int], by_ext_bytes: Dict[str,int], out_svg: str, W=DEFAULT_SVG_W, H=DEFAULT_SVG_H):
    # top: directories by size, bottom: file types by size
    # pick top-N to keep picture readable; aggregate rest as "other"
    def topn(d: Dict[str,int], n=20):
        items = sorted(d.items(), key=lambda kv: kv[1], reverse=True)
        if len(items) <= n: return items
        head = items[:n-1]
        tail_sum = sum(v for _,v in items[n-1:])
        head.append(("other", tail_sum))
        return head

    top_dirs = [(k or ".", v) for k,v in topn(by_dir_bytes, n=18) if v>0]
    top_exts = topn(by_ext_bytes, n=22)
    dir_total = sum(v for _,v in top_dirs) or 1
    ext_total = sum(v for _,v in top_exts) or 1

    # Split canvas horizontally: 60% dirs (top), 40% types (bottom)
    mid = int(H*0.58)
    svg = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}">']
    svg.append(f'<rect x="0" y="0" width="{W}" height="{H}" fill="#ffffff"/>')
    svg.append(f'<text x="12" y="24" font-size="16" fill="#000">Repository Map — {sanitize_label(root)}</text>')
    # Directories
    svg.append(f'<text x="12" y="44" font-size="13" fill="#333">Top directories by total bytes</text>')
    rects_dir = layout_treemap(top_dirs, 10, 60, W-20, mid-70, horizontal=False)
    for x,y,w,h,label,val in rects_dir:
        svg.append(svg_rect(x,y,w,h,color_for(label),"#999", f"{label} ({human_bytes(val)})", small=True))

    # File types
    svg.append(f'<text x="12" y="{mid+24}" font-size="13" fill="#333">File types by total bytes</text>')
    rects_ext = layout_treemap(top_exts, 10, mid+40, W-20, H-(mid+50), horizontal=True)
    for x,y,w,h,label,val in rects_ext:
        lab = f"{label} ({human_bytes(val)})"
        svg.append(svg_rect(x,y,w,h,color_for(label),"#999", lab, small=True))

    svg.append("</svg>")
    atomic_write_text(out_svg, "\n".join(svg))

# -------- Recommendations ----------
def recommend(stats: Dict[str,Any], summary: Dict[str,Any]) -> List[str]:
    rec = []
    by_ext_counts = summary["by_ext_counts"]
    by_ext_bytes  = summary["by_ext_bytes"]
    largest       = summary["largest"]

    # 1) Big CSV/TSV → Parquet
    big_plain = []
    for ext in (".csv",".tsv",".txt",".csv.gz",".tsv.gz"):
        big_plain.append(sum(v for k,v in by_ext_bytes.items() if k == ext))
    if sum(big_plain) > 500*1024*1024:
        rec.append("Convert large CSV/TSV to Parquet (use csv_to_parquet.py). Set --min-mb 700 in batch to target only huge files.")

    # 2) Zips in raw → keep zipped + record index
    if by_ext_counts.get(".zip",0)+by_ext_counts.get(".tar.gz",0) > 0:
        rec.append("Maintain an index of archives (.zip/.tar.gz) in data/01_raw/zipped/, keep raw zipped; extract only needed files.")

    # 3) Partial downloads
    if by_ext_counts.get(".part",0) > 0:
        rec.append("Found .part partials — re-run FTP downloader to resume and then clean leftovers.")

    # 4) Many small files
    small_files = [x for x in largest if x[0] < 64*1024]
    if len(small_files) > 2000:
        rec.append("Thousands of small files detected — consider compacting them (e.g., merge text logs) to speed future scans.")

    # 5) Parquet present? Encourage columnar-first
    if by_ext_counts.get(".parquet",0)==0 and (by_ext_counts.get(".csv",0)+by_ext_counts.get(".tsv",0))>0:
        rec.append("No Parquet found but many CSV/TSV — convert frequently-used tables to Parquet for faster IO.")

    # 6) Time budget truncated
    if stats.get("truncated"):
        rec.append("Scan truncated by time budget — re-run with --time-budget-sec increased or narrow --root.")

    # 7) Master build readiness
    if by_ext_counts.get(".parquet",0)>0:
        rec.append("You can proceed to build the master dataset (build_master_dataset.py).")

    # 8) Oversized images or videos
    media_bytes = 0
    for ext in (".mp4",".mov",".mkv",".avi",".tif",".tiff",".png",".jpg",".jpeg"):
        media_bytes += by_ext_bytes.get(ext,0)
    if media_bytes > 2*1024*1024*1024:
        rec.append("Heavy media detected — move/ignore in analytics to reduce repository bloat.")

    if not rec:
        rec.append("Repository looks fine. Proceed with pipeline; re-run diagnostics after conversions.")
    return rec

# -------- Main ----------
def main():
    ap = argparse.ArgumentParser(description="Quick repository diagnostics (≤5 min) with SVG map & next-step plan.")
    ap.add_argument("--root", default="data", help="Root directory to scan (default: data)")
    ap.add_argument("--out",  default=os.path.join("data","02_interim","diagnostics"), help="Output folder for reports")
    ap.add_argument("--time-budget-sec", type=int, default=DEFAULT_TIME_BUDGET, help="Max seconds to scan (default 300)")
    ap.add_argument("--max-files", type=int, default=DEFAULT_MAX_FILES, help="Max files to index (default 200k)")
    args = ap.parse_args()

    ensure_dir(args.out)
    t0 = time.time()
    try:
        inv, stats = walk_fast(args.root, args.time_budget_sec, args.max_files)
        summ = summarize(inv)

        # Write CSVs
        inventory_csv = os.path.join(args.out, "inventory.csv")
        rows = [[r["rel"], r["dir"], r["name"], r["ext"], r["size"]] for r in inv]
        atomic_write_csv(inventory_csv, ["rel_path","dir","name","ext","size_bytes"], rows)

        ext_counts_csv = os.path.join(args.out, "ext_counts.csv")
        atomic_write_csv(ext_counts_csv, ["ext","count"], [[k,v] for k,v in sorted(summ["by_ext_counts"].items(), key=lambda kv: (-kv[1], kv[0]))])

        ext_bytes_csv = os.path.join(args.out, "ext_bytes.csv")
        atomic_write_csv(ext_bytes_csv, ["ext","total_bytes"], [[k,v] for k,v in sorted(summ["by_ext_bytes"].items(), key=lambda kv: (-kv[1], kv[0]))])

        largest_csv = os.path.join(args.out, "largest_files.csv")
        atomic_write_csv(largest_csv, ["size_bytes","rel_path"], [[sz, rel] for sz, rel in summ["largest"]])

        # JSON (machine-readable tree summary)
        json_path = os.path.join(args.out, "summary.json")
        atomic_write_text(json_path, json.dumps(dict(stats=stats, summary=summ), ensure_ascii=False, indent=2))

        # SVG treemap
        svg_path = os.path.join(args.out, "repo_map.svg")
        make_svg_treemap(args.root, summ["by_dir_bytes"], summ["by_ext_bytes"], svg_path, W=DEFAULT_SVG_W, H=DEFAULT_SVG_H)

        # Markdown plan
        plan_md = os.path.join(args.out, "report.md")
        recs = recommend(stats, summ)
        md = []
        md.append(f"# Quick Repository Diagnostics\n")
        md.append(f"- Root: `{args.root}`\n- Elapsed: **{stats['elapsed']}s**  \n- Files: **{stats['files']}**  \n- Dirs: **{stats['dirs']}**  \n- Total Size: **{human_bytes(stats['bytes'])}**  \n- Truncated: **{stats['truncated']}**\n")
        md.append("## Next Actions (priority)\n")
        for i, r in enumerate(recs, 1):
            md.append(f"{i}. {r}")
        md.append("\n## Outputs\n")
        md.append(f"- `inventory.csv` — one row per file\n- `ext_counts.csv` — file count by extension\n- `ext_bytes.csv` — bytes by extension\n- `largest_files.csv` — top 200 largest\n- `summary.json` — machine-readable snapshot\n- `repo_map.svg` — directory/types treemap image\n- `report.md` — this plan\n")
        atomic_write_text(plan_md, "\n".join(md))

        print(f"[OK] Diagnostics written to: {args.out}")
        print(f"     - {os.path.relpath(svg_path)} (SVG map)")
        print(f"     - {os.path.relpath(plan_md)} (plan)")
    except Exception as e:
        print(f"[ERROR] {e}")
        traceback.print_exc()
        sys.exit(1)
    finally:
        print(f"[INFO] Total runtime: {round(time.time()-t0,2)}s")

if __name__ == "__main__":
    main()
