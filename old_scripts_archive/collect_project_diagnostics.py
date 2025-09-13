#!/usr/bin/env python3
import os, sys, json, zipfile, subprocess
from pathlib import Path
root = Path(".").resolve()
out = root / "_samples"
out.mkdir(exist_ok=True)
bundle = root / f"_diagnostics_bundle.zip"
def write_txt(name, text): (out/name).write_text(text, encoding="utf-8")
def run(cmd): return subprocess.run(cmd, capture_output=True, text=True, shell=True)
# 1) counts & largest
counts = run(r'powershell -NoProfile "Get-ChildItem -Recurse data\01_raw | Group-Object {$_.Extension.ToLower()} | Sort Count -Desc | Format-Table -HideTableHeaders | Out-String"').stdout
largest = run(r'powershell -NoProfile "Get-ChildItem -Recurse data\01_raw | Sort Length -Desc | Select -First 20 FullName,Length | ConvertTo-Csv -NoTypeInformation | Out-String"').stdout
write_txt("ext_counts.txt", counts.strip())
write_txt("largest_20.csv", largest.strip())
# 2) sample CSV/TSV head
import itertools
for ext in ("*.csv","*.tsv"):
    p = next(itertools.chain(*(Path("data/01_raw/STRUCTURED").rglob(ext) for _ in [0])), None)
    if p:
        lines = "\n".join(Path(p).read_text(encoding="utf-8", errors="ignore").splitlines()[:6])
        write_txt("sample_structured_head.csv", lines); break
# 3) env
env = run("python --version").stdout + "\n" + run("pip show pandas pyarrow openpyxl tqdm").stdout
write_txt("env.txt", env.strip())
# 4) scripts
keep = ["extract_unstructured_lowmem.py","build_vocab_from_sources.py","annotate_structured_v2.py","link_patients_across_cohorts.py"]
for s in keep:
    p = Path("scripts")/s
    if p.exists(): (out/s).write_text(p.read_text(encoding="utf-8", errors="ignore"), encoding="utf-8")
# zip
with zipfile.ZipFile(bundle, "w", zipfile.ZIP_DEFLATED) as z:
    for p in out.rglob("*"):
        z.write(p, p.relative_to(root))
print(f"[OUT] {bundle}")
