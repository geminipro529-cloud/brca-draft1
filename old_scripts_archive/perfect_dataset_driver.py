#!/usr/bin/env python3
# scripts/perfect_dataset_driver.py
# Orchestrates: annotate → coverage check → unstructured extract → ID crosswalk → master build
# Usage (PowerShell from project root):
#   python scripts\perfect_dataset_driver.py --dry-run  # preview
#   python scripts\perfect_dataset_driver.py             # do it

import os, sys, argparse, time, subprocess, textwrap, shutil, csv, json
from typing import List, Tuple, Dict, Optional

# ======== DEFAULT PATHS (edit if your layout differs) ========
ROOT                = os.getcwd()
SCRIPTS_DIR         = os.path.join(ROOT, "scripts")

# Inputs
STRUCTURED_UNION    = os.path.join("data", "02_interim", "structured_union")
VOCAB_PARQUET       = os.path.join("data", "02_interim", "VOCAB_COMPILED", "vocabulary_enriched.parquet")
UNSTRUCTURED_IN     = os.path.join("data", "01_raw", "UNSTRUCTURED")
OLD_ANNOTATED_DIR   = os.path.join("data", "02_interim", "structured_annotated")  # if present

# Optional quantitative inputs (set folder even if empty; step will warn if missing)
QUANT_DIR           = os.path.join("data", "02_interim", "quantitative")

# Outputs
ANNOTATED_V2_DIR    = os.path.join("data", "02_interim", "structured_annotated_v2")
UNSTRUCTURED_OUT    = os.path.join("data", "02_interim", "unstructured_lowmem")
ID_MAP_DIR          = os.path.join("data", "02_interim", "id_map")
MASTER_OUT_DIR      = os.path.join("data", "03_processed", "master_dataset")
RUN_LOG_DIR         = os.path.join("data", "02_interim", "run_logs")
REPORT_MD           = os.path.join(RUN_LOG_DIR, "perfect_run_report.md")
REPORT_JSON         = os.path.join(RUN_LOG_DIR, "perfect_run_report.json")

# Scripts expected (placeholders allowed; driver will verify existence)
ANNOTATE_SCRIPT     = os.path.join(SCRIPTS_DIR, "annotate_structured_with_vocab.py")
UNSTRUCTURED_SCRIPT = os.path.join(SCRIPTS_DIR, "extract_unstructured_lowmem.py")
CROSSWALK_SCRIPT    = os.path.join(SCRIPTS_DIR, "build_id_crosswalk.py")
MASTER_SCRIPT       = os.path.join(SCRIPTS_DIR, "build_master_dataset.py")
# =============================================================

PY_MIN = (3, 9)  # Python 3.9+ for comfort on Windows

def eprint(msg:str): 
    print(msg, file=sys.stderr, flush=True)

def ensure_dir(p:str):
    if p: os.makedirs(p, exist_ok=True)

def atomic_write_text(path:str, text:str, enc="utf-8"):
    tmp = path + ".part"
    ensure_dir(os.path.dirname(path))
    with open(tmp, "w", encoding=enc, newline="") as f: f.write(text)
    if os.path.exists(path): os.replace(tmp, path)
    else: os.rename(tmp, path)

def tiny_csv_preview(dir_path:str, max_rows:int=3) -> List[str]:
    """Return up to a few .csv/.tsv file names inside a folder (best-effort)."""
    out = []
    if not os.path.isdir(dir_path): return out
    for root, _, files in os.walk(dir_path):
        for fn in files:
            low = fn.lower()
            if low.endswith(".csv") or low.endswith(".tsv") or low.endswith(".parquet"):
                out.append(os.path.join(root, fn))
                if len(out) >= max_rows: return out
    return out

def has_any_files(path:str) -> bool:
    if not os.path.isdir(path): return False
    for _,_,files in os.walk(path):
        if files: return True
    return False

def exe_py(args:List[str], timeout:int, cwd:Optional[str]=None, env:Optional[Dict[str,str]]=None) -> Tuple[int,str,str,float]:
    """Run a Python subcommand with timeout, capture stdout/stderr, return (rc, out, err, dt)."""
    t0 = time.time()
    try:
        proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=cwd, env=env)
        out, err = proc.communicate(timeout=timeout)
        dt = time.time() - t0
        return proc.returncode, out.decode("utf-8", errors="replace"), err.decode("utf-8", errors="replace"), dt
    except subprocess.TimeoutExpired:
        try:
            proc.kill()
        except Exception:
            pass
        dt = time.time() - t0
        return 124, "", f"TIMEOUT after {timeout}s", dt
    except Exception as e:
        dt = time.time() - t0
        return 1, "", f"{type(e).__name__}: {e}", dt

def path_ok_or(msg:str, ok:bool) -> str:
    return f"[OK] {msg}" if ok else f"[WARN] {msg}"

def detect_breast_only_ready() -> bool:
    """Cheap readiness heuristic: vocab exists, structured_union has files."""
    return os.path.exists(VOCAB_PARQUET) and has_any_files(STRUCTURED_UNION)

def build_steps(args) -> List[Dict[str, any]]:
    py = sys.executable

    steps = []

    # 1) Annotate structured with vocab
    steps.append(dict(
        name="annotate_structured",
        script=ANNOTATE_SCRIPT,
        required_inputs=[STRUCTURED_UNION, VOCAB_PARQUET],
        outputs=[ANNOTATED_V2_DIR],
        cmd=[py, ANNOTATE_SCRIPT,
             "--in", STRUCTURED_UNION,
             "--vocab", VOCAB_PARQUET,
             "--out", ANNOTATED_V2_DIR],
        timeout=args.annotate_timeout
    ))

    # 2) Extract unstructured/semi-structured with vocab (optional if UNSTRUCTURED missing)
    if os.path.isdir(UNSTRUCTURED_IN) and has_any_files(UNSTRUCTURED_IN):
        steps.append(dict(
            name="extract_unstructured",
            script=UNSTRUCTURED_SCRIPT,
            required_inputs=[UNSTRUCTURED_IN, VOCAB_PARQUET],
            outputs=[UNSTRUCTURED_OUT],
            cmd=[py, UNSTRUCTURED_SCRIPT,
                 "--in", UNSTRUCTURED_IN,
                 "--vocab", VOCAB_PARQUET,
                 "--out", UNSTRUCTURED_OUT,
                 "--batch-sents", "300",
                 "--max-chars", "800000"],
            timeout=args.unstructured_timeout
        ))

    # 3) Build ID crosswalk (runs only if old annotated exists)
    if os.path.isdir(OLD_ANNOTATED_DIR) and has_any_files(OLD_ANNOTATED_DIR):
        steps.append(dict(
            name="id_crosswalk",
            script=CROSSWALK_SCRIPT,
            required_inputs=[ANNOTATED_V2_DIR, OLD_ANNOTATED_DIR],
            outputs=[ID_MAP_DIR],
            cmd=[py, CROSSWALK_SCRIPT,
                 "--in-v2", ANNOTATED_V2_DIR,
                 "--in-old", OLD_ANNOTATED_DIR,
                 "--out", ID_MAP_DIR,
                 "--prefer", "sample"],
            timeout=args.crosswalk_timeout
        ))

    # 4) Build master dataset (breast-only switch)
    steps.append(dict(
        name="build_master",
        script=MASTER_SCRIPT,
        required_inputs=[ANNOTATED_V2_DIR],
        optional_inputs=[QUANT_DIR, UNSTRUCTURED_OUT, ID_MAP_DIR],
        outputs=[MASTER_OUT_DIR],
        cmd=[py, MASTER_SCRIPT,
             "--structured", ANNOTATED_V2_DIR,
             "--quant", QUANT_DIR,
             "--qual", UNSTRUCTURED_OUT,
             "--out", MASTER_OUT_DIR,
             "--breast-only"],
        timeout=args.master_timeout
    ))

    return steps

def check_script_exists(p:str) -> bool:
    return os.path.isfile(p)

def summarize_state() -> Dict[str, any]:
    state = dict()
    state["python"] = sys.version
    state["paths"] = dict(
        structured_union = STRUCTURED_UNION,
        vocab_parquet    = VOCAB_PARQUET,
        unstructured_in  = UNSTRUCTURED_IN,
        annotated_v2_dir = ANNOTATED_V2_DIR,
        unstructured_out = UNSTRUCTURED_OUT,
        id_map_dir       = ID_MAP_DIR,
        master_out_dir   = MASTER_OUT_DIR,
    )
    state["previews"] = dict(
        structured_preview=tiny_csv_preview(STRUCTURED_UNION),
        old_annotated_preview=tiny_csv_preview(OLD_ANNOTATED_DIR),
        unstructured_preview=tiny_csv_preview(UNSTRUCTURED_IN),
    )
    return state

def run_steps(steps:List[Dict[str,any]], dry_run:bool, retries:int) -> List[Dict[str,any]]:
    results = []
    for step in steps:
        name = step["name"]
        script = step["script"]
        req_inputs:List[str] = step.get("required_inputs", [])
        opt_inputs:List[str] = step.get("optional_inputs", [])
        outputs:List[str] = step.get("outputs", [])
        cmd = step["cmd"]
        timeout = step["timeout"]

        # Ensure output dirs
        for out_dir in outputs:
            os.makedirs(out_dir, exist_ok=True)

        # Validate script exists
        if not check_script_exists(script):
            results.append(dict(name=name, status="skip_no_script", rc=0, dt=0, out="", err=f"Missing script: {script}", cmd=" ".join(cmd)))
            continue

        # Validate required inputs
        missing = []
        for p in req_inputs:
            if p.endswith(".parquet") or p.endswith(".csv") or p.endswith(".tsv"):
                if not os.path.exists(p): missing.append(p)
            else:
                if not (os.path.isdir(p) and has_any_files(p)): missing.append(p)
        if missing:
            results.append(dict(name=name, status="skip_missing_input", rc=0, dt=0, out="", err="Missing/empty: " + "; ".join(missing), cmd=" ".join(cmd)))
            continue

        # If dry-run, don't execute
        if dry_run:
            results.append(dict(name=name, status="dry_run", rc=0, dt=0, out="", err="", cmd=" ".join(cmd)))
            continue

        # Execute with retries
        attempt = 0
        rc = 1
        out = err = ""
        dt = 0.0
        while attempt <= retries:
            attempt += 1
            rc, out, err, dt = exe_py(cmd, timeout=timeout)
            if rc == 0:
                results.append(dict(name=name, status="ok", rc=rc, dt=dt, out=out[-8000:], err=err[-8000:], cmd=" ".join(cmd)))
                break
            # SyntaxError / ModuleNotFound usually fatal; don't retry more than once
            if "SyntaxError" in err or "ModuleNotFoundError" in err:
                results.append(dict(name=name, status="fatal", rc=rc, dt=dt, out=out[-8000:], err=err[-8000:], cmd=" ".join(cmd)))
                break
            if attempt <= retries:
                time.sleep(min(5*attempt, 20))  # backoff
        else:
            results.append(dict(name=name, status="failed", rc=rc, dt=dt, out=out[-8000:], err=err[-8000:], cmd=" ".join(cmd)))
    return results

def make_plan(results:List[Dict[str,any]], state:Dict[str,any]) -> List[str]:
    rec = []
    # Critical blockers
    for r in results:
        if r["status"] in ("skip_missing_input", "skip_no_script", "fatal", "failed"):
            if r["name"] == "annotate_structured":
                rec.append("❗ Fix annotation first: ensure vocabulary and structured_union exist and that annotate_structured_with_vocab.py runs without SyntaxError.")
            if r["name"] == "extract_unstructured":
                rec.append("ℹ️ Unstructured step skipped/failed—OK for now. You can run it later to enrich text mentions.")
            if r["name"] == "id_crosswalk":
                rec.append("ℹ️ Crosswalk missing old annotations—optional. Create/restore data\\02_interim\\structured_annotated if you need diffs.")
            if r["name"] == "build_master":
                rec.append("❗ Master build failed or skipped—check previous steps, then re-run this driver.")
    # Readiness hints
    if os.path.isdir(ANNOTATED_V2_DIR) and has_any_files(ANNOTATED_V2_DIR):
        rec.append("✅ Structured annotation produced files—open the coverage/summary the script generated and check for unlabeled columns (0% hits).")
    if os.path.isdir(MASTER_OUT_DIR) and has_any_files(MASTER_OUT_DIR):
        rec.append("✅ Master dataset exists—inspect master_dataset.parquet and report.md; verify there are no UNK/None labels.")
    # General hygiene
    rec.append("🔁 Re-run this driver after updating vocabulary until coverage is near 100% on critical columns.")
    rec.append("🧪 Add tiny regression samples (5–10 rows) per table and assert their normalized labels to catch future drift.")
    return rec

def write_report(results:List[Dict[str,any]], state:Dict[str,any]):
    # JSON (machine-readable)
    payload = dict(state=state, results=results, ts=time.strftime("%Y-%m-%d %H:%M:%S"))
    atomic_write_text(REPORT_JSON, json.dumps(payload, ensure_ascii=False, indent=2))

    # Markdown (human-readable)
    lines = []
    lines.append("# Perfection Run Report (non-nanomedicine)\n")
    lines.append(f"- Time: **{time.strftime('%Y-%m-%d %H:%M:%S')}**")
    lines.append(f"- Python: `{sys.version.split()[0]}`")
    lines.append("\n## Paths")
    for k,v in state["paths"].items():
        lines.append(f"- **{k}**: `{v}`")
    lines.append("\n## Step Results")
    for r in results:
        lines.append(f"### {r['name']}")
        lines.append(f"- status: **{r['status']}**  | rc: `{r['rc']}`  | time: `{round(r['dt'],2)}s`")
        lines.append(f"- cmd: `{r['cmd']}`")
        if r["err"]:
            lines.append("<details><summary>stderr (tail)</summary>\n\n```\n" + r["err"][-2000:] + "\n```\n</details>")
        if r["out"]:
            lines.append("<details><summary>stdout (tail)</summary>\n\n```\n" + r["out"][-2000:] + "\n```\n</details>")
    # Plan
    plan = make_plan(results, state)
    lines.append("\n## Next Actions")
    for i, p in enumerate(plan, 1):
        lines.append(f"{i}. {p}")
    atomic_write_text(REPORT_MD, "\n".join(lines))

def main():
    ap = argparse.ArgumentParser(description="Perfect dataset driver (annotate → extract → crosswalk → master).")
    ap.add_argument("--dry-run", action="store_true", help="Show what would run; do not execute steps")
    ap.add_argument("--retries", type=int, default=1, help="Retries per step on non-fatal errors (default 1)")
    ap.add_argument("--annotate-timeout", type=int, dest="annotate_timeout", default=1800, help="Annotate step timeout (s)")
    ap.add_argument("--unstructured-timeout", type=int, dest="unstructured_timeout", default=1800, help="Unstructured step timeout (s)")
    ap.add_argument("--crosswalk-timeout", type=int, dest="crosswalk_timeout", default=600, help="Crosswalk step timeout (s)")
    ap.add_argument("--master-timeout", type=int, dest="master_timeout", default=1800, help="Master build timeout (s)")
    args = ap.parse_args()

    # Environment checks
    if sys.version_info < PY_MIN:
        eprint(f"[ERROR] Python >= {PY_MIN[0]}.{PY_MIN[1]} required. Found {sys.version.split()[0]}."); sys.exit(2)

    ensure_dir(RUN_LOG_DIR)

    # Early precondition sanity
    msgs = []
    msgs.append(path_ok_or(f"vocab parquet exists: {VOCAB_PARQUET}", os.path.exists(VOCAB_PARQUET)))
    msgs.append(path_ok_or(f"structured_union has files: {STRUCTURED_UNION}", has_any_files(STRUCTURED_UNION)))
    msgs.append(path_ok_or(f"unstructured optional: {UNSTRUCTURED_IN}", os.path.isdir(UNSTRUCTURED_IN)))
    msgs.append(path_ok_or(f"quantitative optional: {QUANT_DIR}", os.path.isdir(QUANT_DIR)))
    for m in msgs: eprint(m)

    if not detect_breast_only_ready():
        eprint("[ERROR] Required inputs missing (vocab and structured_union). Fix before running (or use --dry-run).")
        if not args.dry_run:
            sys.exit(3)

    # Build step plan
    steps = build_steps(args)

    # Execute
    state = summarize_state()
    results = run_steps(steps, dry_run=args.dry_run, retries=args.retries)
    write_report(results, state)

    # Final console hints
    print(f"[OK] Perfection driver {'(dry-run) ' if args.dry_run else ''}finished.")
    print(f"     Report: {REPORT_MD}")
    print(f"     JSON  : {REPORT_JSON}")

if __name__ == "__main__":
    main()
