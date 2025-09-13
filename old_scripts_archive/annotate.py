# 1) Quarantine the Polars version so it won’t collide
New-Item -ItemType Directory -Force scripts\experimental | Out-Null
Move-Item -Force scripts\annotate_structured_with_vocab_polars.py scripts\experimental\annotate_structured_with_vocab_polars.py 2>$null

# 2) Create a tiny wrapper so you can pick the engine per run (defaults to pandas v1)
@'
import argparse, os, subprocess, sys

def main():
    ap = argparse.ArgumentParser(description="Wrapper for structured annotation")
    ap.add_argument("--engine", choices=["pandas","polars"], default="pandas",
                    help="pandas = Version 1 (resumable, low-RAM). polars = experimental (needs extra deps).")
    # pass-through args
    ap.add_argument("--in", dest="in_dir", required=True)
    ap.add_argument("--vocab", required=True)
    ap.add_argument("--out", dest="out_dir", required=True)
    ap.add_argument("--columns", nargs="*")
    ap.add_argument("--col-like")
    ap.add_argument("--chunksize", type=int, default=50000)
    ap.add_argument("--checkpoint")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--no-color", action="store_true")
    ap.add_argument("--progress-interval", type=float, default=0.6)
    ap.add_argument("--text-cols", nargs="+")  # for polars mode only
    args = ap.parse_args()

    root = os.path.dirname(__file__)
    if args.engine == "pandas":
        script = os.path.join(root, "annotate_structured_with_vocab.py")
        if not os.path.exists(script):
            print("ERROR: pandas annotator missing at scripts/annotate_structured_with_vocab.py", file=sys.stderr)
            sys.exit(2)
        cmd = [sys.executable, script,
               "--in", args.in_dir, "--vocab", args.vocab, "--out", args.out_dir,
               "--chunksize", str(args.chunksize)]
        if args.columns: cmd += ["--columns", *args.columns]
        if args.col_like: cmd += ["--col-like", args.col_like]
        if args.checkpoint: cmd += ["--checkpoint", args.checkpoint]
        if args.dry_run: cmd += ["--dry-run"]
        if args.no_color: cmd += ["--no-color"]
        cmd += ["--progress-interval", str(args.progress_interval)]
    else:
        script = os.path.join(root, "experimental", "annotate_structured_with_vocab_polars.py")
        if not os.path.exists(script):
            print("ERROR: polars annotator missing at scripts/experimental/annotate_structured_with_vocab_polars.py", file=sys.stderr)
            sys.exit(2)
        # polars script uses --text-cols instead of --columns/--col-like and has no checkpoint
        cmd = [sys.executable, script,
               "--in", args.in_dir, "--vocab", args.vocab, "--out", args.out_dir]
        if args.text_cols: cmd += ["--text-cols", *args.text_cols]

    print(">>", " ".join(cmd))
    sys.exit(subprocess.call(cmd))

if __name__ == "__main__":
    main()
'@ | Set-Content -Encoding UTF8 scripts\annotate.py
