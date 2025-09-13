# File: scripts/io_readers.py
# Purpose: One place to make CSV/TSV/GZ reads safe & uniform across ALL scripts.

import os, csv
import pandas as pd

# Optional helpers if you want explicit calls later
def _sep_for(path: str):
    return "\t" if str(path).lower().endswith(".tsv") else None

def read_table(path: str, usecols=None):
    try:
        return pd.read_csv(path, dtype=str, keep_default_na=False, na_filter=False,
                           engine="python", sep=_sep_for(path), compression="infer",
                           quoting=csv.QUOTE_MINIMAL, on_bad_lines="skip", low_memory=False,
                           encoding="utf-8", usecols=usecols)
    except UnicodeDecodeError:
        return pd.read_csv(path, dtype=str, keep_default_na=False, na_filter=False,
                           engine="python", sep=_sep_for(path), compression="infer",
                           quoting=csv.QUOTE_MINIMAL, on_bad_lines="skip", low_memory=False,
                           encoding="latin1", usecols=usecols)

def iter_table(path: str, chunksize: int = 200_000, usecols=None):
    it = read_table(path, usecols=usecols) if chunksize is None else \
         _read_csv_chunks(path, chunksize, usecols)
    if hasattr(it, "__iter__") and not isinstance(it, pd.DataFrame):
        for chunk in it: yield chunk
    else:
        yield it

def _read_csv_chunks(path: str, chunksize: int, usecols=None):
    try:
        return pd.read_csv(path, chunksize=chunksize, dtype=str, keep_default_na=False, na_filter=False,
                           engine="python", sep=_sep_for(path), compression="infer",
                           quoting=csv.QUOTE_MINIMAL, on_bad_lines="skip", low_memory=False,
                           encoding="utf-8", usecols=usecols)
    except UnicodeDecodeError:
        return pd.read_csv(path, chunksize=chunksize, dtype=str, keep_default_na=False, na_filter=False,
                           engine="python", sep=_sep_for(path), compression="infer",
                           quoting=csv.QUOTE_MINIMAL, on_bad_lines="skip", low_memory=False,
                           encoding="latin1", usecols=usecols)

# ---- ONE-LINE FIX FOR ALL EXISTING CODE: monkey-patch pandas.read_csv ----
def patch_pandas_read_csv():
    """Override pandas.read_csv to safe defaults; keeps all existing call sites working."""
    _orig = pd.read_csv
    def _patched(path, *args, **kwargs):
        # defaults only if caller didn’t set them
        kwargs.setdefault("dtype", str)
        kwargs.setdefault("keep_default_na", False)
        kwargs.setdefault("na_filter", False)
        kwargs.setdefault("engine", "python")
        kwargs.setdefault("compression", "infer")
        kwargs.setdefault("on_bad_lines", "skip")
        kwargs.setdefault("low_memory", False)
        kwargs.setdefault("quoting", csv.QUOTE_MINIMAL)
        if "sep" not in kwargs or kwargs["sep"] is None:
            kwargs["sep"] = _sep_for(path)
        try:
            return _orig(path, *args, encoding="utf-8", **kwargs)
        except UnicodeDecodeError:
            return _orig(path, *args, encoding="latin1", **kwargs)
    pd.read_csv = _patched
