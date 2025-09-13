
"""
Assemble a unified knowledge graph from structured + caselist + literature.

Inputs (pass what you have; missing ones are optional):
  --master            master_structured.parquet (or master_structured_merged.parquet)
  --caselist-index    CASELIST_INDEX.parquet
  --entities          entities.parquet (from unstructured extractor)
  --edges             edges.parquet   (from collapse_relations_to_edges.py)
  --docs              doc_index.csv   (from unstructured extractor)
  --doc-map           doc_to_sample_map.csv (manual/auto curated mapping)
  --vocab             vocabulary_enriched.parquet (to tag node types if needed)

Outputs (under --out):
  nodes.csv, edges.csv   (Neo4j-friendly; id,label,type,… and src,dst,type,weight,…)
  master_kg.graphml      (if networkx is installed; else nodes/edges only)
  kg_report.md           (counts by node/edge type)
"""

from __future__ import annotations
import argparse, os
import polars as pl

try:
    import networkx as nx
    _HAS_NX = True
except Exception:
    _HAS_NX = False

def maybe_read(p: str) -> pl.DataFrame:
    if not p or not os.path.exists(p): return pl.DataFrame({"_":[]})
    if p.lower().endswith(".parquet"): return pl.read_parquet(p)
    return pl.read_csv(p)

def make_id(prefix: str, key: str) -> str:
    return f"{prefix}:{key}"

def norm_str(s) -> str:
    return "" if s is None else str(s)

def write_neo4j(nodes: pl.DataFrame, edges: pl.DataFrame, out_dir: str):
    nodes.write_csv(os.path.join(out_dir, "nodes.csv"))
    edges.write_csv(os.path.join(out_dir, "edges.csv"))

def to_graphml(nodes: pl.DataFrame, edges: pl.DataFrame, out_path: str):
    if not _HAS_NX:
        return
    G = nx.MultiDiGraph()
    for r in nodes.iter_rows(named=True):
        G.add_node(r["id"], **{k:v for k,v in r.items() if k!="id"})
    for r in edges.iter_rows(named=True):
        G.add_edge(r["src"], r["dst"], key=f"{r.get('type','rel')}", **{k:v for k,v in r.items() if k not in ("src","dst")})
    nx.write_graphml(G, out_path)

def main():
    ap = argparse.ArgumentParser(description="Build master knowledge graph.")
    ap.add_argument("--master", default="", help="master_structured*.parquet")
    ap.add_argument("--caselist-index", default="", help="CASELIST_INDEX.parquet")
    ap.add_argument("--entities", default="", help="entities.parquet")
    ap.add_argument("--edges", default="", help="edges.parquet")
    ap.add_argument("--docs", default="", help="doc_index.csv")
    ap.add_argument("--doc-map", default="", help="doc_to_sample_map.csv (doc_relpath,sample_id[,score])")
    ap.add_argument("--vocab", default="", help="vocabulary_enriched.parquet")
    ap.add_argument("--out", required=True, help="output folder")
    args = ap.parse_args()

    os.makedirs(args.out, exist_ok=True)

    V = maybe_read(args.vocab)
    vocab_types = {}
    if not V.is_empty():
        term = "canonical" if "canonical" in V.columns else "term"
        for t, tt in zip(V[term].to_list(), (V["term_type"].to_list() if "term_type" in V.columns else ["unknown"]*V.height)):
            if t: vocab_types[str(t).lower()] = str(tt or "unknown").lower()

    # ---- Nodes ----
    nodes = []

    # Samples from master
    M = maybe_read(args.master)
    if not M.is_empty():
        id_col = "sample_id" if "sample_id" in M.columns else M.columns[0]
        for sid in M[id_col].drop_nulls().cast(pl.Utf8).unique().to_list():
            nodes.append({"id": make_id("Sample", sid), "label": sid, "type": "Sample"})

    # CaseList nodes
    C = maybe_read(args.caselist_index)
    if not C.is_empty():
        for cid, cname in zip(C["case_list_id"].drop_nulls().unique().to_list(),
                              (C["case_list_name"].drop_nulls().unique().to_list() or [])):
            nodes.append({"id": make_id("CaseList", str(cid)), "label": str(cid), "type": "CaseList", "name": str(cname) if cname else str(cid)})

    # Document nodes
    D = maybe_read(args.docs)
    if not D.is_empty():
        for d in D["doc_relpath"].drop_nulls().unique().to_list():
            nodes.append({"id": make_id("Document", d), "label": d, "type": "Document"})

    # Entity nodes from edges/entities (canonical/normalized)
    E = maybe_read(args.edges)
    ents = set()
    if not E.is_empty():
        for s in E["subject"].drop_nulls().unique().to_list():
            ents.add(str(s))
        for o in E["object"].drop_nulls().unique().to_list():
            ents.add(str(o))

    ENT = maybe_read(args.entities)
    if not ENT.is_empty():
        for en in ENT["ent_norm"].drop_nulls().unique().to_list():
            ents.add(str(en))

    for e in sorted(ents):
        tt = vocab_types.get(e.lower(), "unknown")
        nodes.append({"id": make_id("Entity", e), "label": e, "type": tt if tt else "Entity"})

    nodes_df = pl.DataFrame(nodes).unique(subset=["id"])

    # ---- Edges ----
    edges = []

    # Sample --IN_LIST--> CaseList
    if not C.is_empty():
        for sid, cid in zip(C["sample_id"].drop_nulls().to_list(), C["case_list_id"].drop_nulls().to_list()):
            edges.append({
                "src": make_id("Sample", str(sid)),
                "dst": make_id("CaseList", str(cid)),
                "type": "IN_LIST",
                "weight": 1.0
            })

    # Document --MENTIONS--> Entity
    if not ENT.is_empty():
        for d, e in zip(ENT["doc_relpath"].to_list(), ENT["ent_norm"].to_list()):
            if d and e:
                edges.append({
                    "src": make_id("Document", str(d)),
                    "dst": make_id("Entity", str(e)),
                    "type": "MENTIONS",
                    "weight": 1.0
                })

    # Entity --PREDICATE--> Entity (weighted)
    if not E.is_empty():
        for s,p,o,w in zip(E["subject"].to_list(),
                           (E["predicate"].to_list() if "predicate" in E.columns else ["rel"]*E.height),
                           E["object"].to_list(),
                           (E["weight"].to_list() if "weight" in E.columns else [1.0]*E.height)):
            if s and o:
                edges.append({
                    "src": make_id("Entity", str(s)),
                    "dst": make_id("Entity", str(o)),
                    "type": str(p),
                    "weight": float(w) if w is not None else 1.0
                })

    # Sample --EVIDENCE--> Document (if map provided)
    MAP = maybe_read(args.doc_map)
    if not MAP.is_empty():
        sc_present = "score" in MAP.columns
        for d, s, sc in zip(MAP["doc_relpath"].to_list(), MAP["sample_id"].to_list(),
                            (MAP["score"].to_list() if sc_present else [1.0]*MAP.height)):
            if d and s:
                edges.append({
                    "src": make_id("Sample", str(s)),
                    "dst": make_id("Document", str(d)),
                    "type": "EVIDENCE",
                    "weight": float(sc) if sc is not None else 1.0
                })

    edges_df = pl.DataFrame(edges)
    # keep only edges whose nodes exist
    valid_ids = set(nodes_df["id"].to_list())
    if not edges_df.is_empty():
        edges_df = edges_df.filter(pl.col("src").is_in(list(valid_ids)) & pl.col("dst").is_in(list(valid_ids)))

    # ---- Write outputs ----
    write_neo4j(nodes_df, edges_df, args.out)
    if _HAS_NX:
        to_graphml(nodes_df, edges_df, os.path.join(args.out, "master_kg.graphml"))

    # small report
    def count_by(df: pl.DataFrame, col: str):
        return df.group_by(col).len().rename({"len":"n"}).sort("n", descending=True)

    with open(os.path.join(args.out, "kg_report.md"), "w", encoding="utf-8") as f:
        f.write("# Master KG report\n\n")
        f.write(f"- Nodes: **{nodes_df.height}**\n")
        if nodes_df.height:
            f.write("## Nodes by type\n")
            f.write(count_by(nodes_df, "type").to_pandas().to_markdown(index=False))
            f.write("\n\n")
        f.write(f"- Edges: **{edges_df.height}**\n")
        if edges_df.height:
            f.write("## Edges by type\n")
            f.write(count_by(edges_df, "type").to_pandas().to_markdown(index=False))
            f.write("\n")

    print(f"[OK] Wrote nodes/edges to {args.out}")

if __name__ == "__main__":
    main()
