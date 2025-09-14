# compass_brca/step05_build_vocabularies_stratified.py
#
# V2.1: Expert Curated & Stratified Vocabulary Builder
# Purpose: This version enhances the stratified builder by merging the terms
# discovered in the data with expert-curated lists of keywords. This ensures
# that key scientific concepts are included in the vocabulary even if they are
# rare in the dataset, amplifying the signal for downstream filtering.

import sys
import polars as pl
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

# --- Configuration ---
try:
    from src.compass_brca.pipeline_config import PRIMARY_DATA_DIR
except ImportError:
    # ... (error handling)
    sys.exit(1)

# --- Input/Output Paths ---
CROSSWALK_DATASET_DIR = PRIMARY_DATA_DIR / "master_crosswalk.dataset"
VOCABULARY_OUTPUT_DIR = PRIMARY_DATA_DIR / "vocabularies"

# --- V2.1: EXPERT CURATED SCIENTIFIC PARAMETERS ---

# Define categories and the expert terms for each.
# This allows the script to merge the right expert list with the right data type.
EXPERT_VOCABULARIES = {
    # --- Breast Cancer - Biological Principles ---
    "gene_symbol": [
        "BRCA1", "BRCA2", "TP53", "PIK3CA", "HER2", "ERBB2", "ESR1", "PGR",
        "EGFR", "FGFR", "MET", "MYC", "CCND1", "CDK4", "CDK6", "RB1",
        "GATA3", "MAP3K1", "AKT1", "PTEN", "KRAS", "HRAS", "NRAS", "PALB2",
        "ATM", "CHEK2", "MKI67", "AURKA", "BARD1", "RAD51", "CDH1",
    ],
    "pathway": [
        "PI3K-Akt signaling", "MAPK signaling", "Wnt signaling", "Cell Cycle",
        "DNA replication", "Homologous recombination", "p53 signaling",
        "Estrogen signaling", "HER2 signaling", "EGFR signaling",
        "VEGF signaling", "Apoptosis", "Metastasis", "Angiogenesis",
    ],
    "disease_term": [
        "breast cancer", "carcinoma", "triple-negative breast cancer", "TNBC",
        "HER2-positive", "ER-positive", "luminal A", "luminal B", "basal-like",
        "ductal carcinoma in situ", "DCIS", "invasive ductal carcinoma", "IDC",
        "metastatic breast cancer", "neoplasm", "tumor microenvironment", "TME",
    ],
    # --- Nanomedicine - Chemistry & Physical Principles ---
    "nanoparticle": [
        "nanoparticle", "liposome", "dendrimer", "micelle", "polymer-drug conjugate",
        "nanoshell", "nanocage", "quantum dot", "carbon nanotube", "mesoporous silica",
        "gold nanoparticle", "AuNP", "iron oxide nanoparticle", "SPION",
        "polymeric nanoparticle", "PLGA", "PEG", "chitosan", "stealth liposome",
        "theranostic", "drug delivery", "targeted delivery", "nanocarrier",
    ],
    "drug_name": [
        "doxorubicin", "paclitaxel", "docetaxel", "cisplatin", "carboplatin",
        "trastuzumab", "pertuzumab", "tamoxifen", "fulvestrant", "letrozole",
        "palbociclib", "ribociclib", "olaparib", "talazoparib", "pembrolizumab",
        "atezolizumab", "capecitabine", "gemcitabine", "doxil", "abraxane",
    ],
    # Enzymatic principles are often gene/protein names, covered above.
    # We can add a generic category for other principles.
    "biological_principle": [
        "enzyme kinetics", "receptor binding", "endocytosis", "phagocytosis",
        "enhanced permeability and retention", "EPR effect", "active targeting",
        "passive targeting", "drug release", "biodistribution", "pharmacokinetics",
        "opsonization", "biocompatibility", "cytotoxicity", "apoptosis induction",
    ]
}

# A list of common junk terms to exclude from all vocabularies.
EXCLUSION_LIST = ["na", "NA", "nan", "NaN", "null", "NULL", "", "None", "unknown"]

def main():
    """
    Main function to build expert-curated, stratified vocabularies.
    """
    console = Console()
    console.print(Panel("[bold cyan]Step 05 (V2.1): Build Expert Curated Vocabularies[/]", expand=False))

    if not CROSSWALK_DATASET_DIR.exists():
        console.print(f"[bold red]Error:[/bold red] Crosswalk dataset directory not found.")
        sys.exit(1)

    VOCABULARY_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    console.print(f"Output vocabularies will be saved in: [cyan]{VOCABULARY_OUTPUT_DIR}[/]")

    try:
        crosswalk_path_glob = str(CROSSWALK_DATASET_DIR / "*.parquet")
        crosswalk_lazy_df = pl.scan_parquet(crosswalk_path_glob)
        
        all_types = crosswalk_lazy_df.select("identifier_type").unique().collect()["identifier_type"].to_list()
        
        # We will process all types found in the data PLUS all types defined in our expert list
        types_to_process = sorted(list(set(all_types + list(EXPERT_VOCABULARIES.keys()))))
            
        console.print(f"Found {len(types_to_process)} total identifier types to process...")

        results_table = Table(title="Vocabulary Generation Summary", header_style="bold magenta")
        results_table.add_column("Identifier Type", style="cyan")
        results_table.add_column("Output File", style="green")
        results_table.add_column("Total Unique Terms", style="magenta", justify="right")

        for id_type in types_to_process:
            console.print(f"  - Processing [yellow]{id_type}[/]...")
            
            # Start with the expert-curated terms for this type, if they exist
            expert_terms = EXPERT_VOCABULARIES.get(id_type, [])
            
            # Extract terms from the crosswalk data for this type
            if id_type in all_types:
                data_terms_df = (
                    crosswalk_lazy_df
                    .filter(pl.col("identifier_type") == id_type)
                    .select("identifier_value")
                    .unique()
                    .collect()
                )
                data_terms = data_terms_df["identifier_value"].to_list()
            else:
                data_terms = []
            
            # --- V2.1 FIX: Merge expert terms and data-derived terms ---
            # Create a Polars Series, combine them, get unique values, and filter the exclusion list
            combined_series = pl.Series("term", expert_terms + data_terms)
            
            vocabulary_df = (
                pl.DataFrame(combined_series)
                .unique()
                .filter(~pl.col("term").is_in(EXCLUSION_LIST))
                .sort("term")
            )
            
            if not vocabulary_df.is_empty():
                output_filename = f"{id_type}_vocabulary.parquet"
                output_path = VOCABULARY_OUTPUT_DIR / output_filename
                vocabulary_df.write_parquet(output_path)
                
                results_table.add_row(
                    id_type,
                    output_filename,
                    f"{len(vocabulary_df):,}"
                )

        console.print(results_table)
        console.print(Panel("[bold green]Expert curated vocabulary build complete![/]", expand=False))

    except Exception as e:
        console.print(f"[bold red]An unexpected error occurred:[/bold red] {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()