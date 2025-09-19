# scripts/ml_train_first_model.py
# V2 - Training on Rich Genomic Features
import sys
import pandas as pd
from pathlib import Path

# --- SIMPLE PATHING ---
try:
    project_root = Path(__file__).resolve().parents[1]
    sys.path.append(str(project_root / "src"))
    from compass_brca.pipeline_config import OUTPUT_FUSED_TABLE
except ImportError as e:
    sys.exit(f"Fatal Error: Could not import config. Details: {e}")
# --- END SIMPLE PATHING ---

import re
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score
import lightgbm as lgb

try:
    project_root = Path(__file__).resolve().parents[1]
    sys.path.append(str(project_root / "src"))
    from compass_brca.pipeline_config import OUTPUT_FUSED_TABLE
except ImportError as e:
    sys.exit(f"Fatal Error: Could not import config. Details: {e}")

from rich.console import Console
from rich.panel import Panel
console = Console()

TARGET_DRUG_ID = 1003 # Paclitaxel
TEST_SIZE = 0.2
RANDOM_STATE = 42

def main():
    console.rule(f"[bold magenta]ML: Train V2 Model (Rich Features) on Drug ID: {TARGET_DRUG_ID}[/]")
    if not OUTPUT_FUSED_TABLE.exists():
        console.print(f"[bold red]Error: Fused feature table not found![/]"); sys.exit(1)
    df = pd.read_parquet(OUTPUT_FUSED_TABLE)
    
    console.print("\n[yellow]Step 1: Preparing Data (Feature Engineering)...[/]")
    df_drug = df[df['drug_id'] == TARGET_DRUG_ID].copy()
    df_drug.dropna(subset=['outcome_viability_intensity'], inplace=True)
    if len(df_drug) < 100:
        console.print(f"[bold red]Not enough data for Drug ID {TARGET_DRUG_ID}.[/]"); sys.exit(1)
    
    # --- FEATURE SELECTION (V2) ---
    # Our features (X) are now the cell's properties INCLUDING gene mutations
    categorical_features = ['subtype', 'lineage']
    mutation_features = [col for col in df_drug.columns if '_mutation' in col]
    
    features = df_drug[categorical_features + mutation_features]
    target = df_drug['outcome_viability_intensity']

    features_encoded = pd.get_dummies(features, columns=categorical_features, dummy_na=False)
    sanitized_columns = [re.sub(r'[^A-Za-z0-9_]+', '_', col) for col in features_encoded.columns]
    features_encoded.columns = sanitized_columns
    console.print(f"Engineered and sanitized [bold cyan]{features_encoded.shape[1]}[/] numerical features.")

    console.print("\n[yellow]Step 2: Splitting data...[/]")
    X_train, X_test, y_train, y_test = train_test_split(features_encoded, target, test_size=TEST_SIZE, random_state=RANDOM_STATE)

    console.print("\n[yellow]Step 3: Training the LightGBM model...[/]")
    model = lgb.LGBMRegressor(random_state=RANDOM_STATE)
    model.fit(X_train, y_train)

    console.print("\n[yellow]Step 4: Evaluating model performance...[/]")
    predictions = model.predict(X_test)
    mse = mean_squared_error(y_test, predictions)
    r2 = r2_score(y_test, predictions)

    console.print(Panel(
        f"Mean Squared Error (MSE): [bold cyan]{mse:,.2f}[/]\n"
        f"R-squared (R²): [bold cyan]{r2:.4f}[/]\n\n"
        f"[bold]Previous Model R² (V1): 0.0460[/]",
        title="[bold]V2 Model (Rich Features): Evaluation Report[/]",
        expand=False
    ))
    if r2 > 0.05:
        console.print("\n[bold green]SIGNIFICANT IMPROVEMENT DETECTED![/]")
    else:
        console.print("\n[yellow]NOTE: Performance is similar to the baseline.[/]")

if __name__ == "__main__":
    main()