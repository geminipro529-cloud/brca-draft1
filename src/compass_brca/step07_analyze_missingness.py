# scripts/step07_analyze_missingness.py
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import compass_brca.utils.pipeline_config as cfg
from rich.progress import tracks

def main():
    feature_files = list(cfg.FINAL_FEATURES_DIR.glob("*.parquet"))
    if not feature_files:
        print("No final feature files found to analyze for missingness.")
        cfg.CONSOLIDATED_MISSINGNESS_REPORT.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(columns=['variable', 'missing_percentage', 'source_file']).to_csv(cfg.CONSOLIDATED_MISSINGNESS_REPORT, index=False)
        return

    output_dir = cfg.REPORTS_DIR / "missingness_plots"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    all_stats = []
    for file in track(feature_files, description="[cyan]Analyzing Missingness...[/cyan]"):
        try:
            df = pd.read_parquet(file)
            if df.empty: continue

            missing_perc = (df.isnull().sum() / len(df)) * 100
            stats_df = pd.DataFrame({'variable': df.columns, 'missing_percentage': missing_perc})
            stats_df['source_file'] = file.name
            all_stats.append(stats_df)

            if df.isnull().sum().sum() > 0:
                plt.figure(figsize=(20, 10))
                sns.heatmap(df.isnull(), cbar=False, yticklabels=False, cmap='viridis')
                plt.title(f'Missing Data Pattern: {file.name}')
                plt.savefig(output_dir / f"{file.stem}_missingness_heatmap.png")
                plt.close()
        except Exception as e:
            print(f"\n[yellow]Warning:[/yellow] Could not process {file.name} for missingness: {e}")

    if not all_stats:
        print("No data processed for missingness report.")
        pd.DataFrame(columns=['variable', 'missing_percentage', 'source_file']).to_csv(cfg.CONSOLIDATED_MISSINGNESS_REPORT, index=False)
        return

    consolidated_df = pd.concat(all_stats).sort_values(by='missing_percentage', ascending=False)
    consolidated_df.to_csv(cfg.CONSOLIDATED_MISSINGNESS_REPORT, index=False)
    print(f"Missingness analysis complete. Report saved to {cfg.CONSOLIDATED_MISSINGNESS_REPORT}")

if __name__ == "__main__":
    main()