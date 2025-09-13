Project Log & Context Checkpoint: COMPASS-BRCA Pipeline
Generated: 2025-09-11
Status: Mid-Execution of Core Data Processing Pipeline
1. Thesis Goal & Subject Matter
Primary Objective: To develop and apply machine learning models to a comprehensive dataset to understand the relationship between nanoparticle-based drug delivery systems and their effectiveness against different subtypes of breast cancer (e.g., ER+, HER2+, Triple-Negative).
Core Scientific Question: Can we find correlations and build predictive models that link specific, tunable nanoparticle properties (material, size, coating) and the drugs they carry to biological outcomes (gene expression, pathway activity, patient survival)?
2. Final Project Architecture
The project has been architected into a professional, self-aware, and reproducible pipeline.
Core Files:
run_pipeline.py: The master orchestrator that executes all pipeline steps.
pipeline_config.py: The central "source of truth" defining all directory and file paths.
setup.py & __init__.py files: Makes the project an "editable" package (pip install -e .), which is the definitive solution for all Python ModuleNotFoundError issues.
requirements.txt: Defines all dependencies for a reproducible environment.
Final Folder Structure:
00_manifests/: Blueprints for data acquisition (master_manifest_FINAL.csv).
01_data_raw/: Canonical location for all immutable raw data.
02_data_interim/: Cleaned, normalized .parquet files (~200k+ files).
02a_data_filtered/: High-signal, domain-specific data after noise reduction.
03_data_primary/: Key metadata (crosswalks, vocabularies, stratified file lists).
04_features_final/: Fully annotated, analysis-ready datasets.
05_features_balanced/: Datasets prepared for ML.
scripts/: All executable Python scripts.
logs/: Detailed logs from pipeline runs.
3. The Definitive 9-Step Data Processing Pipeline (run_pipeline.py)
This is the final, logical workflow.
step01_fetch_data: Intelligent Dispatcher. Reads a manifest and concurrently downloads web URLs (http/ftp) using a resumable, UI-rich downloader, while also handling local file copies (file:///).
step02_audit_raw_data: Fast summary of raw data.
step03_clean_and_normalize: High-performance "Auto-Medic" using Polars. Extremely fast and robustly fixes data errors like ragged rows and duplicate column headers.
step04_build_crosswalk: The Definitive "Dask Queued Submission" script (V18.0). This is the most critical and heavily debugged script, designed to handle the ~200k file challenge.
Architecture: Uses dask.distributed.Client with a "Queued Submission" (wait in a loop) strategy.
Resilience: Submits tasks in controlled batches, preventing the Dask scheduler from being overwhelmed and crashing. This is the final, stable solution for the memory, stall, and KilledWorker issues.
Performance: Fully concurrent, using a Dask cluster that automatically scales.
step05_build_vocabulary: Creates the master vocabulary.
step05a_filter_by_vocabulary: Crucial noise-reduction step. Creates a smaller, high-signal dataset in 02a_data_filtered/.
step06_annotate_data: Standardizes terms in the filtered dataset.
step07_analyze_missingness: Reports on NaN values.
step08_reconcile_manifests: Verifies manifest against disk.
step09_create_balanced_cohorts: Prepares data for ML.
4. Definitive Utility Scripts (The Project Toolkit)
These are the final versions of the tools used for setup and discovery.
Project Rescue & Preparation:
utility_sanitize_filenames.py: Fixes corrupted file/directory names.
utility_consolidate_data.py: Intelligently moves scattered legacy files into the canonical pipeline structure.
utility_stratify_by_size.py: A key performance tool. Pre-scans files and separates them into "small" and "large" lists, enabling the pipeline to use different strategies for each.
Intelligence Gathering:
utility_harvest_all_urls.py: Finds all URLs in the project.
utility_curate_harvested_urls.py: The "Relevance Engine" that intelligently scores and filters harvested URLs based on thesis-specific keywords.
utility_create_final_manifest.py: The "Expert System" that automates the final merging of all intelligence into master_manifest_FINAL.csv.
Advanced Discovery:
utility_resolve_dois_to_data.py: Translates a list of DOIs into downloadable data links.
analysis_thesis_readiness_report.py: The scientific gap analysis tool.
5. Log of Major Troubleshooting ("Dump Code")
The most significant challenge was processing the ~204,000 Parquet files in 02_data_interim for the step04_build_crosswalk. This required a multi-stage debugging process that evolved through several architectures:
Initial Stalls: Caused by inefficient file discovery (rglob) and simple memory bottlenecks.
Concurrency Bugs on Windows: ProcessPoolExecutor conflicted with the rich UI library (OSError: The handle is invalid). ThreadPoolExecutor was tried but exposed memory issues.
Dask Failures:
High-level dask.dataframe failed due to a KeyError from contaminated, mismatched schemas.
High-level dask.bag failed due to an unserializable task graph (TypeError: cannot pickle).
The Dask cluster itself became unstable and crashed (KilledWorker, TimeoutError) due to a small number of "decompression bomb" / "poison pill" files that exhausted worker memory.
Final, Successful Architecture (V18.0): The definitive solution is a "Queued Submission" strategy using dask.distributed.Client and dask.distributed.wait. This feeds tasks to the Dask scheduler in a controlled stream, preventing it from being overwhelmed, which was the final root cause of the crashes and stalls.
6. Current Status & Next Steps
Current Status: All forensics, cleanup, and consolidation of the initial messy project are complete. The final, comprehensive master_manifest_FINAL.csv has been created. The data processing pipeline has been initiated via run_pipeline.py.
Active Task: The pipeline is currently executing step04_build_crosswalk.py (V18.0), which is the long-running, primary processing step.
Immediate Goal: Allow the run_pipeline.py command to run to completion. It is now equipped with the final, robust versions of all scripts and should be able to finish the entire 9-step workflow.
Next Steps (After Pipeline Finishes):
Execute the Scientific Analysis Pipeline (Phase 3), starting with analysis_thesis_readiness_report.py.
Based on the report, begin "Cycle 2" of the research loop: use the advanced discovery tools to find new data that fills the identified scientific gaps, augment the manifest, and re-run the processing pipeline.