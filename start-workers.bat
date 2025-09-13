@echo off
echo Starting 4 Dask Workers (2.5GB each)...
call .\.venv\Scripts\activate.bat
dask-worker tcp://127.0.0.1:8786 --nworkers 4 --memory-limit 2.5GB