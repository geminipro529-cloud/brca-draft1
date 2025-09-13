#!/bin/bash
# A script to download the specified GEO datasets and platforms in parallel.

URLS=(
    "ftp://ftp.ncbi.nlm.nih.gov/geo/series/GSE106nnn/GSE106685/"
    "ftp://ftp.ncbi.nlm.nih.gov/geo/platforms/GPLnnn/GPL570/"
    "ftp://ftp.ncbi.nlm.nih.gov/geo/platforms/GPL6nnn/GPL6480/"
)

echo "Starting parallel download of GEO files..."
echo "----------------------------------------"

# Loop through each URL and start the download in the background.
for URL in "${URLS[@]}"; do
    echo "Starting download from: $URL"
    # The -b (background) and -q (quiet) flags are useful here.
    # Output will be logged to a file named wget-log by default.
    # The ampersand '&' at the end runs the command in the background.
    wget -b -q -r -np -nH --cut-dirs=3 -R "index.html*" "$URL" &
done

# The 'wait' command pauses the script until all background jobs started in this shell have finished.
echo "Waiting for all downloads to complete..."
wait

echo "----------------------------------------"
echo "All downloads are complete."