#!/usr/bin/env bash
set -euo pipefail

SOURCE_PAGE="https://www.backblaze.com/cloud-storage/resources/hard-drive-test-data"
DOWNLOAD_URL_YEAR1="${DOWNLOAD_URL_YEAR1:-}"
DOWNLOAD_URL_YEAR2="${DOWNLOAD_URL_YEAR2:-}"

mkdir -p data/raw

if [[ -z "$DOWNLOAD_URL_YEAR1" || -z "$DOWNLOAD_URL_YEAR2" ]]; then
  echo "Set DOWNLOAD_URL_YEAR1 and DOWNLOAD_URL_YEAR2 to the official Backblaze download URLs."
  echo "Official source: $SOURCE_PAGE"
  exit 1
fi

curl -fL "$DOWNLOAD_URL_YEAR1" -o data/raw/year1.zip
curl -fL "$DOWNLOAD_URL_YEAR2" -o data/raw/year2.zip
unzip -q data/raw/year1.zip -d data/raw/
unzip -q data/raw/year2.zip -d data/raw/
echo "Download and extraction complete."
