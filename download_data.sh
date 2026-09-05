#!/usr/bin/env bash

set -euo pipefail

BASE_URL="https://f001.backblazeb2.com/file/Backblaze-Hard-Drive-Data"
DATA_DIR="data/raw"

mkdir -p "$DATA_DIR"

for YEAR in 2024 2025
do
    for QUARTER in Q1 Q2 Q3 Q4
    do
        FILE="data_${QUARTER}_${YEAR}.zip"
        URL="${BASE_URL}/${FILE}"

        echo "Downloading ${FILE}..."
        curl -fL --retry 3 "$URL" -o "${DATA_DIR}/${FILE}"

        echo "Extracting ${FILE}..."
        unzip -q -o "${DATA_DIR}/${FILE}" -d "${DATA_DIR}/${FILE%.zip}"

        rm "${DATA_DIR}/${FILE}"
    done
done

echo "Backblaze 2024 and 2025 data download and extraction completed."
