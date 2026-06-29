#!/usr/bin/env bash
# Example weekly update script for cron / systemd timer.
# Put this somewhere in PATH or call directly from crontab.
#
# 0 3 * * * /home/you/nppes-sqlite/scripts/weekly_update_example.sh >> /var/log/nppes-weekly.log 2>&1

set -euo pipefail

DB_PATH="${NPPES_DB:-/data/nppes/nppes.db}"
DATA_DIR="${NPPES_DATA_DIR:-/tmp/nppes-weekly}"
mkdir -p "$DATA_DIR"

# Calculate last 7-day window (CMS publishes e.g. 060126_060726)
# Adjust dates as needed for your timezone / schedule.
START=$(date -d '7 days ago' +%m%d%y)
END=$(date +%m%d%y)
ZIP_NAME="NPPES_Data_Dissemination_${START}_${END}_Weekly_V2.zip"
URL="https://download.cms.gov/nppes/${ZIP_NAME}"

echo "[$(date)] Fetching $URL"
curl -fsSL -o "${DATA_DIR}/weekly.zip" "$URL" || {
    echo "No weekly file for ${START}-${END} yet (or network error)"
    exit 0
}

cd "$DATA_DIR"
unzip -o weekly.zip -d .

MAIN=$(ls npidata_pfile_*.csv | head -1)
PL=$(ls pl_pfile_*.csv | head -1)
EP=$(ls endpoint_pfile_*.csv | head -1)
ON=$(ls othername_pfile_*.csv | head -1)

echo "[$(date)] Loading into $DB_PATH"
python -m nppes.loader \
    --db "$DB_PATH" \
    --main "$MAIN" \
    --pl "$PL" \
    --endpoint "$EP" \
    --othername "$ON" \
    --delta \
    --batch 20000

echo "[$(date)] Weekly load complete. Cleaning up."
rm -rf "$DATA_DIR"

# Optional: report size
du -sh "$DB_PATH" || true
