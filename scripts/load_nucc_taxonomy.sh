#!/usr/bin/env bash
# Convenience wrapper to load the latest NUCC taxonomy codes.
# Usage:
#   ./scripts/load_nucc_taxonomy.sh /data/nppes/nppes.db
#   NPPES_DB=/data/nppes/nppes.db ./scripts/load_nucc_taxonomy.sh

set -euo pipefail

DB_PATH="${1:-${NPPES_DB:-nppes.db}}"

echo "Loading NUCC taxonomy codes into $DB_PATH"
python -m nppes.taxonomy --db "$DB_PATH" --download

echo "Done. You can now JOIN taxonomies.code against taxonomy_codes.code"
