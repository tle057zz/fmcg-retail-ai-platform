#!/usr/bin/env bash
# Upload local notebooks/ to Databricks workspace folder fmcg-retail/notebooks
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
# shellcheck disable=SC1091
set -a
source "$ROOT/.env"
set +a

export PATH="${HOME}/bin:${PATH}"

WS="/Users/tle.profession.au@gmail.com/fmcg-retail"
NB="$ROOT/notebooks"

if ! command -v databricks >/dev/null 2>&1; then
  echo "databricks CLI not found. Expected at ~/bin/databricks"
  exit 1
fi

echo "Listing $WS ..."
databricks workspace list "$WS"

databricks workspace mkdirs "$WS/notebooks"

for f in "$NB"/*.py; do
  name="$(basename "$f" .py)"
  echo "Uploading $name ..."
  databricks workspace import "$WS/notebooks/$name" \
    --file "$f" \
    --language PYTHON \
    --format SOURCE \
    --overwrite
done

echo "Done. Workspace contents:"
databricks workspace list "$WS/notebooks"
