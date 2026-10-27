#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
: "${ARROWCOL_AZURE_TABLE_MANIFEST:?bootstrap must provide a private manifest output path}"
: "${ARROWCOL_AZURE_TABLE_ROOT:?bootstrap must provide the canonical ADLS root}"
: "${AZURE_STORAGE_ACCOUNT:?bootstrap must provide the owned storage account}"
: "${AZURE_STORAGE_CONTAINER:?bootstrap must provide the owned data container}"
: "${AZURE_STORAGE_CHECKPOINT_CONTAINER:?bootstrap must provide the reset authority container}"
exec cargo run --locked --quiet --package arrowcol-loadgen -- \
  initialize-azure --manifest "$ARROWCOL_AZURE_TABLE_MANIFEST"
