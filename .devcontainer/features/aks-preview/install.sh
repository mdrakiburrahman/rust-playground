#!/usr/bin/env bash
set -euo pipefail

az extension add --name aks-preview --version 22.0.0b15 --system --yes --only-show-errors
test "$(az extension show --name aks-preview --query version --output tsv)" = 22.0.0b15
