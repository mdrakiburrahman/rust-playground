#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s infra/azure -p 'test_*.py'
# Compilation requires no Azure sign-in and performs no deployment.
az bicep build --file infra/azure/main.bicep --stdout >/dev/null
