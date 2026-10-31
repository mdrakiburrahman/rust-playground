#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
npx --no-install tsc --project infra/azure/tsconfig.json
npx --no-install tsx --test infra/azure/*.test.ts
# Compilation requires no Azure sign-in and performs no deployment.
az bicep build --file infra/azure/main.bicep --stdout >/dev/null
