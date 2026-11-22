#!/usr/bin/env bash
set -euo pipefail

echo "native Darwin evolution is blocked: passthrough does not isolate the evaluator from controller privileges or credentials; no credential or inference was used" >&2
exit 2
