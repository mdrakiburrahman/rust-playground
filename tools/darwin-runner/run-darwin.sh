#!/usr/bin/env bash
set -euo pipefail

config=${1:?config path is required}
experiment=${2:?experiment name is required}
run_id=${3:?run id is required}
envelope=${DARWIN_TOKEN_ENVELOPE:?DARWIN_TOKEN_ENVELOPE is required}
token_file=/run/darwin-home/token.env

age --decrypt --identity /run/age/identity "$envelope" > "$token_file"
chmod 600 "$token_file"
awk -F= '
  $1 == "COPILOT_GITHUB_TOKEN" && length(substr($0, index($0, "=") + 1)) > 0 { count++; next }
  $0 == "" { next }
  { invalid = 1 }
  END { exit !(count == 1 && !invalid) }
' "$token_file" || {
  echo "encrypted input must contain exactly one non-empty COPILOT_GITHUB_TOKEN entry" >&2
  exit 1
}
rm -f -- "$envelope"
exec /usr/local/bin/darwin evolve "/workspace/$config" \
  --repo /workspace \
  --home "/workspace/.darwin/runner-$experiment-$run_id" \
  --only "$experiment" \
  --env-file "$token_file"
