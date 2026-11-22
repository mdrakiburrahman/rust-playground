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
export DARWIN_HOME=/run/darwin-home/.darwin
git config --global --add safe.directory /workspace
server_log=/run/darwin-home/darwin-server.log
/usr/local/bin/darwin start --host 127.0.0.1 --port 0 >"$server_log" 2>&1 &
server_pid=$!
stop_server() {
  if kill -0 "$server_pid" 2>/dev/null; then
    /usr/local/bin/darwin shutdown --yes >/dev/null 2>&1 || true
    for _ in $(seq 1 50); do
      kill -0 "$server_pid" 2>/dev/null || break
      sleep 0.1
    done
    if kill -0 "$server_pid" 2>/dev/null; then
      kill -TERM "$server_pid" 2>/dev/null || true
    fi
  fi
  wait "$server_pid" 2>/dev/null || true
}
trap stop_server EXIT
ready=
for _ in $(seq 1 100); do
  if grep -q "http://127.0.0.1:" "$server_log"; then
    ready=yes
    break
  fi
  if ! kill -0 "$server_pid" 2>/dev/null; then
    cat "$server_log" >&2
    exit 1
  fi
  sleep 0.1
done
if [[ "$ready" != yes ]]; then
  echo "Darwin foreground daemon did not become ready" >&2
  cat "$server_log" >&2
  exit 1
fi
/usr/local/bin/darwin evolve "/workspace/$config" \
  --repo /workspace \
  --home "/workspace/.darwin/runner-$experiment-$run_id" \
  --only "$experiment" \
  --env-file "$token_file"
