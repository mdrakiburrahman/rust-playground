#!/usr/bin/env bash
set -euo pipefail

controller_image=${1:?controller image is required}
candidate_image=${2:?candidate image is required}
suffix="${BASHPID:-$$}-${RANDOM}${RANDOM}"
repo_volume="darwin-bridge-repo-$suffix"
socket_volume="darwin-bridge-socket-$suffix"
worker_name="darwin-bridge-worker-$suffix"

cleanup() {
  local status=$?
  trap - EXIT
  if [[ "$status" -ne 0 ]] && docker container inspect "$worker_name" >/dev/null 2>&1; then
    docker logs "$worker_name" >&2
  fi
  docker rm -f "$worker_name" >/dev/null 2>&1 || true
  docker volume rm -f "$repo_volume" "$socket_volume" >/dev/null 2>&1 || true
  exit "$status"
}
trap cleanup EXIT

docker volume create "$repo_volume" >/dev/null
docker volume create "$socket_volume" >/dev/null

read -r base_commit level3_commit level7_commit protected_commit invalid_commit < <(
  docker run --rm --network none \
    --mount "type=volume,source=$repo_volume,target=/trusted" \
    --entrypoint /bin/bash "$controller_image" -euo pipefail -c '
      git -C /trusted init -q repo
      git -C /trusted/repo config user.name "Darwin bridge fixture"
      git -C /trusted/repo config user.email "darwin-bridge@example.invalid"
      mkdir -p /trusted/repo/fixtures
      printf "%s\n" "{\"compression_level\":1,\"other\":\"fixed\"}" > /trusted/repo/fixtures/compression.json
      printf "%s\n" "trusted fixture" > /trusted/repo/README.md
      git -C /trusted/repo add fixtures/compression.json README.md
      git -C /trusted/repo commit -qm "pinned producer"
      base=$(git -C /trusted/repo rev-parse HEAD)
      printf "%s\n" "{\"compression_level\":3,\"other\":\"fixed\"}" > /trusted/repo/fixtures/compression.json
      git -C /trusted/repo commit -qam "compression level 3"
      level3=$(git -C /trusted/repo rev-parse HEAD)
      git -C /trusted/repo checkout -q --detach "$base"
      printf "%s\n" "{\"compression_level\":7,\"other\":\"fixed\"}" > /trusted/repo/fixtures/compression.json
      git -C /trusted/repo commit -qam "compression level 7"
      level7=$(git -C /trusted/repo rev-parse HEAD)
      git -C /trusted/repo checkout -q --detach "$base"
      printf "%s\n" "{\"compression_level\":5,\"other\":\"fixed\"}" > /trusted/repo/fixtures/compression.json
      printf "%s\n" "unapproved" > /trusted/repo/README.md
      git -C /trusted/repo commit -qam "protected file change"
      protected=$(git -C /trusted/repo rev-parse HEAD)
      git -C /trusted/repo checkout -q --detach "$base"
      printf "%s\n" "{\"compression_level\":0,\"other\":\"fixed\"}" > /trusted/repo/fixtures/compression.json
      git -C /trusted/repo commit -qam "invalid compression level"
      invalid=$(git -C /trusted/repo rev-parse HEAD)
      printf "%s %s %s %s %s\n" "$base" "$level3" "$level7" "$protected" "$invalid"
    '
)

[[ "$base_commit" =~ ^[0-9a-f]{40}$ && "$level3_commit" =~ ^[0-9a-f]{40}$ \
  && "$level7_commit" =~ ^[0-9a-f]{40}$ && "$protected_commit" =~ ^[0-9a-f]{40}$ \
  && "$invalid_commit" =~ ^[0-9a-f]{40}$ ]]

docker run --detach --name "$worker_name" --network none --read-only \
  --cap-drop ALL --security-opt no-new-privileges --pids-limit 64 \
  --mount "type=volume,source=$repo_volume,target=/trusted,readonly" \
  --mount "type=volume,source=$socket_volume,target=/run/darwin-bridge" \
  --entrypoint node "$controller_image" --experimental-strip-types \
  /opt/darwin-runner/bridge-fixture-worker.ts \
  /run/darwin-bridge/dispatch.sock /trusted/repo "$base_commit" >/dev/null

ready=
for _ in $(seq 1 50); do
  if docker logs "$worker_name" 2>&1 | grep -q '^TRUSTED_LISTENER_READY$'; then
    ready=yes
    break
  fi
  if [[ "$(docker inspect --format '{{.State.Running}}' "$worker_name")" != true ]]; then
    docker logs "$worker_name" >&2
    exit 1
  fi
  sleep 0.1
done
if [[ "$ready" != yes ]]; then
  docker logs "$worker_name" >&2
  echo "trusted bridge listener did not become ready" >&2
  exit 1
fi

client_script='import { connect } from "node:net";
import { existsSync, writeFileSync, unlinkSync } from "node:fs";
const [commit, expected] = process.argv.slice(1);
if (!commit || !expected || process.env.COPILOT_GITHUB_TOKEN !== undefined
    || process.env.DARWIN_TEST_SENTINEL !== undefined
    || existsSync("/var/run/docker.sock") || existsSync("/var/run/docker-host.sock")
    || existsSync("/trusted/repo")) throw new Error("candidate boundary check failed");
try {
  writeFileSync("/run/darwin-bridge/agent-write-probe", "blocked");
  unlinkSync("/run/darwin-bridge/agent-write-probe");
  throw new Error("candidate can write the trusted socket volume");
} catch (error) {
  if (!error || typeof error !== "object" || !("code" in error)
      || (error.code !== "EROFS" && error.code !== "EACCES")) throw error;
}
const socket = connect("/run/darwin-bridge/dispatch.sock");
const chunks = [];
let length = 0;
socket.setTimeout(5000, () => socket.destroy(new Error("bridge response timed out")));
socket.on("connect", () => socket.end(JSON.stringify({version:1,lane:"compression-config",candidateCommit:commit})));
socket.on("data", chunk => { length += chunk.length; if (length > 4096) socket.destroy(new Error("oversized response")); else chunks.push(chunk); });
socket.on("error", error => { console.error(error.message); process.exitCode = 1; });
socket.on("end", () => {
  const response = Buffer.concat(chunks).toString("utf8");
  if (!response) throw new Error("trusted bridge closed without a response");
  const value = JSON.parse(response);
  if (expected === "reject") {
    if (value.ok !== false) throw new Error("unapproved request was accepted");
  } else if (value.ok !== true || value.result?.candidateCommit !== commit
      || value.result?.compressionLevel !== Number(expected)) {
    throw new Error("trusted bridge returned an unexpected result");
  }
  console.log(JSON.stringify(value));
});'

request() {
  docker run --rm --network none --read-only --tmpfs /tmp:rw,nosuid,nodev,size=16m \
    --cap-drop ALL --security-opt no-new-privileges --pids-limit 64 --user 10001:10001 \
    --mount "type=volume,source=$socket_volume,target=/run/darwin-bridge,readonly" \
    --entrypoint node "$candidate_image" --input-type=module -e "$client_script" "$1" "$2"
}

request "$level3_commit" 3
request "$level7_commit" 7
request "$protected_commit" reject
request "$invalid_commit" reject

callback_count=$(docker logs "$worker_name" 2>&1 | grep -c '^TRUSTED_CALLBACK ' || true)
[[ "$callback_count" -eq 2 ]] || {
  echo "expected exactly two validated dispatches, observed $callback_count" >&2
  docker logs "$worker_name" >&2
  exit 1
}
printf '%s\n' "Trusted Unix-socket bridge passed: levels 3 and 7 accepted; protected and invalid commits rejected before the trusted callback."
