#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<'EOF'
Usage:
  tools/darwin-runner/run.sh dry-run <tracked-config.yaml>
  tools/darwin-runner/run.sh evolve <tracked-config.yaml> <experiment>
EOF
  exit 2
}

action=${1:-}
[[ -n "$action" ]] || usage
shift

repo=$(git rev-parse --show-toplevel)
cd "$repo"
script_dir="$repo/tools/darwin-runner"
lock="$script_dir/tools.lock.json"
config=${1:-}
[[ -n "$config" && -f "$repo/$config" ]] || usage
config_rel=$(realpath --relative-to="$repo" "$repo/$config")
[[ "$config_rel" != ../* ]] || { echo "config must be inside the repository" >&2; exit 2; }
git -C "$repo" ls-files --error-unmatch "$config_rel" >/dev/null || {
  echo "config must be tracked before running Darwin" >&2
  exit 2
}

readarray -t pins < <(node -e '
  const p = require(process.argv[1]);
  console.log(p.debian_image);
  console.log(p.rust.version);
  console.log(p.rust.rustup_init_sha256);
  console.log(p.node.version);
  console.log(p.node.linux_x64_sha256);
  console.log(p.docker_cli.debian_package);
' "$lock")
base_image=${pins[0]}
rust_version=${pins[1]}
rustup_sha=${pins[2]}
node_version=${pins[3]}
node_sha=${pins[4]}
docker_cli_package=${pins[5]}

image_digest=$(sha256sum \
  "$lock" "$script_dir/Dockerfile" "$script_dir/install-tools.sh" "$script_dir/candidate-exec.sh" |
  sha256sum | cut -c1-16)
controller_image="rust-playground/darwin-controller:$image_digest"
candidate_image="rust-playground/darwin-candidate:$image_digest"

docker build --pull \
  --build-arg "DEBIAN_IMAGE=$base_image" \
  --build-arg "RUST_VERSION=$rust_version" \
  --build-arg "RUSTUP_INIT_SHA256=$rustup_sha" \
  --build-arg "NODE_VERSION=$node_version" \
  --build-arg "NODE_SHA256=$node_sha" \
  --build-arg "DOCKER_CLI_PACKAGE=$docker_cli_package" \
  --target candidate --tag "$candidate_image" "$script_dir"
docker build --pull \
  --build-arg "DEBIAN_IMAGE=$base_image" \
  --build-arg "RUST_VERSION=$rust_version" \
  --build-arg "RUSTUP_INIT_SHA256=$rustup_sha" \
  --build-arg "NODE_VERSION=$node_version" \
  --build-arg "NODE_SHA256=$node_sha" \
  --build-arg "DOCKER_CLI_PACKAGE=$docker_cli_package" \
  --target controller --tag "$controller_image" "$script_dir"

run_controller() {
  docker run --rm --init \
    --mount "type=bind,source=$repo,target=/workspace" \
    --mount type=bind,source=/var/run/docker.sock,target=/var/run/docker-host.sock \
    --tmpfs /run/darwin-home:rw,nosuid,nodev,noexec,size=512m \
    --workdir /workspace \
    --env "HOME=/run/darwin-home" \
    --env "DARWIN_CANDIDATE_IMAGE=$candidate_image" \
    --env DOCKER_HOST=unix:///var/run/docker-host.sock \
    "$@"
}

case "$action" in
  dry-run)
    [[ $# -eq 1 ]] || usage
    run_controller --entrypoint /bin/bash "$controller_image" -euo pipefail -c '
      export DARWIN_HOME=/run/darwin-home/.darwin
      darwin start --host 127.0.0.1 --port 0 --background
      trap "darwin shutdown --yes >/dev/null 2>&1 || true" EXIT
      darwin dry-run "/workspace/$1"
    ' runner "$config_rel"
    ;;
  evolve)
    [[ $# -eq 2 ]] || usage
    experiment=$2
    [[ "$experiment" =~ ^[A-Za-z0-9._-]+$ ]] || {
      echo "experiment must be a simple name" >&2
      exit 2
    }
    npx --no-install tsx "$script_dir/config-check.ts" "$repo/$config_rel"
    git check-ignore -q -- .env || {
      echo ".env must be ignored by the repository before inference" >&2
      exit 1
    }
    run_controller \
      --env COPILOT_GITHUB_TOKEN=darwin-runner-fake-token \
      --env DARWIN_TEST_SENTINEL=darwin-runner-fake-token \
      --entrypoint /usr/local/bin/darwin-candidate-exec \
      "$controller_image" --isolation-check

    token_file=$(mktemp)
    trap 'rm -f "$token_file"' EXIT
    chmod 600 "$token_file"
    awk '/^COPILOT_GITHUB_TOKEN=/ { print; count++ } END { if (count != 1) exit 1 }' \
      "$repo/.env" > "$token_file" || {
        echo "expected exactly one COPILOT_GITHUB_TOKEN in the gitignored .env" >&2
        exit 1
      }

    run_id="$(date -u +%Y%m%dT%H%M%SZ)-$$"
    run_controller \
      --env-file "$token_file" \
      --entrypoint /usr/local/bin/darwin \
      "$controller_image" evolve "/workspace/$config_rel" \
      --repo /workspace \
      --home "/workspace/.darwin/runner-$experiment-$run_id" \
      --only "$experiment"
    ;;
  *)
    usage
    ;;
esac
