#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<'EOF'
Usage:
  tools/darwin-runner/run.sh prepare [run-id]
  tools/darwin-runner/run.sh dry-run <tracked-config.yaml>
  tools/darwin-runner/run.sh evolve <tracked-config.yaml> <experiment> <run-id>
  tools/darwin-runner/run.sh cleanup <run-id>
EOF
  exit 2
}

valid_run_id() {
  [[ "$1" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{7,79}$ ]]
}

action=${1:-}
[[ -n "$action" ]] || usage
shift
repo=$(git rev-parse --show-toplevel)
cd "$repo"
script_dir="$repo/tools/darwin-runner"
lock="$script_dir/tools.lock.json"

volume_for() {
  printf 'rust-playground-darwin-key-%s' "$1"
}

cleanup_run() {
  local run_id=$1
  local envelope="$repo/.darwin-runner-$run_id.env.age"
  local volume
  volume=$(volume_for "$run_id")
  rm -f -- "$envelope"
  docker volume rm "$volume"
}

if [[ "$action" == cleanup ]]; then
  [[ $# -eq 1 ]] || usage
  valid_run_id "$1" || { echo "invalid run id" >&2; exit 2; }
  cleanup_run "$1"
  exit 0
fi

readarray -t pins < <(node -e '
  const p = require(process.argv[1]);
  console.log(p.debian_image);
  console.log(p.debian_snapshot);
  console.log(p.rust.version);
  console.log(p.rust.rustup_init_sha256);
  console.log(p.node.version);
  console.log(p.node.linux_x64_sha256);
  console.log(p.docker_cli.debian_package);
  console.log(p.age.debian_package);
' "$lock")
base_image=${pins[0]}
debian_snapshot=${pins[1]}
rust_version=${pins[2]}
rustup_sha=${pins[3]}
node_version=${pins[4]}
node_sha=${pins[5]}
docker_cli_package=${pins[6]}
age_package=${pins[7]}

image_digest=$(cd "$script_dir" &&
  sha256sum tools.lock.json Dockerfile install-tools.sh candidate-exec.sh run-darwin.sh |
  sha256sum | cut -c1-16)
controller_image="rust-playground/darwin-controller:$image_digest"
candidate_image="rust-playground/darwin-candidate:$image_digest"

build_images() {
  local build_args=(
    --build-arg "DEBIAN_IMAGE=$base_image"
    --build-arg "DEBIAN_SNAPSHOT=$debian_snapshot"
    --build-arg "RUST_VERSION=$rust_version"
    --build-arg "RUSTUP_INIT_SHA256=$rustup_sha"
    --build-arg "NODE_VERSION=$node_version"
    --build-arg "NODE_SHA256=$node_sha"
    --build-arg "DOCKER_CLI_PACKAGE=$docker_cli_package"
    --build-arg "AGE_PACKAGE=$age_package"
  )
  docker build --pull "${build_args[@]}" --target candidate --tag "$candidate_image" "$script_dir"
  docker build --pull "${build_args[@]}" --target controller --tag "$controller_image" "$script_dir"
}

run_controller_base() {
  docker run --rm --init \
    --mount type=bind,source=/var/run/docker.sock,target=/var/run/docker-host.sock \
    --tmpfs /run/darwin-home:rw,nosuid,nodev,noexec,size=512m \
    --env HOME=/run/darwin-home \
    --env "DARWIN_CANDIDATE_IMAGE=$candidate_image" \
    --env DOCKER_HOST=unix:///var/run/docker-host.sock \
    "$@"
}

run_controller() {
  run_controller_base \
    --mount "type=bind,source=$repo,target=/workspace" \
    --workdir /workspace "$@"
}

check_config() {
  local config=$1
  [[ -f "$repo/$config" ]] || { echo "config not found in repository" >&2; exit 2; }
  local relative
  relative=$(realpath --relative-to="$repo" "$repo/$config")
  [[ "$relative" != ../* ]] || { echo "config must be inside the repository" >&2; exit 2; }
  git ls-files --error-unmatch "$relative" >/dev/null || {
    echo "config must be tracked before running Darwin" >&2
    exit 2
  }
  npx --no-install tsx "$script_dir/config-check.ts" "$repo/$relative" >&2
  printf '%s' "$relative"
}

case "$action" in
  prepare)
    [[ $# -le 1 ]] || usage
    run_id=${1:-"$(date -u +%Y%m%dT%H%M%SZ)-$$-$RANDOM"}
    valid_run_id "$run_id" || { echo "invalid run id" >&2; exit 2; }
    build_images
    volume=$(volume_for "$run_id")
    if docker volume inspect "$volume" >/dev/null 2>&1; then
      echo "run id already has a key volume" >&2
      exit 1
    fi
    docker volume create "$volume" >/dev/null
    trap 'docker volume rm "$volume"' EXIT
    docker run --rm \
      --mount "type=volume,source=$volume,target=/run/age" \
      --entrypoint /bin/bash "$controller_image" -euo pipefail -c '
        test ! -e /run/age/identity
        umask 077
        age-keygen -o /run/age/identity
        chmod 600 /run/age/identity
      '
    recipient=$(docker run --rm \
      --mount "type=volume,source=$volume,target=/run/age,readonly" \
      --entrypoint age-keygen "$controller_image" -y /run/age/identity)
    fingerprint=$(printf '%s' "$recipient" | sha256sum | cut -d' ' -f1)
    printf 'run_id=%s\nrecipient=%s\nrecipient_sha256=%s\n' \
      "$run_id" "$recipient" "$fingerprint"
    trap - EXIT
    ;;
  version)
    [[ $# -eq 0 ]] || usage
    build_images
    docker run --rm --entrypoint /usr/local/bin/darwin "$controller_image" --version
    ;;
  dry-run)
    [[ $# -eq 1 ]] || usage
    config_rel=$(check_config "$1")
    build_images
    run_controller --entrypoint /bin/bash "$controller_image" -euo pipefail -c '
      export DARWIN_HOME=/run/darwin-home/.darwin
      git config --global --add safe.directory /workspace
      server_log=/run/darwin-home/darwin-server.log
      darwin start --host 127.0.0.1 --port 0 >"$server_log" 2>&1 &
      server_pid=$!
      stop_server() {
        if kill -0 "$server_pid" 2>/dev/null; then
          darwin shutdown --yes >/dev/null 2>&1 || true
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
      darwin dry-run "/workspace/$1"
    ' runner "$config_rel"
    ;;
  evolve)
    [[ $# -eq 3 ]] || usage
    config_rel=$(check_config "$1")
    experiment=$2
    run_id=$3
    [[ "$experiment" =~ ^[A-Za-z0-9._-]+$ ]] || {
      echo "experiment must be a simple name" >&2
      exit 2
    }
    valid_run_id "$run_id" || { echo "invalid run id" >&2; exit 2; }
    envelope="$repo/.darwin-runner-$run_id.env.age"
    volume=$(volume_for "$run_id")
    [[ -f "$envelope" && ! -L "$envelope" ]] || {
      echo "run-scoped encrypted token must be a regular file" >&2
      exit 1
    }
    docker volume inspect "$volume" >/dev/null

    run_controller_base \
      --env COPILOT_GITHUB_TOKEN=darwin-runner-fake-token \
      --env DARWIN_TEST_SENTINEL=darwin-runner-fake-token \
      --entrypoint /usr/local/bin/darwin-candidate-exec \
      "$controller_image" --isolation-check

    trap 'rm -f -- "$envelope"; docker volume rm "$volume"' EXIT
    run_controller \
      --mount "type=volume,source=$volume,target=/run/age,readonly" \
      --env "DARWIN_TOKEN_ENVELOPE=/workspace/$(basename "$envelope")" \
      --entrypoint /usr/local/bin/darwin-runner-evolve \
      "$controller_image" "$config_rel" "$experiment" "$run_id"
    ;;
  *)
    usage
    ;;
esac
