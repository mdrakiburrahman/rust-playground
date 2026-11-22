#!/usr/bin/env bash
set -euo pipefail

mode=${1:-}
if [[ "$mode" == --isolation-check ]]; then
  sentinel=darwin-runner-fake-token
  [[ "${COPILOT_GITHUB_TOKEN:-}" == "$sentinel" ]]
  [[ "${DARWIN_TEST_SENTINEL:-}" == "$sentinel" ]]
  unset COPILOT_GITHUB_TOKEN DARWIN_TEST_SENTINEL

  repo=$(mktemp -d)
  trap 'rm -rf "$repo"' EXIT
  git -C "$repo" init -q
  git -C "$repo" config user.name "Darwin isolation probe"
  git -C "$repo" config user.email "darwin-isolation-probe@example.invalid"
  printf 'tracked input\n' > "$repo/input.txt"
  printf 'DARWIN_TEST_SENTINEL=%s\n' "$sentinel" > "$repo/.env"
  git -C "$repo" add input.txt .env
  git -C "$repo" commit -qm "isolation probe"
  (
    cd "$repo"
    /usr/local/bin/darwin-candidate-exec --assert-isolation
  )
  printf '%s\n' 'Isolation proof passed: fake token env, tracked .env, home, and controller socket are absent from candidate.'
  exit 0
fi

assertion=false
if [[ "$mode" == --assert-isolation ]]; then
  assertion=true
  shift
fi

command_text=${1:-}
if [[ "$assertion" != true && -z "$command_text" ]]; then
  echo "usage: darwin-candidate-exec '<candidate command>'" >&2
  exit 2
fi

repo=$(git rev-parse --show-toplevel)
image=${DARWIN_CANDIDATE_IMAGE:?DARWIN_CANDIDATE_IMAGE is required}
docker_host=${DOCKER_HOST:-unix:///var/run/docker-host.sock}

set +e
git -C "$repo" archive --format=tar HEAD |
  env -i PATH=/usr/bin:/bin DOCKER_HOST="$docker_host" docker run --rm --interactive \
    --network bridge \
    --cap-drop ALL \
    --security-opt no-new-privileges \
    --pids-limit 2048 \
    --user 10001:10001 \
    --workdir /workspace \
    --env HOME=/tmp/candidate-home \
    --env CARGO_HOME=/tmp/cargo \
    --env npm_config_cache=/tmp/npm-cache \
    --tmpfs /tmp:rw,nosuid,nodev,mode=1777,size=2g \
    --entrypoint /bin/bash \
    "$image" -euo pipefail -c '
      tar -xf - -C /workspace --no-same-owner \
        --exclude=.env --exclude="*/.env" \
        --exclude=".env.*" --exclude="*/.env.*" \
        --exclude=".azure" --exclude="*/.azure" \
        --exclude=".aws" --exclude="*/.aws" \
        --exclude=".config/gh" --exclude="*/.config/gh" \
        --exclude=".kube" --exclude="*/.kube" \
        --exclude=".ssh" --exclude="*/.ssh" \
        --exclude=".docker" --exclude="*/.docker" \
        --exclude=".netrc" --exclude="*/.netrc" \
        --exclude=".npmrc" --exclude="*/.npmrc" \
        --exclude=".git-credentials" --exclude="*/.git-credentials" \
        --exclude=".gitconfig" --exclude="*/.gitconfig"

      if find /workspace \( -type f -o -type l \) \( -name ".env" -o -name ".env.*" -o -name ".netrc" -o -name ".npmrc" -o -name ".git-credentials" -o -name ".gitconfig" \) -print -quit | grep -q .; then
        echo "credential-like file found in candidate snapshot" >&2
        exit 1
      fi
      if [[ -e /var/run/docker.sock || -e /var/run/docker-host.sock || -e /home/vscode/.azure || -e /home/vscode/.config/gh ]]; then
        echo "controller socket or credential path found in candidate container" >&2
        exit 1
      fi
      if [[ -n "${COPILOT_GITHUB_TOKEN+x}" || -n "${DARWIN_TEST_SENTINEL+x}" ]]; then
        echo "controller environment found in candidate container" >&2
        exit 1
      fi
      if [[ "$1" == --assert-isolation ]]; then
        [[ "$(id -u)" != 0 && "$HOME" == /tmp/* ]]
        printf '%s\n' "candidate isolation assertions passed"
        exit 0
      fi

      mkdir -p "$HOME" "$CARGO_HOME" "$npm_config_cache"
      exec env -i \
        HOME="$HOME" \
        PATH="$PATH" \
        CARGO_HOME="$CARGO_HOME" \
        RUSTUP_HOME="$RUSTUP_HOME" \
        npm_config_cache="$npm_config_cache" \
        LANG=C.UTF-8 \
        LC_ALL=C.UTF-8 \
        /bin/bash --noprofile --norc -euo pipefail -c "$1"
    ' candidate "$([[ "$assertion" == true ]] && printf -- '--assert-isolation' || printf '%s' "$command_text")"
status=$?
set -e
exit "$status"
