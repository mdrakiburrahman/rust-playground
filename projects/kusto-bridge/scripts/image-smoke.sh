#!/usr/bin/env bash
# Smoke-tests the locally loaded kusto-bridge image without a Kusto emulator.
set -euo pipefail

image="ghcr.io/mdrakiburrahman/rust-playground/kusto-bridge:dev"
container="kusto-bridge-image-smoke-$$"

cleanup() {
  docker rm --force "$container" >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker run --rm --platform linux/amd64 "$image" --version >/dev/null
docker run --rm --platform linux/amd64 --entrypoint /bin/sh "$image" -eu -c '
  test "$(id -u)" -eq 10001
  test "$(id -g)" -eq 10001
  test ! -w /usr/local/bin/kusto-bridge
  test -w /state
  ! command -v curl >/dev/null
'
# Invalid configuration must fail fast instead of serving.
if docker run --rm --platform linux/amd64 \
  --env KUSTO_BRIDGE_KUSTO_ENDPOINT=http://kusto.invalid:8080 \
  --env KUSTO_BRIDGE_SOURCES=direct=relative \
  "$image" >/dev/null 2>&1; then
  echo "kusto-bridge accepted a relative source root" >&2
  exit 1
fi

docker run --detach --platform linux/amd64 --name "$container" \
  --env KUSTO_BRIDGE_KUSTO_ENDPOINT=http://kusto.invalid:8080 \
  --env KUSTO_BRIDGE_SOURCES=direct=/delta/direct \
  --health-interval=2s \
  --health-timeout=3s \
  --health-start-period=5s \
  --health-retries=30 \
  "$image" >/dev/null

for _ in $(seq 1 60); do
  status="$(docker inspect --format '{{.State.Health.Status}}' "$container" 2>/dev/null || true)"
  case "$status" in
    healthy) break ;;
    unhealthy)
      docker logs "$container" >&2
      exit 1
      ;;
  esac
  if [[ "$(docker inspect --format '{{.State.Running}}' "$container" 2>/dev/null || true)" != true ]]; then
    docker logs "$container" >&2
    exit 1
  fi
  sleep 1
done
test "$(docker inspect --format '{{.State.Health.Status}}' "$container")" = healthy

docker exec "$container" /usr/local/bin/kusto-bridge healthcheck --url http://127.0.0.1:9464/status
docker exec "$container" /usr/local/bin/kusto-bridge healthcheck --url http://127.0.0.1:9464/metrics
if docker exec "$container" /usr/local/bin/kusto-bridge healthcheck --url http://127.0.0.1:9464/missing 2>/dev/null; then
  echo "unknown route answered successfully" >&2
  exit 1
fi

docker stop --time 20 "$container" >/dev/null
test "$(docker inspect --format '{{.State.ExitCode}}' "$container")" -eq 0
docker rm "$container" >/dev/null
trap - EXIT
