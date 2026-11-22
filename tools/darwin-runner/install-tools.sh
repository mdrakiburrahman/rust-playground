#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"
readarray -t pin < <(node -e '
  const p = require("./tools.lock.json").darwin;
  console.log(p.url);
  console.log(p.sha256);
  console.log(p.version);
')

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
curl --fail --silent --show-error --location --retry 2 "${pin[0]}" -o "$work/darwin"
printf '%s  %s\n' "${pin[1]}" "$work/darwin" | sha256sum --check
install -m 755 "$work/darwin" /usr/local/bin/darwin
test "$(darwin --version)" = "darwin ${pin[2]}"
