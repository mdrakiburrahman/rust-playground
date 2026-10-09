# Registry tooling

`tools-scripts` is the Nx project for repository automation that must remain
testable outside workflow YAML. Its process runner uses argument arrays rather
than a shell, supports captured or inherited streams, and keeps stdin separate
from argv and command errors.

Run all side-effect-free checks with:

```bash
npx nx run tools-scripts:verify
```

The registry targets forward arguments to a Commander CLI:

```bash
npx nx run tools-scripts:registry-login -- --owner OWNER --environment local
npx nx run tools-scripts:registry-tags -- --branch main --sha FULL_GIT_SHA
npx nx run tools-scripts:registry-manifest -- \
  --owner OWNER --repository rust-playground --image hello-world --tag TAG
npx nx run tools-scripts:registry-public -- \
  --owner OWNER --repository rust-playground --image hello-world
```

In CI, `registry-login` uses `GHCR_TOKEN` and then `GITHUB_TOKEN`. Locally it
uses `gh auth token`. The selected token is supplied only to
`docker login ghcr.io --username OWNER --password-stdin`; it is not placed in
argv, emitted by the CLI, or written to repository files.

Tag metadata contains a sanitized mutable branch tag, an immutable
`sha-FULL_GIT_SHA` tag, and structured `latest` metadata only for the default
branch. Later image targets can import the same helpers or consume the JSON
CLI output.

`registry-manifest` checks the exact remote reference with
`docker manifest inspect` while suppressing the manifest body.

`registry-public` is deliberately read-only. GitHub's REST Packages API can
inspect package visibility but does not provide a visibility mutation
operation. A public package returns a `public` JSON result. A private package
returns exit code 2 and a machine-readable `needs-ui-change` result containing
the package settings URL and target visibility. That result is the integration
boundary for later Playwright UI automation. Nested package names are encoded
as one path segment, for example `rust-playground%2Fdevcontainer`.

See [container publishing](../../docs/containers/publishing.md) for the exact
local devcontainer flow, tag policy, public-visibility step, workflow triggers,
and manifest checks.
