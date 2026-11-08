# Registry tooling

`tools-scripts` is the Nx project for repository automation that must remain
testable outside workflow YAML. Its process runner uses argument arrays rather
than a shell, supports captured or inherited streams, and keeps stdin separate
from argv and command errors.

Run all side-effect-free checks with:

```bash
npx nx run tools-scripts:verify
```

## Markdown and working-tree checks

```bash
npx --no-install nx run tools-scripts:markdown-lint
npx --no-install nx run tools-scripts:markdown-lint-fix
npx --no-install nx run rust:fail-on-untracked-files
```

Root `rust:lint` includes `markdown-lint`, which runs without caching so new
files cannot bypass checks. `.markdownlint-cli2.yaml` covers all first-party
Markdown, including hidden directories and untracked files, while honoring Git
ignore rules and excluding submodules, dependencies, and generated output.
Long lines and inline HTML are allowed; the PR template alone is exempt from
requiring a top-level heading.

CI discovers Markdown lint as a Node check and runs it read-only. Autofixing is
explicit and local. Host and container verification then invoke the root
cleanliness gate, backed by `tools-scripts:clean-worktree`. The gate rejects
staged and unstaged tracked changes, deletions, renames, dirty submodules, and
unexpected nonignored untracked files, with status and diff diagnostics. It is
not a dependency of local lint: normal authoring need not be committed first.

## Schema catalog bug bash

Run `npx --no-install tsx tools/scripts/src/schema-catalog-bugbash.ts <step>`
inside the Linux devcontainer. Each step needs a new absolute `--evidence-dir`
outside the checkout. Stop on nonzero; copy its `evidence.tar`, `receipt.json`,
and `SHA256SUMS` before recreating the devcontainer.

| Step | Required arguments | Expected result |
| --- | --- | --- |
| `verify` | None; optional `--expected-sha <full SHA>` | Clean checkout must match the exact HEAD SHA; all exact-head CI runs succeed, regression/build gates pass, six `loadtest:build` images are revision- and config-verified against Compose aliases, supported image smokes and SDK bundles pass. |
| `local` | `--verified-receipt <verify/receipt.json>` | Normal then Quiver crash/recovery, native local schema evolution, all 13 table SDK/artifact bundles, preserved private seals and no live attempt writers. |
| `reset` | `--verified-receipt <verify/receipt.json> --confirm-reset <dedicated group>` | **Destructively resets only the explicitly confirmed isolated group**, then provisions and checks fresh readiness. Saves private base/ready/manifest files outside the checkout; receipt binds their hashes, generation and source. |
| `azure` | `--reset-receipt <reset/receipt.json>` | Checks the receipt chain and live ready generation, runs native Azure evolution, verifies owned-process/data cleanup. Never provisions or resets. |

`verify` requires existing Node dependencies and Docker; it builds the six
`loadtest:build` image dependencies locally and does not pull published images.
Azure needs the synced `.env`, `infra/azure/stamp.yaml` and signed-in
`az`. The CLI projects only the subscription and Kusto tags from `.env`; it uses
`--resource-group`, defaulting to `rust-playground-schema-evo-qualification-20261108`,
and refuses the old/default or any non-dedicated group. Private inputs are mode 0600
in CLI-managed directories outside the checkout. Stamp/AKS/ACR identities and stale
ready generations are never copied into the schema-evolution lane.

Optional `--env-file` supplies an approved base instead; `azure --verified-receipt
<verify/receipt.json> --ready-env <ready output>` supports an already-provisioned
sandbox without resetting it. Optional `--subscription <UUID>` must match the base.
Do not move/delete CLI-managed private state before the Azure step. A blocked
CI gate exits nonzero: wait for the automatic run, retain its failed
evidence, then retry `verify` with a new evidence directory. Local failure stops
owned services and archives diagnostics without discarding/resetting the attempt.

## Registry commands

The registry targets forward arguments to a Commander CLI:

```bash
npx nx run tools-scripts:registry-login -- --owner OWNER --environment local
npx nx run tools-scripts:registry-tags -- --branch main --sha FULL_GIT_SHA
npx nx run tools-scripts:registry-manifest -- \
  --owner OWNER --repository rust-playground --image hello-world --tag TAG
npx nx run tools-scripts:registry-public -- \
  --owner OWNER --repository rust-playground --image hello-world
npx nx run tools-scripts:image-publish-plan
```

In CI, `registry-login` uses `GHCR_TOKEN` and then `GITHUB_TOKEN`. Locally it
uses `gh auth token`, whose login needs the `write:packages` package scope for
publication; `read:packages` supports downloads only. The selected token is
supplied only to `docker login ghcr.io --username OWNER --password-stdin`; it
is not placed in argv, emitted by the CLI, or written to repository files.

Tag metadata contains a collision-resistant mutable
`branch-SANITIZED_BRANCH` tag, an immutable `sha-FULL_GIT_SHA` tag, and
structured `latest` metadata only for the default branch. Image targets and
workflows consume the JSON CLI output.

`image-publish-plan` reads the Nx graph, fails closed unless the five production
runtime images and retained `hello-world` sample expose the complete image,
smoke, cached CI build, guarded publish, and manifest contract, then emits the
GitHub Actions matrix through `GITHUB_OUTPUT`.

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
