# Schema evolution experiment

| Purpose | Command | Result |
| --- | --- | --- |
| Verify exact data counts and bounded hydration polling | `npx nx run schema-evolution-experiment:verify` | Offline verifier checks; not an end-to-end qualification. |
| Compile immutable v1/v2 SDK fixtures and independent Rust clients | `npx nx run schema-evolution-experiment:build-fixtures` | v1 binary has no v2 dependency; v2 includes nullable `bar`. Offline artifacts are not remote catalog proof. |
| Run the real catalog, DFE, historical SDKs, Delta and pinned Kustainer | `npx nx run schema-evolution-experiment:run -- --target local` | Records real CES, same-process old writes, physical nulls and bounded native schema/data hydration. A failed native refresh is not a pass. |
| Run on the approved isolated Azure sandbox | `npx --no-install tsx experiments/schema-evolution/src/private-nx.ts run --mode schema-evolution --env-file .env.schema-evo.private --ready-env .env.azure.schema-evo` | Invokes public `schema-evolution-experiment:run` with isolated private environment; requires genuine ready generation. Real Azure charges apply. |
| Probe pinned Kustainer prerequisites | `npx nx run schema-evolution-experiment:probe-kustainer` | Evidence/report under `.temp/experiments/schema-evolution`; capability acceptance is inconclusive without real data evolution. |
| Read scenario and qualification gates | [Schema evolution to Kusto](../../docs/experiments/schema-evolution/README.md) | Architecture, separate Azure stamp setup and evidence requirements. |

The verifier reuses the Azure QAP Kusto REST client and never modifies the
external table during hydration polling. Initial setup must create an inferred
external table, enable QAP, then enable `AutoUpdateSchema`; the flag cannot be
set before QAP exists. The real runner never rebuilds v1 as v2, never manually
reconciles the external table after ALTER, and never infers the shared Azure
environment. It cleans up only its own processes, external table and run
prefix; failed publisher shutdown retains cloud data and occupancy. Two
fresh-bootstrap live rehearsals remain required before final qualification.

## Fresh-bootstrap bug bash

| Step | Command or action | Required result |
| --- | --- | --- |
| Pin inputs | Record `git rev-parse HEAD` and `sha256sum experiments/schema-evolution/README.md` | Both rehearsals use the same final integration SHA and runbook hash. |
| Fresh environment | Through the owning privy lease, run `Start-DevboxBootstrap -Repo rust-playground -Mode Full -Branch <branch> -LeaseId <lease>` and check out the recorded SHA remotely | New devcontainer; record devbox, lease ID and source SHA. No local builds. |
| Local qualification | `npx nx run schema-evolution-experiment:run -- --target local` | All six stages pass, actual old/new Delta cohorts and native Kustainer refresh recorded. |
| Azure pre-pass preparation | After explicit custody authorization only: read the owned authority and inventory, then follow the bounded reset sequence below | Fresh Full deletes local manifests; a ready stamp without its manifest must not be fully provisioned or implicitly recovered. |
| Isolated Azure readiness | `npx --no-install tsx experiments/schema-evolution/src/private-nx.ts provision --mode schema-evolution --env-file .env.schema-evo.private --tables .env.azure.schema-evo.tables.json --output .env.azure.schema-evo` | Invokes public `azure-infra:provision`: DFE-only initialization under custody, KQL reconciliation and captured ready generation. Real Azure charges apply. |
| Azure qualification | `npx --no-install tsx experiments/schema-evolution/src/private-nx.ts run --mode schema-evolution --env-file .env.schema-evo.private --ready-env .env.azure.schema-evo` | Invokes public Nx run: actual native schema/data hydration and clean publisher shutdown. |
| Repeat | Preserve evidence, then repeat the Full bootstrap and both qualifications at the same recorded SHA/runbook hash | Two independent reports with matching inputs, stamp identities and reproducible commands. |

Prepare `.env.schema-evo.private` through repo-bootstrapper's private sync,
without copying shared resource-name/table-root overrides. A Full bootstrap
does not preserve ignored ready outputs or table manifests: regenerate them
using supported provision, not copied or fabricated generations. If an
already-ready stamp lacks its required table manifest, stop at the explicit
bootstrap error and use the documented isolated sandbox recovery/reset flow;
never weaken its custody check or operate on the shared stamp.

Before **each authorized Azure pass**, require exact owned resource/container
identities, released occupancy and an empty publisher registry through
`PublisherControl.read` and `reset_inventory` using
`new Config(load_env(".env.schema-evo.private"), "schema-evolution")`.
Do not recover an orphan or adopt authority as qualification preparation.
The following commands are destructive/billed and require the coordinator's
final custody authorization, not merely a source-verification assignment:

```sh
# Explicit preconditioning keeps authority intact and does not require a local manifest.
npx --no-install tsx infra/azure/bootstrap.ts provision --mode schema-evolution --env-file .env.schema-evo.private --infra-only --output .env.schema-evo.precondition
# Read-only inventory preview; recheck ready-v1/released/empty registry and Running.
npx --no-install tsx infra/azure/bootstrap.ts reset --mode schema-evolution --env-file .env.schema-evo.private
# Stops only workspace-owned services with a genuine generation-bound receipt, waits 60s, then resets under custody.
npx --no-install tsx infra/azure/bootstrap.ts reset --mode schema-evolution --env-file .env.schema-evo.private --execute
npx --no-install tsx experiments/schema-evolution/src/private-nx.ts provision --mode schema-evolution --env-file .env.schema-evo.private --tables .env.azure.schema-evo.tables.json --output .env.azure.schema-evo
```

After reset, require fenced-v1/released/empty registry and actual empty data
inventory before full provision. The precondition output is genuine but is
not the final ready input. Full provision creates its own new manifest and
exports the captured live generation; never copy old manifests/generations.
Immediately verify scenario cleanup (including owned PID and exact all-state
Kustainer absence for local) before repeating Full. Mid-run recovery or orphan
cleanup invalidates that pass.

The approved bootstrap sync must supply `.env` and Azure CLI sign-in on each
fresh box. Create the minimal private input without copying shared resource
names, roots or old ready generations:

```sh
node --import tsx --input-type=module -e '
import fs from "node:fs";
import { load_env, Config, write_private } from "./infra/azure/bootstrap.ts";
const source = load_env(".env");
const values = { AZURE_SUBSCRIPTION_ID: source.AZURE_SUBSCRIPTION_ID,
  AZURE_RESOURCE_GROUP: "rust-playground-schema-evo" };
new Config(values, "schema-evolution");
if (fs.existsSync(".env.schema-evo.private")) throw Error("Private input already exists");
write_private(".env.schema-evo.private", values);
console.log("Private input created; values withheld.");
'
```

Stop if the approved synced subscription or sign-in is missing; never select
another subscription. `write_private` creates mode-0600 output. The private Nx
launcher reuses the Azure configuration/process helpers, removes ambient
stamp `AZURE_*`/`ARROWCOL_*` overrides while preserving Azure CLI
`AZURE_CONFIG_DIR` and `AZURE_EXTENSION_DIR`, overlays validated isolated names, and sets
`NX_LOAD_DOT_ENV_FILES=false`. This is necessary because root dotenv loading
can override a private `--env-file`. Both provision and scenario still run their
public Nx targets; the launcher does not substitute direct scenario execution.
Fresh full provision generates the 13-root manifest and ready output itself.

`--mode schema-evolution` is mandatory for every isolated bootstrap command
(including read-only `config`/`status`, reset, recovery and occupancy), private
launcher and Azure scenario. The default bootstrap remains stamped and requires
the validated stamp YAML. Isolated mode never loads that YAML: it accepts
`rust-playground-schema-evo` or a dedicated
`rust-playground-schema-evo-<suffix>` group (lowercase alphanumeric/hyphen
suffix, alphanumeric at both ends, at most 90 characters), keeps the
subscription:RG hash and rejects stamp/AKS/ACR configuration or foreign accounts. Generated outputs carry
`ARROWCOL_AZURE_MODE=schema-evolution` for the initializer and Rust consumers;
that value alone cannot select the TypeScript mode. It does not adopt or migrate
existing authority. No new Azure run is implied by a source-only verification.

The local run at `377b388` passed with Delta v4, three partitioned files and
24 exact old/new rows. The old SDK process continued after ALTER with physical
NULLs, the new SDK wrote exact sentinels, and native Kustainer QAP accelerated
all rows after 294006 ms without post-ALTER reconciliation. Keep
`PARTITIONED BY (tenant)` and every data assertion unchanged. This run,
component checks and the infrastructure bootstrap do not substitute for the
two complete fresh-bootstrap rehearsals.

The first isolated Azure rehearsal also proved all six data stages, including
native QAP hydration after 292289 ms. Its overall result is **failed**:
an active publisher remained after runtime shutdown, so the runner retained
cloud data and test occupancy. Complete supported publisher drain/recovery
before another Azure run or fresh-bootstrap qualification.

Supported isolated recovery has since completed with a genuine stop receipt
and the 60-second safeguard; authority is fenced, occupancy released and the
registry empty. The failed run remains failed. Reset and full provision must
use the DFE-only initializer before either final fresh-bootstrap rehearsal.
