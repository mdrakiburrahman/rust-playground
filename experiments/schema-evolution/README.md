# Schema evolution experiment

| Purpose | Command | Result |
| --- | --- | --- |
| Verify exact data counts and bounded hydration polling | `npx nx run schema-evolution-experiment:verify` | Offline verifier checks; not an end-to-end qualification. |
| Compile immutable v1/v2 SDK fixtures and independent Rust clients | `npx nx run schema-evolution-experiment:build-fixtures` | v1 binary has no v2 dependency; v2 includes nullable `bar`. Offline artifacts are not remote catalog proof. |
| Run the real catalog, DFE, historical SDKs, Delta and pinned Kustainer | `npx nx run schema-evolution-experiment:run -- --target local` | Records real CES, same-process old writes, physical nulls and bounded native schema/data hydration. A failed native refresh is not a pass. |
| Run on the separate, fully provisioned Azure stamp | `npx nx run schema-evolution-experiment:run -- --target azure --env-file .env.schema-evo.private --ready-env .env.azure.schema-evo` | Requires genuine ready generation; uses unique experiment-container prefix and native QAP/AutoUpdateSchema. Real Azure charges apply. |
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
| Fresh environment | Through the owning privy lease, run `Start-DevboxBootstrap -Repo rust-playground -Mode Full -Branch mdrakiburrahman-schema-catalog -LeaseId <lease>` and check out the recorded SHA remotely | New devcontainer; record devbox, lease ID and source SHA. No local builds. |
| Local qualification | `npx nx run schema-evolution-experiment:run -- --target local` | All six stages pass, actual old/new Delta cohorts and native Kustainer refresh recorded. |
| Isolated Azure readiness | `AZURE_RESOURCE_GROUP=rust-playground-schema-evo npx nx run azure-infra:provision -- --env-file .env.schema-evo.private --tables .env.azure.schema-evo.tables.json --output .env.azure.schema-evo` | Supported DFE-only initializer topology; captured ready generation exported. Real Azure charges apply. Never select shared sandbox inputs. |
| Azure qualification | `npx nx run schema-evolution-experiment:run -- --target azure --env-file .env.schema-evo.private --ready-env .env.azure.schema-evo` | Actual native schema and data hydration, elapsed observations and clean publisher shutdown. |
| Repeat | Preserve evidence, then repeat the Full bootstrap and both qualifications at the same recorded SHA/runbook hash | Two independent reports with matching inputs, stamp identities and reproducible commands. |

Prepare `.env.schema-evo.private` through repo-bootstrapper's private sync,
without copying shared resource-name/table-root overrides. A Full bootstrap
does not preserve ignored ready outputs or table manifests: regenerate them
using supported provision, not copied or fabricated generations. If an
already-ready stamp lacks its required table manifest, stop at the explicit
bootstrap error and use the documented isolated sandbox recovery/reset flow;
never weaken its custody check or operate on the shared stamp.

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
