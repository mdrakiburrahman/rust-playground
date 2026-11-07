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
