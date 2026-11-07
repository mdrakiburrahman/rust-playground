# Schema evolution experiment

| Purpose | Command | Result |
| --- | --- | --- |
| Verify exact data counts and bounded hydration polling | `npx nx run schema-evolution-experiment:verify` | Offline verifier checks; not an end-to-end qualification. |
| Compile immutable v1/v2 SDK fixtures and independent Rust clients | `npx nx run schema-evolution-experiment:build-fixtures` | v1 binary has no v2 dependency; v2 includes nullable `bar`. Offline artifacts are not remote catalog proof. |
| Probe pinned Kustainer prerequisites | `npx nx run schema-evolution-experiment:probe-kustainer` | Evidence/report under `.temp/experiments/schema-evolution`; capability acceptance is inconclusive without real data evolution. |
| Read scenario and qualification gates | [Schema evolution to Kusto](../../docs/experiments/schema-evolution/README.md) | Architecture, separate Azure stamp setup and evidence requirements. |

The verifier reuses the Azure QAP Kusto REST client and never modifies the
external table during hydration polling. Initial setup must create an inferred
external table, enable QAP, then enable `AutoUpdateSchema`; the flag cannot be
set before QAP exists. Catalog/SDK runner integration and fresh-bootstrap live
qualification remain required.
