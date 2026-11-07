# Schema evolution experiment

| Purpose | Command | Result |
|---|---|---|
| Verify exact data counts and bounded hydration polling | `npx nx run schema-evolution-experiment:verify` | Offline verifier checks; not an end-to-end qualification. |
| Read scenario and qualification gates | [Schema evolution to Kusto](../../docs/experiments/schema-evolution/README.md) | Architecture, separate Azure stamp setup and evidence requirements. |

The verifier reuses the Azure QAP Kusto REST client and never modifies the
external table during hydration polling. Catalog/SDK runner integration and
fresh-bootstrap live qualification remain required.
