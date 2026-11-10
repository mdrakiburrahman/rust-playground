# E1-E5 comparable AKS inputs

| Run | ArrowCols | Tables | Clients/table | DFE concurrency |
| --- | --- | --- | --- | --- |
| E1 | 4 | All 32 canonical DMV schemas | Baseline: 1 | Baseline |
| E2 | 8 | Same 32 | Same | Same |
| E3 | 16 | Same 32 | Same | Same |
| E4 | 16 | Same 32 | 4 (128 clients total) | Same |
| E5 | 16 | Same 32 | 1 | Explicitly approved higher value |

This is source/config preparation, not evidence that a pilot or any experiment
ran. Earlier parse-only drafts with 19 `TODO` table slots and fixed 30/120-second
timings are historical receipts, not executable inputs. The binder reads the
now-approved canonical fixture rather than copying or inventing table names.

## Inputs and remaining binding

| Input | Source / rule |
| --- | --- |
| Private baseline | Generated `out/arrowcol-oat/oat-config.aks.json`, its runtime JSON and bootstrap environment file. Do not copy secrets into tracked files. |
| Data and load | Baseline `rows_per_table`, `dmv_concurrency`, one client/table, exactly one baseline DFE setting. Retain all other generated settings. |
| Images, storage, pod sizing | Preserve runtime file, immutable image digests, destinations and optional overlay unchanged across all five configs. Freeze the checkout, runtime and overlay contents after the pilot; paths alone do not freeze their contents. |
| QAP database | Read from the private baseline; no hardcoded database name or null fallback. |
| Warm-up and measurement | **Unbound until live pilot review.** No duration defaults in the binder. |
| E5 concurrency | **Unbound until explicitly selected.** Must exceed baseline and remain in 1-256. |

At the separately authorized pilot stage, use the existing OAT launcher and
`pilot_mode: true` with all 32 tables and the same frozen workload/runtime
settings. Pilot measurements must consist of complete 30-second windows, at
least two windows and at most ten minutes including warm-up. Retain all raw
`pilot_table=... window=...` and `stable_from_seconds=...` output, configs,
source/image hashes, orchestration/cell run IDs and failures.

Compare candidate warm-up/steady durations and choose the shortest qualifying
pair only after inspecting the **entire** observation for every table. The
range divided by mean must be strictly below 5%; an early stable prefix cannot
qualify a later drift. Select a shared pair that covers all five comparison
settings; if any setting fails its qualification, revise/re-pilot before
freezing the common pair. A printed shortest horizon is not proof that a
different warm-up was tested. Do not treat untested candidates as measured.

The final binding step, after that review and explicit run authorization, is:

```bash
# Repository root, devcontainer. These three values come from the approved pilot record.
: "${WARMUP_SECONDS:?bind the measured warm-up}"
: "${MEASURE_SECONDS:?bind the measured steady duration}"
: "${HIGHER_DFE:?bind the approved E5 concurrency}"
npx --no-install tsx projects/arrowcol-oat/prepare-experiments.ts \
  --base out/arrowcol-oat/oat-config.aks.json \
  --out out/arrowcol-oat/e1-e5-approved \
  --warmup-seconds "$WARMUP_SECONDS" --measure-seconds "$MEASURE_SECONDS" \
  --higher-dfe "$HIGHER_DFE"
```

This command is offline: no Azure, Kubernetes, image publication or benchmark
operation. It reuses the OAT config loader and runtime validator, requires
existing private runtime/bootstrap files and digest-pinned images, and writes
five mode-0600 JSON configs into a new private output directory. It refuses to
overwrite existing output. Keep the original pilot records unchanged; this
command binds reviewed values, **does not independently certify pilot evidence**
and does not replace the Rust runner's complete config validation.

Before any authorized execution, archive hashes of the five configs, source,
runtime, images and overlay beside the pilot decision. Compare E4/E5 to E3:
only `clients_per_table` / `dfe_worker_concurrency`, respectively, may differ.
Do not change rows, DMV concurrency, pod resources or windows between runs.
Launch each config separately with the existing command:
`npx nx run arrowcol-oat:cli -- --mode aks --config out/arrowcol-oat/e1-e5-approved/E1.config.json`
(substitute E2-E5). **Launching is a live mutating operation and requires
separate authorization; preparation does not grant it.** Each launch retains
the runner's fresh cell identity/scoped namespace and cleanup behavior; the
common result/mirror locations intentionally aggregate the comparison.

Retain launched run IDs and exact expected owned-server Git SHA with each
Parquet/verdict receipt. QAP completion is observation evidence, not ADX
row-count proof. Keep failures and cleanup receipts; do not rewrite unknown
historical provenance or claim five experiments ran from these inputs.
