# Lane P screening kit

This is an offline, fail-closed harness for screening the Parquet writer and Delta-statistics knobs. It runs on one local collector against one Delta table.

**Status:** it is validated with synthetic, non-performance tests only. It contains no measurement, no winner and no A/B/A result. Historical BC6 rows are reference inputs, not Lane P evidence.

The pinned base is `3259d8cd0fff5eeac77d77308898721cf696f7bd`; `crates/arrow-deltalake` is byte-identical to the earlier `cb33bfa1` pin, and every emitted env name was re-checked against its parse sites. The kit adds no new collector knobs and changes no defaults.

## Files

| Path | Role |
| --- | --- |
| `policy.mjs` | Shared rules for the offline pre-parser, row identity, resume and admission. The runner and the analyzers use them. |
| `screen-plan.mjs` | Generates plans (pilot, probe, headroom, grid, conditional) and handles validation, readiness, admission, the adaptive grid choice and the heatmap. |
| `confirm.mjs` | Candidate selection, frozen n=5 A/B/A registration and scoring. The gates are unchanged: effect 1.05, t 2.776, bytes 1.10, drift 0.05, n=5. The headroom requirement is above 1.5x. |
| `prepare-harness.mjs` | Deterministically turns the pinned BC6 harness in `reference/` into `fixture/`. |
| `fixture/` | Generated runner (`score.ts`, `measure.ts`, `base.yaml`) plus `audit.mjs` and `policy.mjs`. It has **no `footer` yet**; that is compiled on the cleared host. |
| `reference/` | Pinned BC6 inputs: the harness, `defaults-rate.json` (`.common`), a 26-leaf `footer.json`, and `bc4-footer.rs`. |
| `writer-inventory.csv` | The 33 writer and Delta-statistics surfaces at the base SHA. |
| `coverage-map.csv` | How each of those 33 rows is handled (see below). |
| `screen-pilot.json`, `screen-probe.json`, `screen-headroom.json` | Real-mode plans regenerated from the current kit. Regenerate them again after any change to the base settings or topology. |
| `test/kit.test.mjs` | Synthetic end-to-end test plus negative tests (15 tests). |
| `test/mutate.cjs` | Mutation check: re-introduces each listed defect in a temp copy and requires the suite to fail. |
| `validation.txt` | Exact output of the last offline validation run. |
| `lane-state.md` | Lane checkpoint: lease waiter, kit status and the campaign host rules (verbatim). |
| `SHA256SUMS` | Hashes of every kit file except itself. |

## Offline validation (run here; no host needed)

```powershell
cd <kit>
Get-ChildItem *.mjs, fixture\*.mjs, test\* | % { node --check $_.FullName }
node --experimental-strip-types --check fixture\score.ts
node --experimental-strip-types --check fixture\measure.ts
node screen-plan.mjs validate reference\defaults-rate.json reference\data\defaults-rate\defaults-rate-a4az2c\footer.json screen-pilot.json screen-probe.json screen-headroom.json
Get-FileHash -Algorithm SHA256 (Get-Content SHA256SUMS | % { $_.Substring(66) })   # compare with SHA256SUMS
node --test test\kit.test.mjs
node test\mutate.cjs <kit>
```

The last two commands copy the kit into `%TEMP%` and delete their copies when they finish. `score.ts` cannot execute on Windows because it calls `getconf`, so only syntax checks run here.

## Synthetic mode

Set `BWP_SYNTHETIC_NON_PERFORMANCE=1` to enable synthetic mode. Any other value is rejected.

- In synthetic mode the kit admits only rows with `synthetic_non_performance: true`. Every output is stamped with that flag, and the heatmap SVGs say `SYNTHETIC HARNESS TEST DATA - NOT A MEASUREMENT`.
- Real mode (the variable unset) refuses synthetic rows, selections, registrations and verdicts.
- Synthetic mode refuses real ones.

The test suite's response model only shapes test data. Its numbers mean nothing.

## The pre-parser is not the Rust parser

`policy.mjs` mirrors `writer.rs` and `delta_stats.rs` at the base SHA, and adds stricter checks whose messages start with `kit rule:`. It catches the following before a plan reaches the host:

- unknown plan or cell fields, and environment names that are typos or forbidden (`BWA_ROOT`, `BWA_BIN`, allocator variables)
- enum casing, and dictionary tokens used as a fallback encoding
- encodings that are illegal for a column's physical type
- codec and level mismatches (zstd 1..=22, gzip 0..=9, brotli 0..=11; no level for the other codecs)
- inactive or contradictory combinations, and `sorting_columns`
- column worker counts other than 1

These errors are reported by the offline kit, not the collector. Acceptance by the Rust parser and OAT, plus effective-config readback, is verified only by the live readiness probe.

## Integrity rules (coordinator gaps → fix)

| Gap | Fix | Negative test / mutant |
| --- | --- | --- |
| `score.ts` hashed only itself | It now pins `harness_files` for `score.ts`, `measure.ts`, `audit.mjs`, `policy.mjs` and `base.yaml`, plus `config_sha256`, `harness_sha256` and the `footer_sha256` of `fixture/footer`. `identityProblems` requires every field to be present and well shaped (40-hex sha, clean tree, exactly four SHA-256 binaries including the `arrowcol-loadgen` table initializer, mimalloc provenance at that sha, Node version, allocator env, local storage). An all-undefined identity has 14 problems. | 21 identity mutations; mutants "identity accepts missing hashes/dirty tree/malformed harness hashes" |
| `footer_binary_sha256` was registered but never compared | Every row's `footer_sha256` must equal the registered footer during admission and scoring. Registration hashes `fixture/footer` against the screened identity. The runner refuses if the footer changes mid-run. | "score ignores the footer hash", "register ignores the fixture footer" |
| Resume treated any prior cell as done | `resumeProblems` accepts only a clean, in-order prefix of the same plan: same plan sha, knobs and identity, status `ok`, and unique run directories directly under the output. Anything else stops the runner with `Refusing incompatible or replayed output`. | 9 resume cases; 3 mutants |
| Admission did not check duration/warmup, and selection did not require bookends | Each row must match the planned duration and warmup and a measured window within max(1 s, 1%). Rows must have finite, self-consistent rates, bytes and CPU, physical footer reconciliation, and no retries. Phases need the complete row count, no time overlap, a start after their prerequisite ends, one identity and corpus cohort, and `base`/`base-end` bookends with drift ≤ 0.05. | 25 admission cases; 4 mutants |
| Build provenance and the executed binaries were checked separately | `identityProblems` also requires `build_provenance.binaries` to equal the hashed binaries (and `delta-maintenance` to equal `maintenance_sha256`). `audit.mjs` requires exactly one runtime `arrowcol_effective_config` build receipt with `allocator: mimalloc` and `source_revision` equal to the row sha. The runner also refuses ambient `MIMALLOC_*` overrides. | 3 identity mutations; mutant "identity ignores provenance binary hashes" |
| Unknown fields, env typos and illegal enum/type/level combinations | Strict pre-parser (above). The grid skips illegal combinations and prints `SKIPPED <id>: <reason>`. | 31 writer rejections and 4 accepts, 10 plan/env cases, 6 stats-column cases; 3 mutants |
| Stale plans or evidence replayed into later phases | Each phase plan must be the byte-exact regeneration from the base settings. Selection and registration have `.sha256` sidecars, and the analyzer files are pinned at selection. Changed plans, analyzers or gates are rejected with `Frozen registration mismatch`. Every output is written with `wx`, so nothing is overwritten. | CLI, registration and scoring refusals |

## Live corrections (first probe, 2026-11-25)

The first live probe (6 of 29 cells) found two fixture defects. Both are fixed in the runner; neither changes a gate.

- **Delta statistics were never delivered.** Without an initializer, `arrowcol-catalog-rust` creates table v0 from DDL and ignores `ARROWCOL_COLLECTOR_DELTA_STATS_COLUMNS`, so every `delta_stats-*` commit failed its stats check. OAT local delivers this creation-time policy through `arrowcol-loadgen initialize-local --partition-by-ingestion-date` before startup (`arrowcol-oat-rust/src/runner.rs`, `local_initializer_args`). The runner now runs that initializer for **every** cell, defaults included, so all arms share one table-creation path; the catalog adopts the table at v1 and keeps `delta.dataSkippingNumIndexedCols`. The initializer binary is part of the pinned identity, and the result records `table_initializer` (the parsed `arrowcol_delta_stats_config` receipts). Feasibility: `live/feas-stats.sh`, `live/feas-stats-job.log`.
- **First-cell cold-memory stall.** The first cell showed a 15.6 s commit gap, consistent with direct reclaim/compaction on a cold host. `live/phase.sh` now runs one untimed, unscored defaults warm-up (`runs/<phase>.warm`, its rc ignored) before every timed phase. Each row records non-scoring `host_memory` (`allocstall_*`, `pgscan_direct*`, `compact_stall`, `thp_fault_alloc`, MemFree/MemAvailable/Cached) at the window bounds, and the phase log records the same counters.
- **Probe-only plans continue past an invalid cell** so that one bad arm does not hide the other 28. Readiness still fails closed, because `admit()` requires every row present with `status: ok`. All other phases still stop at the first invalid cell. Invalid-cell errors now name `retry_offered_rps` and `committed_rps`.

The scoring gates and statistics are unchanged. An incomplete or failed confirmation returns `INCONCLUSIVE` with errors; it is never silently resumed or replaced. `speed_beats_control` is a precondition for registering a screening candidate, not a statistical gate.

## Coverage of the 33 inventory rows

| Status | Rows | Meaning |
| --- | --- | --- |
| `screened` | 10 | A pilot arm sets the knob. |
| `screened_partial` | 4 | `compression_level` (four levels only), `encoding` (delta fallbacks only), `dictionary_enabled` (off with delta fallbacks; the plain fallback is probe-only under amendment A1) and `delta.dataSkippingNumIndexedCols` (only 0, through `[]`). |
| `conditional_probe` | 8 | Arms run only if their trigger factor moves rate or bytes/row by more than 5%. All of them are always present in the readiness probe. |
| `not_expressible` | 4 | No knob at the base SHA: `bloom_filter_position`, `coerce_types`, `content_defined_chunking`, `skip_arrow_metadata`. Adding one would be a separate, measured proposal. |
| `non_performance_or_unsupported` | 7 | Not tuning inputs, or they would falsify metadata. |

The pilot has **11 factors in 20 arms** (18 treatments and 2 bookends): codec, encoding, dictionary_bytes, data_page_bytes, statistics, bloom, version, row_group, page_rows, write_batch and delta_stats. That pilot is not evidence that all 33 rows were exercised. `coverage-map.csv` lists the arms for each row and the reason for every row that has no arm. The test checks every listed arm against the plans.

## Amendment A1: `encoding=no_dictionary` is probe-only

`amendment-A1.md` records this amendment. It was pre-registered at 2026-11-25T07:20:48Z, before the second pilot. `SCREEN_EXCLUDED` in `screen-plan.mjs` removes the arm from the pilot, grid and heatmap levels. The readiness probe keeps it, so `screen-probe.json` is unchanged at `7f8e3733…`. The pilot drops from 21 to 20 arms; the other cells and their order are unchanged. The gates, the duration rule and the runner are unchanged.

## Live phase order (after lease clearance)

Use real mode throughout (leave `BWP_SYNTHETIC_NON_PERFORMANCE` unset). Use these settings:

- `B=reference/defaults-rate.json` and `F=reference/data/defaults-rate/defaults-rate-a4az2c/footer.json`.
- `RUN` must be outside the repository checkout, or gitignored.
- Use a **fresh output directory per phase**.

Run the fixture from the guarded rust-playground checkout. Put the fixture outside the checkout, and set `BWA_BIN` to the verified release directory containing `build-provenance.json`.

```bash
node --experimental-strip-types $FIX/score.ts <plan.json> <out-dir>   # rows: <out-dir>/results.jsonl
```

1. `screen-plan.mjs probe B F probe.json`. Run it, then `screen-plan.mjs readiness B F readiness.json probe.json probe/results.jsonl`. This proves Rust parser acceptance, readback and footer properties for all 29 arms.
2. `screen-plan.mjs pilot B F pilot.json`, then run it.
3. `screen-plan.mjs grid B F grid.json pilot.json pilot/results.jsonl` picks the two factors with the largest log-rate spans. Then run it.
4. `screen-plan.mjs conditional B F cond.json pilot.json pilot/results.jsonl`. Run it if a plan is written; `NO_CONDITIONAL_PHASE` writes nothing.
5. `screen-plan.mjs headroom B F room-default.json`, then run it.
6. `confirm.mjs select B F pilot.json pilot/results.jsonl grid.json grid/results.jsonl selection.json room-fast.json [cond.json cond/results.jsonl]`, then run `room-fast.json`.
7. `confirm.mjs register B F selection.json room-default.json room-default/results.jsonl room-fast.json room-fast/results.jsonl $FIX confirm.json registration.json`.
8. Run `confirm.json` once with no replacements, then `confirm.mjs score B F registration.json confirm.json confirm/results.jsonl verdict.json`.
9. `screen-plan.mjs heatmap B F heat pilot.json pilot/results.jsonl grid.json grid/results.jsonl` writes `heat.csv`, `heat-rps.svg` and `heat-bytes.svg`.

If the fastest arm trades bytes for speed, `selection.json` also reports the arm with the smallest bytes/row as `smallest`, with its rate cost in `smallest_throughput_cost`.

## Remaining live-only gates

1. **Lease clearance** from the coordinator, under the campaign host rules (in `lane-state.md`), and the path guard at the start of every job.
2. **Build provenance:** binaries are built at the integration HEAD, the tree is clean, `build-provenance.json` shows mimalloc and the source sha, and the four binary hashes (including `arrowcol-loadgen`) are recorded.
3. **Footer tool:** compile `reference/bc4-footer.rs` with the pinned versions, put it at `$FIX/footer`, and hash it into the identity.
4. **Rust parser acceptance and effective-config readback** for all 29 probe arms (readiness).
5. **Physical audit:** every ingestion Add must reconcile with its footer, and every requested property must reconcile with the startup and table receipts.
6. **Client headroom** above 1.5x for both the default and the candidate, from L0/L1 diagnostic arms.
7. **Storage reserve:** check free space before each phase. A `failed_storage_reserve` arm is a failure, not a result, and no data is deleted.
8. **Measured topology:** set CPU masks and the memory budget for the cleared host's measured topology, including SMT, rather than assuming the BC6 shape. Then regenerate the plans.
