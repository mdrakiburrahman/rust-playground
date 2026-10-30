# Azure QAP and Delta maintenance

**One Azure run showed broader acceleration work during partial OPTIMIZE.**
It created only **one replacement file**, retaining **seven unchanged files**,
but QAP subsequently reported **seven pending data files** and only **256 of
2,816 rows accelerated** at the new Delta version. Do not treat this as a clean
incremental-maintenance result.

That is evidence of broader pending acceleration work, **not sufficient proof
that unchanged Parquet bytes were reread/reingested**. Public aggregate
statistics do not provide the source-origin ingestion history or background CPU
needed to settle that question. The reingestion verdict remains
**INCONCLUSIVE**, not "no reload".

## Measured Azure observations

Observed on 2026-10-30 UTC with `rapid.json`: 256 rows/file, four small files in
each mergeable partition, four Hive partition roles, a continuously observed
idle table, and real physical zero-retention VACUUM. These are small functional
fixtures on a shared cluster, not production cost or throughput measurements.
Public cluster `ProductVersion`, sampled during the repeat:
`2026.10.19.2019-2641-911fa54-WeeklyProduction`.

First matrix:

| Scenario | Files + / - / same | Rows after | Deleted objects | Catch-up (s) | Observed pending peak | Final exact parity |
| --- | --- | --- | --- | --- | --- | --- |
| Idle control | 0 / 0 / 10 | 2,560 | n/a | 13.483 | 0 | PASS |
| Intentional full-cache recreation | 0 / 0 / 10 | 2,560 | n/a | 33.814 | 10 | PASS |
| Small blind append | 1 / 0 / 10 | 2,816 | n/a | 171.475 | 1 | PASS |
| Partial OPTIMIZE | 1 / 4 / 7 | 2,816 | n/a | 293.718 | **7** | PASS |
| Broad OPTIMIZE | 3 / 11 / 1 | 3,072 | n/a | 268.230 | 3 | PASS |
| Old-partition DELETE/PURGE | 0 / 4 / 4 | 1,792 | n/a | 114.913 | 0 | PASS |
| Caught-up VACUUM LITE | 0 / 0 / 4 | 1,792 | 8 | 23.990 | 0 | PASS |
| Idempotent QAP policy refresh | 0 / 0 / 4 | 1,792 | n/a | 2.867 | 0 | PASS |
| Concurrent appends + maintenance | 17 / 8 / 2 | 5,632 | 8 | 159.777 | 17 | PASS |
| Immediate OPTIMIZE/PURGE then VACUUM | 1 / 8 / 2 | 1,536 | 8 | 150.151 | 1 | PASS |

The full-cache control exposed ten pending files for ten unchanged source
files. Partial OPTIMIZE's seven-pending/one-replacement observation is the
standout anomaly; the runner surfaces this as a **finding** without converting
it into an unproven physical-reingestion claim.
Across 255 samples during partial OPTIMIZE, the untouched idle table stayed at
version 10, zero pending files and the same last-ingestion timestamp.
The repeat's Delta-log audit confirmed that OPTIMIZE committed one add and four
removes, all `dataChange=false`, with no `metaData` or `protocol` action. It was
not a schema/protocol change. Why Kusto scheduled broader work is not yet
established.

DELETE/PURGE removed 1,024 rows; subsequent VACUUM deleted eight tombstoned
objects without changing Delta version 13 or its 1,792 active rows. Neither
step showed a pending-file spike in this run. Immediate VACUUM also converged
correctly and produced no observed transient query error here; this does not
make zero retention generally reader-safe.

Both query lanes matched the complete independent Delta row multiset for
every completed case. First-matrix default-query CPU was 0-31.25 ms; **background ingestion CPU was
not available**. Catch-up includes mutation/oracle/convergence elapsed time,
not pure ingestion time. Last-ingestion timestamps moved for append and
rewrites, but not for the measured PURGE, VACUUM or idempotent policy refresh.

### Repeat: the extra work was not deterministic

The complete repeat finished in 24m20s with every exact-result check passing
and no execution-failed stage. Partial OPTIMIZE showed one pending file, not
seven; the subsequent PURGE observation window showed seven pending files
despite adding no files. Both VACUUM cases again physically deleted eight
objects and converged correctly. Default-query CPU ranged from 0 to 93.75 ms.

| Window | First matrix pending peak | Repeat pending peak | New files |
| --- | --- | --- | --- |
| Partial OPTIMIZE | 7 | 1 | 1 |
| DELETE/PURGE | 0 | 7 | 0 |

These are **windowed observations, not proof of which operation caused the
extra work**. QAP details and statistics are separate, non-atomic reads; a
reported older QAP version or delayed work from the preceding operation must
not be silently discarded or misattributed. The runner retains observed state
versions and flags excess pending work, including prior-version samples,
without claiming per-file ingestion provenance.

The repeat's overall result was `inconclusive`; its source-repoint stage was
an explicit `finding`. An overall inconclusive result does not erase
individual findings or establish no reingestion.

### Source repoint is an explicit reset boundary

Native Kusto rejected a connection-string change while QAP remained enabled:
"Cannot alter the connection string ... as it has query_acceleration policy
enabled." The old exact result remained intact. **Lower-version adoption
without a reset was therefore not exercised**, rather than silently repaired.

A separate owned-resource probe explicitly removed the QAP policy, changed
the source and re-enabled QAP. It converged from the old source's version 12 /
3,072 rows to the new source's version 10 / 2,560 rows, with exact parity in
both query lanes. This is an intentional new-cache workflow, not evidence that
ordinary maintenance needs a full reload.

The initial matrix failed at that native guard, after the ten cases above
completed. The runner now recognizes only that specific structured rejection
as a finding and verifies the unchanged old result; unrelated HTTP 400s and
wrong query results still fail. It does **not** automatically reset QAP to hide
the guard. Every completed run/probe cleaned only its owned resources.

## Run the experiment

This is a deterministic scenario runner, **not a loadtest**. Start in a
devcontainer with the private `.env`, Azure sign-in and the
[provisioned sandbox](../operations/AZURE_SANDBOX.md).

```bash
npx nx run azure-qap-experiment:run -- --config experiments/azure-qap/rapid.json --allow-immediate-vacuum
```

Each run declares its own database, data/checkpoint containers and reader
permissions on the existing cluster/account/identity. It does not reset the
developer's sandbox, launch a Flight deployment, build container images or run
the benchmark. Fixture appends use the production
`BufferManager -> DeltaPublicationSink -> BatchedTable` path in-process.

The cluster remains shared: do not stop/reprovision it during a run. Sandbox
reset deliberately refuses unexpected databases/containers; finish the
experiment's manifest-scoped cleanup first.

The rapid profile deliberately allows **zero-retention VACUUM** on those isolated
resources. This can break readers of removed files, including QAP catching up to
an older Delta snapshot. The ordinary maintenance default remains seven days;
shorter production retention requires its own explicit acknowledgement.

## What each scenario establishes

| Scenario | Actual source mutation | Question |
| --- | --- | --- |
| Idle control | None; continuously observe a separate, untouched table | Is an apparent ingestion change background activity? |
| Known full-reload control | Drop/recreate external definition and QAP over unchanged Delta files | Can the diagnostics detect a cache reset that we deliberately caused? |
| Small append | Add new rows/files through ArrowCol's publication path | Are unchanged files reingested along with the new files? |
| Partial OPTIMIZE | Replace small files in one closed partition; preserve other files | Are unrelated partitions reingested? |
| Broad OPTIMIZE | Replace files across multiple closed partitions | Is new acceleration explained by replacement files rather than unchanged data? |
| Old-partition DELETE/PURGE | Commit removes for an existing expired Hive partition | Does removing rows cause unrelated source ingestion? |
| Physical VACUUM LITE | Delete tombstoned objects without changing the active snapshot | Does physical deletion affect a caught-up QAP reader? |
| Metadata-only refresh | Reapply the same QAP policy | Does an idempotent policy update reset acceleration? |
| Concurrent appends/maintenance | Prove new appends overlap OPTIMIZE/PURGE/VACUUM | Do publication, maintenance and the final exact snapshot remain correct? |
| Immediate VACUUM/lagging reader | OPTIMIZE/PURGE followed immediately by physical deletion | Can QAP still read its older snapshot before catching up? |
| Source repoint/version downgrade | Repoint an external definition to a fresh, lower-version Delta source | Does QAP follow the new generation rather than retaining stale rows? |

Every mutating scenario must prove it performed work. A no-op OPTIMIZE, empty
PURGE, VACUUM that deleted nothing or writer that finished before maintenance
does not count as coverage. Maintenance dry-runs and applies independently plan
their work; the independent Rust Delta scan verifies the result.

## Read the evidence

| Evidence | What it means | What it does not prove |
| --- | --- | --- |
| Files added/removed/unchanged and bytes | Exact immutable source-file inventory before/after | Whether Kusto actually ingested any particular file |
| Exact row-multiset parity | Kusto's rows, values and duplicates match the frozen Delta version | That the result was served entirely from QAP |
| Exact QAP version, row count, health, pending count and completion | QAP reports convergence to that source snapshot | That it reached the snapshot without rebuilding its cache |
| Pending-file peak | Largest value observed in the sampled interval | That no shorter pending/reload event occurred between samples |
| Last-ingestion timestamp movement | Some reported ingestion activity changed | Which source files were ingested, or how much CPU it used |
| Query CPU and remote-download counters, when supplied | Cost/fallback diagnostics for that query | Background QAP ingestion CPU |
| End-to-end QAP catch-up time | Mutation, oracle scan and observed convergence elapsed time | Pure ingestion time or a throughput benchmark |
| Complete source-origin ingestion history, distinct from merges | Which unchanged files were genuinely reingested | Available evidence when only aggregate public statistics are returned |

The ordinary query and `external_table(..., timespan(1s))` freshness lane both
undergo exact row comparison. The freshness argument is **not asserted to force
a cache bypass**; only returned download diagnostics establish observed remote
reads. Missing measurements stay `null`/`n/a`, never zero.
The table's query CPU is the default lane; freshness-lane CPU is retained in
JSON rows. Neither measures background QAP ingestion CPU.

Reingesting OPTIMIZE's **new replacement files** is expected work. Reingesting
files that remain unchanged is a different finding. A broad rewrite of every
file can legitimately require acceleration of all replacement data without
establishing that unrelated unchanged data was reingested.

## Limits and verdicts

The public diagnostic adapter does not provide complete per-source ingestion
history or attributable background CPU. It therefore cannot award a
no-reingestion PASS, even if every correctness check succeeds. A positive
full-reload control must be detected through adequate ingestion evidence before
absence of such events can support a negative conclusion.

Transient reader failures after immediate VACUUM remain findings even if QAP
eventually converges. Final wrong rows or an unexecuted/failed mutation are
failures, not an inconclusive ingestion verdict.

| Direct runner exit code | Meaning |
| --- | --- |
| `0` | Passed with sufficient calibrated evidence |
| `1` | Execution or exact-result failure |
| `2` | Inconclusive evidence, not proof of no reingestion |
| `3` | Behavioral finding |
| `130` | Interrupted |

Nx normalizes any nonzero runner exit to `1`, including scientific findings and
inconclusive results. Inspect `report.status` and the individual stages, not
just the Nx exit code. Aggregate precedence is
`failed > inconclusive > finding > passed`, so a report can be inconclusive
overall while preserving specific findings.

Private service flags, unsupported schema/deletion-vector cases and synthetic
Delta-log edits are not presented as exercised scenarios.

## Reports and cleanup

The terminal prints the report directory under `.temp/experiments/azure-qap/`.
It contains fsynced `evidence.jsonl`, atomic `report.json`/`report.md`, and an
`ownership.json` manifest. The report retains individual stage outcomes,
unexecuted stages, request IDs, observations and reader errors.

Cleanup runs automatically after work has settled. If interrupted or blocked,
preview and execute cleanup using that run's manifest:

```bash
npx nx run azure-qap-experiment:cleanup -- --manifest <report-directory>/ownership.json
npx nx run azure-qap-experiment:cleanup -- --manifest <report-directory>/ownership.json --execute
```

Cleanup verifies ownership, fences publisher authority and refuses deletion
while a publisher is active or uncertain. It deletes only the run's database,
containers, reader assignment and deployment record. It never uses the sandbox
reset command or deletes the shared cluster/account/identity.

## References

- [Query acceleration policy](https://learn.microsoft.com/en-us/kusto/management/query-acceleration-policy?view=microsoft-fabric)
- [External Delta tables](https://learn.microsoft.com/en-us/kusto/management/external-tables-delta-lake?view=microsoft-fabric)
- [Experiment configuration and commands](../../experiments/azure-qap/README.md)
- [Maintenance retention and safety](../operations/DELTA_MAINTENANCE.md)
