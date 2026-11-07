# Delta maintenance

| Surface | Contract |
| --- | --- |
| Policy | Catalog TBLPROPERTIES override CLI/process defaults, then the [central registry](../reference/TABLE_PROPERTIES.md). Unknown `arrowcol.*` keys fail DDL validation. |
| Discovery | `--catalog-endpoint http://catalog:8817` uses `arrowcol-catalog-client`'s generated GetSnapshot RPC: one atomic catalog snapshot per pass, shared by all workers. `FilesystemDiscovery` supports explicit local/cloud allowlists and bounded automatic local discovery. |
| Ordering | PURGE -> OPTIMIZE -> VACUUM -> checkpoint -> PRUNE-LOG, serially for each table; bounded parallelism across tables. |
| Coordination | Process-wide striped mutexes serialize passes for the same table URL, including overlapping service invocations. Delta commits and guarded storage still coordinate independent processes. |
| Schedule | `arrowcol.maintenance.interval` applies independently to each table. An error is not an idle success and does not trigger idle exponential backoff. |
| Observability | JSON results identify `negative_probe`; Prometheus `delta_maintenance_negative_probes_total` and outcome-labelled runs distinguish cheap noops from plans, work and errors. Equivalent counters are exported through OpenTelemetry. |

## Negative admission, before replay

Probes open the guarded storage transport only: no schema initialization, Delta commit, checkpoint or maintenance-record write. Complete bounded inventories are mandatory. Every negative proof is fenced by the entire log-object identity inventory before and after, not just its latest version. Inventory failure, cancellation, malformed metadata or concurrent log advancement fails explicitly rather than producing a success-shaped noop.

| Phase | Cheap negative proof |
| --- | --- |
| PURGE | No physical ingestion partition ends at or before the retention cutoff. Malformed or non-ingestion partition layouts require the normal planner or return an error. |
| OPTIMIZE | No eligible physical partition contains the configured minimum small files (at least two). The newest ingestion partition is excluded when `sealedPartitionsOnly` is true; categorical/multi-column Hive partitions conservatively use the full planner. Inactive physical files can force a full plan, but cannot authorize compaction. |
| VACUUM | Every physical data object is at or newer than the retention cutoff, no old log objects exist, and checkpoint debt is below the configured interval. This is a negative proof only; physical timestamps never authorize deletion. |
| Checkpoint | Latest log version minus checkpoint version is below `delta.checkpointInterval`. Unsupported multipart/v2 layouts go through the full path. |
| PRUNE-LOG | No checkpoint exists or no retained log object is older than log retention. Otherwise the existing checkpoint/history equivalence proof remains mandatory. |

Metadata-only filesystem property caches are fenced by log-object identity tokens, including checkpoint changes and same-version replacements. Retention probes are recomputed against the current clock every pass; they are never cached solely by version.

## Destructive safety

**PURGE removes logical data; VACUUM physically deletes files.** Keep readers within the retained history window. Table properties cannot bypass the seven-day VACUUM floor, retained-tombstone constraints or the storage publication/reset fence. CLI-only disposable-test acknowledgements remain invocation controls and are never persisted into catalog policy.

VACUUM plans and applies in one pass, reusing the independently validated history and active set only while the fresh Kernel snapshot version and metadata still match. Each deletion chunk rechecks current metadata, active references, cancellation and publication authority. Historical replay is repeated only when concurrent advancement requires it. Progress is written after durable deletions; an idle pass does not rewrite an existing same-version progress record. PRUNE-LOG retains its complete history/checkpoint proof before deleting any log.

New writer publications resolve current Delta metadata through `LayoutPolicy::from_properties` and `AdaptiveFileSizeConfig::from_properties`. Table values override validated process defaults. Sort/Bloom column names must exist in the actual schema and must not be partition columns: partition columns are omitted from the physical Parquet schema. Unsupported partition-column layout requests fail explicitly. Generated ingestion granularity must not replace a catalog-declared partition specification.

**Adaptive grouping, not file splitting:** the seven `arrowcol.writer.*` properties govern the existing DFE publication-group scheduling estimator. `targetFileSize / estimatedParquetRatio`, clamped by `minInputSize` and `maxInputSize`, determines when queued Arrow bytes trigger publication. Successful durable file observations update the bounded EWMA; failed commits do not install observations. Table properties override `file_batching.adaptive_file` process defaults and refresh on the table IO worker without a catalog RPC per admitted batch. A changed effective configuration starts a cold estimate; unchanged configuration retains learned observations.

Each admitted publication (and each declared partition key within it) retains its own replay-safe file and transaction identity. Grouping changes commit scheduling, not file boundaries: `targetFileSize` is not a guarantee of individual Parquet file size. Adaptive splitting of durable publications was not a baseline capability and is not introduced here. OPTIMIZE's separately supported output target remains unchanged.

| Baseline `628e5131` evidence | Supported effect preserved and centrally governed |
| --- | --- |
| `crates/arrowcol-dataflow-components/src/config.rs` lines 637-679; `crates/arrowcol-dataflow-components/src/exporter.rs` lines 252 and 634-639 | All seven process controls already populated the estimator and triggered group flush through `should_flush(bytes)`. |
| `crates/arrowcol-dataflow-components/src/exporter.rs` lines 730-776 | One file per admitted publication; staged Parquet observations became the live estimate only after successful DFE commit acknowledgements. |
| Retired generic Direct buffer implementation | DFE staging preserves each admitted publication's file and transaction identity across retries; table overrides govern only its downstream publication-group scheduler. |

`cargo test --locked -p arrowcol-dataflow-components --lib exporter_refreshes_catalog_adaptive_grouping_without_changing_publication_identity` proves table-over-process precedence, a live same-owner high-to-low threshold update, durable completion of independent receipt files and replay without duplicate additions.

Refresh applies to new publications. An already-staged publication retains its file plan and transaction identity during recovery; changing layout/granularity never changes a replay identity midway through a retry.

| Property group | Runtime consumer / observable acceptance |
| --- | --- |
| `arrowcol.maintenance.enabled`, `.interval`, `.operationTimeout` | Effective operation selection, policy-aware per-table schedule and cancellation deadline. Changed policy is reconsidered before the old interval expires; errors are retryable, not idle. |
| `arrowcol.optimize.enabled`, `.minFiles`, `.targetFileSize`, `.smallFileSize`, `.sealedPartitionsOnly`, `.latestPartitions` | Effective OPTIMIZE config and bin filtering. Real catalog `minFiles=3 -> 2` change switches from unchanged idle state to a two-file sealed-partition commit; newest ingestion partition remains untouched. |
| `arrowcol.optimize.max*`, `.batchRows` | Existing planner/decoder/Parquet/action limits receive resolved values; effective JSON config exposes exact bounds and violations fail explicitly. |
| `arrowcol.vacuum.enabled`, `.retention`, `.allowShortRetention`, `.maxDeletes`, `.maxListedFiles` | Full retention authorization and bounded delete/probe config. Retention equality is not expired; incomplete inventories fail with no mutation. Persisted policy cannot lower the seven-day floor. |
| `arrowcol.purge.enabled`, `.retention`, `arrowcol.partition.granularity` | Logical ingestion cutoff and PURGE plan before compaction; generated writer partition formatting uses current metadata policy. Declared catalog partition keys remain authoritative. |
| `arrowcol.layout.enabled`, `.sortColumns`, `.bloomColumns`, `.maxRows`, `.maxInputSize`, `.rowGroupRows`, `.pageRows` | Sorted writer/OPTIMIZE layout config; output sorting, Bloom metadata, row-group limits and bounded buffering are observable. Invalid columns or bounds fail before commit. |
| `arrowcol.writer.targetFileSize`, `.minInputSize`, `.maxInputSize`, `.coldStartParquetRatio`, `.ewmaWeight`, `.minParquetRatio`, `.maxParquetRatio` | Live table policy drives DFE group-flush threshold and successful-file EWMA observations. Real pending-versus-durable receipt assertions prove table precedence and dynamic updates without changing replay identity. No individual-file size guarantee. |
| `delta.targetFileSize`, `.deletedFileRetentionDuration`, `.logRetentionDuration`, `.checkpointInterval`, `.appendOnly` | Target fallback, tombstone/log cutoffs, checkpoint debt and strict boolean syntax. Syntax validation does not authorize a protocol/feature change. |

## Retained history and pruning proof

VACUUM requires contiguous JSON commits `0..=version`, or a durable log floor `F` with its checkpoint present and covered by the VACUUM progress record, followed by contiguous JSON `F+1..=version`. Any other gap fails explicitly. Traversal and external data paths are rejected. Cancellation mid-delete reports partial progress.

The progress record `_arrowcol_maintenance/vacuum-lite.json` is `{table_id, version, cutoff_ms}`: every remove in the replayed history before that cutoff was physically deleted or confirmed absent. A partial, cancelled, dry-run or backlogged pass never writes this claim. The record is table-bound and monotonic; local writes use exclusive locking, a temporary file, fsync and rename, while cloud writes are create-if-absent or ETag-conditional.

A PRUNE-LOG plan chooses a classic checkpoint `C` only when all of these conditions hold:

1. The checkpoint exists, `_last_checkpoint` points to a version at least `C`, and no unsupported multipart, v2 or compacted log object exists at or below it.
2. Independent JSON replay through `C` agrees with both a direct Parquet checkpoint read and Delta Kernel loading `C`: table id, every active path with partition values and size, protocol, schema, partition columns, configuration, and every application transaction. Advisory statistics and serialized byte encodings are not compared. Transaction-retention settings are refused.
3. The progress record has the same table id and covers `C`; every remove through `C` has a deletion timestamp strictly before its progress cutoff. Checkpoints may drop expired tombstones, so this prevents pruning from hiding unvacuumed files.
4. Every deleted JSON commit below `C` is older than log retention using the later of commit/in-commit time and object modification time. Every deleted older checkpoint also passes the modification-time cutoff. `C.json` is retained for append compatibility, though its removes still satisfy the previous rule.

Apply re-proves the plan against fresh state, checks publication authority, and durably writes `_arrowcol_maintenance/log-floor.json` before deleting anything. The checkpoint, `C.json`, `_last_checkpoint` and newer objects are retained. Deletions are ordered and bounded by `--max-prune-files`; `NotFound` counts as already absent, and cancellation reports partial progress. A later pass can finish leftovers below an already-durable floor after re-verifying that floor with Kernel.

Malformed checkpoint hints, record/checkpoint disagreement, unsupported history or missing remove timestamps fail closed. A hint below the durable floor produces a no-work plan rather than dropping more history. Shorter log retention requires the CLI-only marked disposable-test override; acknowledging short VACUUM reader risk does not authorize shorter log retention.

**Log-size limitation:** pruning bounds historical JSON/checkpoint object count, not total log bytes. Checkpoints retain publication and maintenance application transactions forever; transaction retention is deliberately disallowed. The newest checkpoint can therefore grow with publication count. Only classic single-file checkpoints support pruning.

## Final-compaction evidence and lifecycle

The explicit one-shot `--sealed-final --operation optimize` policy bypasses only newest/open partition churn protection, not schema, retention, publication or byte/action/decoding limits. It is not a verified publisher-release receipt or reader lease. The CLI still rejects scheduled, mixed-operation or other-operation uses of this flag. A table's `arrowcol.optimize.sealedPartitionsOnly=false` is likewise an explicit churn policy, never a storage authority override.

Final-compaction runs retain their full typed OPTIMIZE plan and effective configuration even for an empty result: the cheap negative shortcut is deliberately not used for that lifecycle gate. A dry-run empty final plan reports `planned`; an apply reports `noop` and writes no commit. Zero output files alone prove neither emptiness nor success.

Lifecycle orchestration must require one successful, error-free result per expected table with `optimize.plan.bins=[]`, no backlog and no skipped/protected partitions, and verify the expected policy, table/root/run/SDK bindings and current versions. Missing reports and failures are not noops. Writer release and clean mutator-drain evidence must be established independently before compaction; maintenance processes must be joined before capturing immutable reader snapshots.

OPTIMIZE uses `dataChange=false` and preserves exact rows, schema, partition values, publication identities and retained history. JSON `optimize.config` includes resolved policy and limits, `optimize.plan` contains the input bins/read version, and `report.outputs` describes actual outputs only after successful rewriting. Planned evidence is retained when apply fails. Neither one file per partition nor external cache-convergence time is guaranteed.

## Commands and expected results

Run these inside the repository devcontainer on a leased devbox:

| Command | Expected result |
| --- | --- |
| `npx nx run arrowcol-props:generate-docs` | Regenerates the reference table from the runtime registry. |
| `npx nx run-many -t verify --projects=arrowcol-props,arrow-deltalake,delta-maintenance --parallel=1` | Registry, generated-document freshness, transport/planner safety and maintenance service checks pass. |
| `cargo test --locked -p delta-maintenance --test properties` | Exercises catalog-style policy overrides, negative probes, idle before/after state, retention boundaries and inventory failure. |
| `cargo run --locked -p delta-maintenance -- --root file:///tmp/disposable-tables/ --table events --operation optimize --operation vacuum` | Dry-run JSON only. Negative results leave table and maintenance metadata unchanged. |
| `cargo test --locked -p delta-maintenance --test properties real_catalog_ddl_discovery_refreshes_policy_without_idle_writes` | Real Rust catalog + Flight SQL DDL + gRPC discovery: two idle passes preserve file bytes/mtimes; stale policy is rejected; changing `minFiles` from three to two compacts only the sealed pair. |

For a bug bash, first create a disposable table using the catalog, capture the `_delta_log` and `_arrowcol_maintenance` file inventory, run two idle passes, and compare names, sizes, hashes and modification times. Set `arrowcol.optimize.minFiles` above the sealed partition's file count: no compaction should occur. Lower it to two and add two small files in an older partition plus one newest partition: only the sealed pair may compact. An unknown property or a retention below seven days must be rejected before table metadata changes.

Catalog policy is refreshed each scheduling pass. Changes to typed policy immediately reconsider a table's interval instead of retaining its previous deadline; failed passes remain retryable. Catalog history and purge bookkeeping are not user policy, but changed registered tunables must match current Delta metadata before a negative probe can report success.
