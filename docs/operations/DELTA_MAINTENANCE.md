# Delta maintenance

| Surface | Contract |
| --- | --- |
| Policy | Catalog TBLPROPERTIES override CLI/process defaults, then the [central registry](../reference/TABLE_PROPERTIES.md). Unknown `arrowcol.*` keys fail DDL validation. |
| Discovery | `--catalog-endpoint http://catalog:8817` uses `arrowcol-catalog-client`'s generated GetSnapshot RPC: one atomic catalog snapshot per pass, shared by all workers. `FilesystemDiscovery` supports explicit local/cloud allowlists and bounded automatic local discovery. |
| Ordering | PURGE -> OPTIMIZE -> VACUUM -> checkpoint -> PRUNE-LOG, serially for each table; bounded parallelism across tables. |
| Coordination | Process-wide striped mutexes serialize passes for the same table URL, including overlapping service invocations. Delta commits and guarded storage still coordinate independent processes. |
| Pass inventory | Each table pass reuses bounded log, checkpoint-hint and physical-file observations only while the full probe-policy fingerprint (including the pass time anchor) is unchanged. Successful negative proofs are fenced by fresh log-object identity; policy changes, a new pass anchor, or entry into a full planner invalidates the inventory. Unknown or incomplete probes fall back to reconciliation, never deletion. |
| Schedule | `arrowcol.maintenance.interval` applies independently to each table. An error is not an idle success and does not trigger idle exponential backoff. |
| PURGE retries | Changed expired-partition membership invalidates the entire plan; bounded replans share one end-to-end attempt/time budget across preparation, backoff and publication. Reports distinguish commit retries/collisions from membership replans. Metadata changes remain hard failures, and partitions are never split. |
| OPTIMIZE upload | `--optimize-upload-mode` / `ARROWCOL_MAINT_OPTIMIZE_UPLOAD_MODE` is strictly `put_create` (default, atomic create-only) or `buffered_multipart` (opt-in, UUID-collision-only and unqualified; do not use in scored cells). `--optimize-upload-buffer-bytes` / `ARROWCOL_MAINT_OPTIMIZE_UPLOAD_BUFFER_BYTES` applies only to buffered mode, defaults to 8 MiB, and accepts 4..=64 MiB. Successful upload shutdown precedes Add publication; a failed/uncertain multipart completion may leave an orphan. `BufWriter` concurrency 1 does not cap final-part overlap. |
| Observability | JSON results identify `negative_probe` and report total `duration_ms` plus scoped `probe_duration_ms` (log/property validation and negative probe) and `plan_apply_duration_ms` (full table open, planning and apply). Prometheus `delta_maintenance_negative_probes_total` and outcome-labelled runs distinguish cheap noops from plans, work and errors. Equivalent counters are exported through OpenTelemetry. |
| Compaction debt | OPTIMIZE reports `compaction_debt` with the pass anchor (`observed_at_ms`), inventory source, eligible file/byte totals, bounded backlog totals and, for ingestion partitions, the oldest eligible partition start. Present evidence with zero counts is a measured zero; `null` or a missing report is unavailable, never zero. Incomplete bounded inventories fail instead of emitting a zero. |
| Due work | `MaintenanceReport.maintenance_due_work` is a per-operation, bounded observation. Missing/null is UNKNOWN; only complete evidence may report exact zero. Scan limits and incomplete plans never become zero or backlog. |
| Startup readback | `arrowcol_maintenance_effective_config.optimize_upload` reports mode, requested buffer, and effective buffer; `null` means inactive for `put_create`, while `buffered_multipart` defaults to 8 MiB. `startup.operation_limits` retains the exact parsed consumer values. |
| Parquet writer | Optional OAT `maintenance.parquet_writer` is role-scoped: absent inherits the global writer; present replaces the maintenance writer only. Startup `parquet_writer_overrides` reports the maintenance process settings, and an omitted writer row-group limit preserves `output_row_group_rows`. |

## Due-work evidence

`maintenance_due_work` uses snake_case JSON and is attached to each operation report. It is one observation, not a sustained-window verdict. Campaign qualification requires one complete observation per enabled operation in every contiguous 30-second window for the same run/table/process; missing, incomplete, failed, stale, dropped, or out-of-window observations remain UNKNOWN with no carry-forward.

| Field | Type / units | Meaning |
| --- | --- | --- |
| `observed_at_ms` | `i64`, UTC Unix milliseconds | Operation observation/pass anchor. |
| `source` | enum | `snapshot_active_files`, `bounded_physical_inventory`, `bounded_tombstone_replay`, `checkpoint_log_inventory`, `bounded_log_inventory`, or `verified_prune_proof`. |
| `completeness` | `complete` / `incomplete` | Whether the bounded authoritative source covers the complete eligible set. |
| `unit` | enum | `parquet_files`, `tombstones`, `commits`, or `log_objects`; applies to eligible/backlog counts. |
| `due` | nullable boolean | Exact configured due condition; it may be false while a threshold-based operation has nonzero eligible count. |
| `eligible_count`, `eligible_bytes` | nullable `u64` | Exact eligible work before action caps; bytes are null unless the planner measures them. |
| `backlog_count`, `backlog_bytes` | nullable `u64` | Eligible work omitted only by an action cap; never use these for a truncated scan. |
| `oldest_eligible_timestamp_ms` | nullable UTC Unix milliseconds | Timestamp, not age; basis is `ingestion_partition_start`, `delta_remove_timestamp`, or `log_object_last_modified`. |
| `incomplete_reason` | nullable enum | `active_file_limit`, `physical_listing_limit`, `log_listing_limit`, `malformed_partition`, `unsupported_layout`, `missing_checkpoint_proof`, `undated_tombstones`, `stale_identity`, `operation_failed`, or `cancelled`. |

A complete observation has exact `due` and `eligible_count`; optional byte/backlog/oldest fields remain null unless authoritatively measured. An incomplete observation has null `due`, eligible/backlog counts and bytes, and oldest timestamp, plus an `incomplete_reason`. A missing `maintenance_due_work` value or a top-level operation error without a report is UNKNOWN, never zero. A scan bound, malformed layout, stale identity, or missing proof cannot be relabeled as action-cap backlog.

The wire path is `OperationResult.report.maintenance_due_work`. Successful wrappers omit `error` and `error_kind`; failures can retain pre-action evidence. An error, `report.interrupted=true`, or `report.outcome=partial` makes eligibility UNKNOWN even when retained evidence says `complete`. The idle service test emits `maintenance_due_work_serde_fixtures=` with all five real operation wrappers and explicitly constructed error/interrupted/partial/missing-report qualification cases for reader compatibility checks.

| Operation | Authoritative source and unit |
| --- | --- |
| PURGE | Active-file plan or bounded physical inventory; `parquet_files`. |
| OPTIMIZE | Active-file plan or bounded physical inventory; `parquet_files`. |
| VACUUM | Bounded tombstone replay; `tombstones`. Physical object age cannot prove tombstone eligibility. |
| CHECKPOINT | Checkpoint/log inventory; `commits`. |
| PRUNE-LOG | Bounded log inventory or verified prune proof; `log_objects`. |

## Negative admission, before replay

Probes open the guarded storage transport only: no schema initialization, Delta commit, checkpoint or maintenance-record write. Complete bounded inventories are mandatory. Every negative proof is fenced by the entire log-object identity inventory before and after, not just its latest version. Inventory failure, cancellation, malformed metadata or concurrent log advancement fails explicitly rather than producing a success-shaped noop.

| Phase | Cheap negative proof |
| --- | --- |
| PURGE | No physical ingestion partition ends at or before the retention cutoff. Names that cannot decode or strictly parse at the configured granularity defer to the normal planner, which skips them and reports incomplete `malformed_partition` evidence (UNKNOWN, not zero debt). Non-ingestion layouts require the normal planner. |
| OPTIMIZE | No eligible physical partition contains the configured minimum small files (at least two). Ingestion partition names must decode and parse at the configured granularity; malformed names defer to the full planner and remain incomplete, even below the file-count threshold. The newest ingestion partition is excluded when `sealedPartitionsOnly` is true; categorical/multi-column Hive partitions conservatively use the full planner. Inactive physical files can force a full plan, but cannot authorize compaction. |
| VACUUM | Always defers to bounded tombstone replay. Physical object timestamps cannot establish tombstone eligibility or prove zero debt. |
| Checkpoint | Latest log version minus checkpoint version is below `delta.checkpointInterval`. Unsupported multipart/v2 layouts go through the full path. |
| PRUNE-LOG | No retained log object is older than log retention. Otherwise the existing checkpoint/history equivalence proof remains mandatory; missing proof is incomplete. Full apply preserves the plan's due-work evidence, including no-work exits. |

Metadata-only filesystem property caches are fenced by log-object identity tokens, including checkpoint changes and same-version replacements. Retention probes are recomputed against the current clock every pass; they are never cached solely by version.

Writer and maintenance must resolve the same `arrowcol.partition.granularity` policy. A partition column alone does not persist that policy: when metadata has no value, separate writer and maintenance process defaults can disagree. Minute (`YYYY-MM-DD-HH-MM`), hour (`YYYY-MM-DD-HH`) and day (`YYYY-MM-DD`) values are parsed only at the resolved granularity; maintenance never guesses another format or truncates a timestamp to make a mismatched policy pass. Align the producer and maintenance policy rather than treating skipped partitions as healthy idle work.

## Destructive safety

OPTIMIZE decoded-input `resource_limit` diagnostics name `max_batch_bytes` (one decoded batch) or `max_decoded_bytes_per_bin` (cumulative decoded input for the bin), with `measured_bytes` and `allowed_bytes`. If both are exceeded, the batch bound is reported first. These diagnostics do not change either cap.

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
