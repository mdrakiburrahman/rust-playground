# Delta maintenance

| Surface | Contract |
| --- | --- |
| Policy | Catalog TBLPROPERTIES override CLI/process defaults, then the [central registry](../reference/TABLE_PROPERTIES.md). Unknown `arrowcol.*` keys fail DDL validation. |
| Discovery | `TableDiscovery` supplies current table locations and properties. `FilesystemDiscovery` supports explicit local/cloud allowlists and bounded automatic local discovery. |
| Ordering | PURGE -> OPTIMIZE -> VACUUM -> checkpoint -> PRUNE-LOG, serially for each table; bounded parallelism across tables. |
| Coordination | Process-wide striped mutexes serialize passes for the same table URL, including overlapping service invocations. Delta commits and guarded storage still coordinate independent processes. |
| Schedule | `arrowcol.maintenance.interval` applies independently to each table. An error is not an idle success and does not trigger idle exponential backoff. |
| Observability | JSON results identify `negative_probe`; Prometheus `delta_maintenance_negative_probes_total` and outcome-labelled runs distinguish cheap noops from plans, work and errors. Equivalent counters are exported through OpenTelemetry. |

## Negative admission, before replay

Probes open the guarded storage transport only: no schema initialization, Delta commit, checkpoint or maintenance-record write. Complete bounded inventories are mandatory. Every negative proof is fenced by the entire log-object identity inventory before and after, not just its latest version. Inventory failure, cancellation, malformed metadata or concurrent log advancement fails explicitly rather than producing a success-shaped noop.

| Phase | Cheap negative proof |
| --- | --- |
| PURGE | No physical ingestion partition ends at or before the retention cutoff. Malformed or non-ingestion partition layouts require the normal planner or return an error. |
| OPTIMIZE | No eligible physical partition contains the configured minimum small files (at least two). The newest partition is excluded when `sealedPartitionsOnly` is true. Inactive physical files can force a full plan, but cannot authorize compaction. |
| VACUUM | Every physical data object is at or newer than the retention cutoff, no old log objects exist, and checkpoint debt is below the configured interval. This is a negative proof only; physical timestamps never authorize deletion. |
| Checkpoint | Latest log version minus checkpoint version is below `delta.checkpointInterval`. Unsupported multipart/v2 layouts go through the full path. |
| PRUNE-LOG | No checkpoint exists or no retained log object is older than log retention. Otherwise the existing checkpoint/history equivalence proof remains mandatory. |

Metadata-only filesystem property caches are fenced by log-object identity tokens, including checkpoint changes and same-version replacements. Retention probes are recomputed against the current clock every pass; they are never cached solely by version.

## Destructive safety

**PURGE removes logical data; VACUUM physically deletes files.** Keep readers within the retained history window. Table properties cannot bypass the seven-day VACUUM floor, retained-tombstone constraints or the storage publication/reset fence. CLI-only disposable-test acknowledgements remain invocation controls and are never persisted into catalog policy.

VACUUM plans and applies in one pass, reusing the independently validated history and active set only while the fresh Kernel snapshot version and metadata still match. Each deletion chunk rechecks current metadata, active references, cancellation and publication authority. Historical replay is repeated only when concurrent advancement requires it. Progress is written after durable deletions; an idle pass does not rewrite an existing same-version progress record. PRUNE-LOG retains its complete history/checkpoint proof before deleting any log.

The sorted-file and adaptive writer helpers consume the same resolved properties through `LayoutPolicy::from_properties` and `AdaptiveFileSizeConfig::from_properties`. Column names are also validated against the actual schema, and adaptive ratio/input bounds are checked before use.

## Commands and expected results

Run these inside the repository devcontainer on a leased devbox:

| Command | Expected result |
| --- | --- |
| `npx nx run arrowcol-props:generate-docs` | Regenerates the reference table from the runtime registry. |
| `npx nx run-many -t verify --projects=arrowcol-props,arrow-deltalake,delta-maintenance --parallel=1` | Registry, generated-document freshness, transport/planner safety and maintenance service checks pass. |
| `cargo test --locked -p delta-maintenance --test properties` | Exercises catalog-style policy overrides, negative probes, idle before/after state, retention boundaries and inventory failure. |
| `cargo run --locked -p delta-maintenance -- --root file:///tmp/disposable-tables/ --table events --operation optimize --operation vacuum` | Dry-run JSON only. Negative results leave table and maintenance metadata unchanged. |

For a bug bash, first create a disposable table using the catalog, capture the `_delta_log` and `_arrowcol_maintenance` file inventory, run two idle passes, and compare names, sizes, hashes and modification times. Set `arrowcol.optimize.minFiles` above the sealed partition's file count: no compaction should occur. Lower it to two and add two small files in an older partition plus one newest partition: only the sealed pair may compact. An unknown property or a retention below seven days must be rejected before table metadata changes.
