# ArrowCol lakehouse loadtest

[![Complete local and Azure lakehouse loadtest](../../../loadtest-architecture.png)](../../../loadtest-architecture.html)

The [root architecture diagram](../../../loadtest-architecture.html) follows
the complete workload from catalog registration to independent validation.
The [collector-path detail](arrow-load-generator.html) and its
[PNG snapshot](arrow-load-generator.png) focus on generated Arrow and paired
publication. HTML sources and PNGs are kept together; PNGs are rendered with
a real browser, not exported from a different diagram.
The [snapshot receipt](../snapshots.json) records the browser version,
viewport widths, and source/PNG SHA-256 digests.

## Generated workload and publication

The generator registers 13 table schemas through Flight SQL, exports immutable
catalog artifacts, generates Rust and TypeScript SDKs, and constructs every
`RecordBatch` from the generated schema. Every schema includes explicit
`customer_id`; deterministic interleaved customers exercise tenant predicates
and sorted Parquet row groups.

The paired workload sends the same logical batch through the Pingora router
to separate Direct and Dataflow pools. Each pool owns independent Delta roots.
Direct polls SQL-backed receipts until publication or a structured terminal
failure. Dataflow reports best-effort admission by default, or durable admission
with native Quiver. Neither path deduplicates independent client submissions.

Delta Kernel publication writes data before atomically publishing the log.
Stable publication transaction identity protects the retry of that publication,
not two separate requests with identical content. Ingestion-time UTC partition
stamps survive retry and Quiver replay; loadtests use minute partitions.

## Modes and downstream semantics

`--mode local|azure` selects the storage and Kusto pair together; the default
comes from `ARROWCOL_MODE`, falling back to `local`.

| Surface | Local mode | Azure mode |
| --- | --- | --- |
| Source Delta | Local disk, separate pool roots | ADLS Gen2, separate pool roots |
| Kusto | Pinned emulator: native external Delta with Query Acceleration Policy | External Delta tables with native Query Acceleration Policy |
| Storage identity | Scoped local mounts | Existing Azure CLI writer identity; dedicated Kusto storage-read UAMI |
| Evidence | Functional integration, active-file visibility, recovery | Real QAP state, capacity, lag, correctness, and reacceleration |

The emulator is a functional integration surface, not an Azure performance
benchmark. Native external Delta/QAP is the default; an experimental bridge is
not an automatic fallback. The local policy uses `MaxAge=1min`, with a bounded
five-minute convergence gate after append, OPTIMIZE, delete, partition PURGE,
and VACUUM. Longer `MaxAge` can retain stale cached rows.

Cache statistics alone do not establish freshness: a reported 100% acceleration,
zero pending files, and recent update time can coexist with stale query results.
Require current `details.TableVersion`, healthy `details.IsHealthy`, expected
`details.AcceleratedRows`, and actual cached row-multiset equality with both the
independent oracle and uncached fallback. Record observed convergence separately
from the bounded deadline; focused native-probe acceptance does not prove the
integrated 13-schema workload or Explorer/stress acceptance.

Native Kusto follows the current active snapshot after asynchronous refresh,
including OPTIMIZE replacements and partition removals. A tenant fork has
intentionally different semantics: capture a snapshot, tail logical appends,
ignore OPTIMIZE, and retain already-copied rows after source purge.

Standalone maintenance plans and revalidates work before applying it. OPTIMIZE
rewrites real files with `dataChange=false`; PURGE removes existing expired
partitions; VACUUM LITE uses retained tombstones and keeps the ordinary seven-day
safety floor. Physical retention bounds recovery: missing required source history
or files must fail visibly rather than advancing a consumer checkpoint.

## Operations and qualification

Use the [loadtest CLI guide](../../../projects/loadtest/README.md) as the
canonical source for current commands, flags, prerequisites, lifecycle,
diagnostics, and evidence locations. Keeping operational examples there avoids
the obsolete duplicated CLI that previously lived in this architecture guide.

Run the repository checks inside the Linux devcontainer:

```bash
npx nx run rust:verify
npx nx run loadtest:verify
npx nx run-many -t verify --all --parallel=1
npx nx run hello-world:image-smoke
```

Qualification compares both pools across all schemas with an independent Delta
reader, committed snapshot-selected files, manifest hashes, row multisets,
partition metadata, routing ownership, and recovery evidence. The delta-rs
reader is a validation oracle, not the production transaction writer.

Metrics, logs, and traces feed LGTM/Grafana. Maintenance, stream/checkpoint, native QAP,
and publication signals must be observed live, not inferred from dashboard JSON.
Keep the optional OTEL baseline separate from Arrow Flight panels.

Azure configuration remains in a gitignored private `.env`; do not commit
subscription/resource values or credentials into diagrams, screenshots, logs,
or public evidence. Cloud checks are explicit and credential-gated: a skipped
check is not a pass. Bind measurements to the integrated source SHA and record
limitations. Stop the Azure cluster after live tests. Reset is preview-first;
public execution remains unconditionally disabled until distributed publisher
admission, uncertain-operation retention, fencing, and definitive drain are
enforced across every mutator. The intended explicit-execution path removes only
verified owned data and preserves the account, containers, cluster, and managed
identity. Preview or offline control-plane tests do not establish live reset safety.

See [storage contracts](../arrowcol-platform/storage.md),
[delivery guarantees](../arrowcol-platform/delivery-guarantees.md), and
[observability](../arrowcol-platform/observability.md) for the corresponding
platform boundaries.
