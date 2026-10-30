# Benchmark exchange v1

`benchmark-v1.schema.json` is the canonical, closed-field JSON Schema.
`benchmark-v1.fixture.json` is synthetic, not a performance result. Consumers
copy both files unchanged and verify their hashes. No import authorizes commands.
Per-file `-text` attributes preserve the shared physical bytes, including CRLF,
for the schema, fixture and acceptance corpus. Do not normalize them on checkout
or export.

Bound input to 32 MiB before parsing, depth to 16, and schema array limits.
Reject duplicate object keys, nonfinite numbers, unsupported versions, duplicate
record/run IDs, missing link/run/trace references and cross-run links. Parent
links must form an acyclic graph, remain in one clock domain, and enclose the
child interval. Derived records require `derived-from` links. An unavailable
measurement is not zero. A complete dataset cannot contain dropped events or
incomplete attempts.
`incomplete_attempts` counts distinct attempts, not the number of missing
surfaces: unresolved completion plus missing stages still counts as one attempt.

Trace IDs reference a same-run request or attempt record; table IDs reference a
same-run table record. Resource records describe an interval, not an unexplained
cumulative counter: offset and duration refer to the local sampling interval.
CPU is a counter delta over that interval; memory is the observed gauge at its
end (a zero-duration record is a point sample). The finite exporter uses
container-scoped counters and observer-local monotonic receive timestamps for resources, distinct
from each service's monotonic clock. Neither supports per-request CPU allocation.
`complete` means structurally complete, not full service-stage measurement.
Reports must list unobserved required stages independently, using
`unobserved_stages` when present. Missing stage coverage is unknown, never proof
that all stages were measured.

The finite exporter expresses service-span membership through `trace_id`, without
duplicating it as attempt-to-span `contains` links. Compact dataset-local aliases
and removal of those redundant edges keep the measured cohort within the unchanged
32 MiB limit; all retained span identities, intervals, outcomes and causal
parent/asynchronous edges remain present. Consumers must use the record's
`trace_id` for membership rather than require a redundant containment edge.

`records` is a discriminated union using `kind`; links explicitly express
request/attempt/unit/batch/commit many-to-many relationships. Nanoseconds are
decimal strings; use BigInt for arithmetic, never subtract different clock
domains. A process restart creates a new clock ID. Wall-clock ordering is not
part of this contract. Links to asynchronous work are not enclosing parents.

Logical units are run/table-scoped across attempts. A physical commit is one
run/table/version with one digest, even when several units or observations
reference it. Conflicting digests for that identity are invalid. Repeated
receipt observations remain separate; sharing a commit never duplicates its
duration or allocates its whole cost to each request. Failed, cancelled and
timed-out transport terminals remain distinct from eventual receipt completion.

Processing spans are inclusive elapsed observations, not necessarily exclusive
local work or CPU allocation. A processing span can contain downstream waits.
Gateway lifetime includes downstream wait. Do not sum parents and children, concurrent intervals,
percentiles, or a shared commit once for each linked request. Process resource
samples remain process-scoped. Estimated values carry an explicit assumption;
they never replace measured values.

IDs must be dataset-local aliases. Exporters construct an allowlisted projection,
not a recursive copy of runtime state. Do not encode secrets, user/tenant names,
payloads, arbitrary text, URLs, paths, command lines or raw errors in IDs.
Descriptions, error classes, stages and metric methods are bounded vocabulary.
Unknown schema versions are rejected; v1 has no implicit minor-version extension.

New measured cohorts explicitly record `measurement: real-sql-delta` in their
private revision metadata and bind exact frozen source/shared binaries by hash.
Only completed runs with independent ordinary-Delta multiset reconciliation and
SQL receipt artifacts can export as nonsynthetic measurements. Their workloads
are still synthetic finite marker inputs, labeled in methodology and reports;
the canonical closed description remains `finite-local-comparison`.
Historical exports without this provenance retain their prior synthetic label;
the exporter does not relabel old evidence.

`delta_report.generate_fresh` additionally verifies the complete six-run,
alternating-order, nine-phase cohort and all four same-harness tracing
calibrations before producing a separate sanitized public package. Its
calibration/provenance summary carries physical exchange/manifest hashes,
actual postprocessor hashes, exact common binary identities, bounded resource
configuration and observed coverage. It does not import historical report
qualification claims. Counter-reset CPU intervals remain unavailable with
retained dropped/error counts; valid memory gauges are not discarded.
