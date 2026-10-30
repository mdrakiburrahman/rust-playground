# Benchmark exchange v1

`benchmark-v1.schema.json` is the canonical, closed-field JSON Schema.
`benchmark-v1.fixture.json` is synthetic, not a performance result. Consumers
copy both files unchanged and verify their hashes. No import authorizes commands.

Bound input to 32 MiB before parsing, depth to 16, and schema array limits.
Reject duplicate object keys, nonfinite numbers, unsupported versions, duplicate
record/run IDs, missing link/run/trace references and cross-run links. Parent
links must form an acyclic graph, remain in one clock domain, and enclose the
child interval. Derived records require `derived-from` links. An unavailable
measurement is not zero. A complete dataset cannot contain dropped events or
incomplete attempts.

Trace IDs reference a same-run request or attempt record; table IDs reference a
same-run table record. Resource records describe an interval, not an unexplained
cumulative counter: offset and duration refer to the local sampling interval.
CPU is a counter delta over that interval; memory is the observed gauge at its
end (a zero-duration record is a point sample). The finite exporter uses
container-scoped counters and Engine sampling timestamps for resources, distinct
from each service's monotonic clock. Neither supports per-request CPU allocation.
`complete` means structurally complete, not full service-stage measurement.
Reports must list unobserved required stages independently, using
`unobserved_stages` when present. Missing stage coverage is unknown, never proof
that all stages were measured.

`records` is a discriminated union using `kind`; links explicitly express
request/attempt/unit/batch/commit many-to-many relationships. Nanoseconds are
decimal strings; use BigInt for arithmetic, never subtract different clock
domains. A process restart creates a new clock ID. Wall-clock ordering is not
part of this contract. Links to asynchronous work are not enclosing parents.

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
