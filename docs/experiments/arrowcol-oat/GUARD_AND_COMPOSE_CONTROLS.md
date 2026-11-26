# Pressure thresholds versus exporter capacity

`collector.pressure.buffered_*_limit` values do **not** allocate exporter
capacity. Requiring them to be at least twice
`collector.batching.global_capacity` would change the shipped defaults (which
are equal in OAT) without a memory-safety justification. Smaller thresholds are
safe but may be deliberately poor throughput settings.

| Invariant | Implementation evidence |
| --- | --- |
| Exporter capacity remains independently bounded | [`Capacity::acquire`](../../../crates/arrowcol-dataflow-components/src/exporter.rs#L558) rejects an oversized batch and acquires separate byte, row and batch semaphores. [`CapacityReservation::drop`](../../../crates/arrowcol-dataflow-components/src/exporter.rs#L601) refunds reservations, including partial/cancelled acquisition. |
| Pressure controls routing admission rather than capacity accounting | [`RuntimeMetrics` pressure snapshot](../../../crates/arrowcol-dataflow-components/src/metrics.rs#L2000) compares observed buffer/backlog counters against thresholds and marks the collector non-accepting when exceeded. Lowering a threshold sheds new admission sooner; it does not enlarge or bypass a semaphore. |
| Previously admitted work does not need pressure to fall before flushing | [Exporter collection loop](../../../crates/arrowcol-dataflow-components/src/exporter.rs#L705) flushes on its independent `max_age`, size/schema boundaries and drain signal. This loop does not consult the pressure acceptance flag. Thus a low pressure threshold does not introduce a circular dependency between new admission and draining existing work. |
| Existing regression coverage protects cancellation and drain behavior | [`temporary_capacity_waits_and_cancellation_refunds_reservations`](../../../crates/arrowcol-dataflow-components/src/exporter.rs#L3548) exercises partial-acquisition refund and resumed capacity. [`graceful_drain_flushes_partial_publication`](../../../crates/arrowcol-dataflow-components/src/exporter.rs#L2977) exercises publication below normal batching thresholds. |

Safety is not a throughput guarantee: an undersized pressure threshold can
fragment incoming work, reduce routing availability or trip the overload guard
before a batch reaches its preferred size. Memory pressure also includes live
decoding/writer/runtime allocations not represented by the exporter queue.
Keep the existing positivity, representability, per-table/global capacity and
`buffered_bytes_limit <= rss_limit_bytes` checks; use observed pressure and
durable commit evidence to choose an experimental threshold rather than
silently raising it to an arbitrary multiple.

See [the tunable reference](TUNABLES.md#base-compose-environment-audit) for the
collector/client/maintenance base-Compose environment audit and fixed controls.
