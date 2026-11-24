# Single-collector isolation measurements

| Command | Input | Output |
| --- | --- | --- |
| `sample` | One running production DFE, one table, one future UTC interval and its Linux PID | Timestamped JSONL, one compact sample per tick. No traffic generation or configuration changes. |
| `reduce` | JSONL plus terminal DMV logs; L5 also takes durable commit evidence | JSON with rates, occupancy, per-stage measurements, scoped OS counters and provenance. |
| `chart --kind waterfall` | Up to 12 reports (two core arms per level) | One linear scale for L0–L4 discarded and L5 newly committed rows/s. |
| `chart --kind resources` | Same reports | CPU-seconds/million output rows; NIC RX/TX and disk read/write MB/s. These are achieved rates, not capacity limits. |

## Capture and report

Use the actual [collector executable and deployed config](../../docs/arrowcol-dataflow/PERFORMANCE.md), not a microbenchmark. The load runner owns the DFE, clients, run schedule, codec, core allocation and warm-up. Use a **fresh, dedicated collector process and one table per cell**, plus a unique output directory. `--cores` labels the configured arm; it does not impose a CPU limit. Outputs are exclusive-create.

```bash
# From repository root, in the same Linux PID namespace as the collector.
cli=(npx --no-install tsx experiments/collector-isolation/src/cli.ts)
# PID is the actual arrowcol-dataflow-rust process, not cargo/docker/launcher.
# START/END are shared measurement UTC epoch milliseconds, after warm-up.
"${cli[@]}" sample --level "$LEVEL" --cores "$CORES" --run-id "$CELL" \
  --metrics-url http://127.0.0.1:32011/metrics --pid "$PID" --nic lo,eth0 --disk sdd \
  --start-ms "$START" --end-ms "$END" --interval-ms 500 --out "$CELL/samples.jsonl"

clients=(--client-count 2 --client "first=$CELL/client-1.log" --client "second=$CELL/client-2.log")
extra=(); if [[ $LEVEL == L5 ]]; then extra=(--commit-log "$CELL/collector.log"); fi
"${cli[@]}" reduce --samples "$CELL/samples.jsonl" "${clients[@]}" "${extra[@]}" --out "$CELL/report.json"

reports=(); for f in "$RUN_ROOT"/*/report.json; do reports+=(--report "$f"); done
"${cli[@]}" chart --kind waterfall "${reports[@]}" --out "$RUN_ROOT/waterfall.png"
"${cli[@]}" chart --kind resources "${reports[@]}" --out "$RUN_ROOT/resource-roofline.png"
```

Use only devices actually involved. Loopback measures both peers in the network namespace; `sdd` is an example, not an inferred backing device. For production L5 keep normal fencing, dedup and ACK settings. L0–L4 require matching `ARROWCOL_COLLECTOR_ISOLATION_LEVEL` / `ARROWCOL_CLIENT_ISOLATION_LEVEL` tokens (`l0`…`l4`); discard receipts are not production admissions. L3/L4 need a dedicated `ARROWCOL_ISOLATION_SCRATCH_DIR`; local L4 uses `ARROWCOL_ISOLATION_OBJECT_STORE=local`. Do not interpret local object-store traffic as Azure bandwidth.

| Evidence | Accounting boundary / failure behavior |
| --- | --- |
| `/metrics` | Process-cumulative counter deltas divided by actual monotonic-midpoint seconds. Received and successful exporter publication bytes are **logical Arrow**; published-file histogram sums are **physical Parquet**. Publication receipts can include replay; not the new-commit numerator. Stage histograms give completed durations/counts, not inferred row or byte rates. |
| `/v1/isolation` (L0–L4) | Scraped in parallel after warm-up. Ingress rows/IPC body bytes are successful stream completions. Transport poll CPU/wall excludes async idle and does not isolate decoder CPU. Physical encode/fsync/readback/upload count successful batches/rows/compressed bytes. Failed attempts contribute to stage duration/errors; asynchronous upload has no attributed CPU. Non-exercised or missing stages are null. |
| Optional `--stages PATH` | External physical samples in `{run_id,level,start_unix_ms,end_unix_ms,samples:[{at_unix_ms,snapshot}]}`. Pass the *physical* subtree, same run and actual scrape midpoints. Overrides the automatic physical source only. |
| Client offered/accepted | One unique terminal `OAT_CLIENT_METRICS` log per expected client. All must match `[START,END)`. Uses `measurement_window.offered_rows` when emitted; `--offered-field attempted_rows` explicitly selects that field **only if its producer defines send-start rows in this window**. Never uses `workers[].attempted_rows`, capacity, retry projections or configured concurrency. Accepted L5 / diagnostic L0–L4 use exact-window `completed_rows`, cross-checked against process-lifetime receipt kinds. Offered and completed need not be the same cohort. |
| New durable L5 rows | `--commit-log` needs the complete, unfiltered, dedicated single-collector stdout/stderr. Counts the producer's `arrowcol_data_commit` events by finish timestamp and unique table/version, not exporter/client ACKs. Unknown outcomes fail closed. Alternatively pass `--commit-window PATH` with that cell's native OAT `commit_window_output_throughput` object. No fallback to receipts. |
| Backlog, permits, flush slots | Trapezoidal time mean and sampled peak. Global and table permit pools are separate; multiple group states overlap. DFE buffered bytes, unique publication backlog bytes, and reserved permits are different populations. Completed group-time counters can straddle either edge. |
| Commit busy | Successful physical commit durations attributed once by `CommitReceipt.commit_duration`, divided by elapsed time. Completion-boundary estimate for a single table/worker; may exceed one in short windows. Not all lock acquisitions, CPU use, or instantaneous lock occupancy. |
| Little’s law | For healthy unique publications only: `W = Δsettled_byte_seconds / Δsettled_bytes`, `L = time-mean(publication_backlog_bytes)`. Reports `L/W` versus measured `Δsettled_bytes/elapsed`. Resets, missing counters, unknown accounting or zero denominators yield null. Boundary carry and gauge sampling can separate estimates; not a capacity ceiling. |
| Linux | Process user+system ticks include all threads, no children; individual TIDs must survive both boundaries with the same start time. `/proc/PID/io` is process storage I/O; NIC is whole target netns; block device is kernel-wide (512-byte sectors). Disk busy/queue are kernel `io_ticks`/`time_in_queue`. Verified cgroup v2 stats include siblings; missing PSI, OOM/limit or inaccessible counters stay null. No unmeasured bandwidth, per-device error or CPU ceiling is inferred. |
| Timing / provenance | Arm before `START`. Thread scans only at both ends. Requests have bounded timeouts; missed interior ticks are skipped. Endpoints disclose read spans and mean-clock skew; sources outside `min(500 ms, interval/2)` at either boundary are unavailable. Scrape rates are near-boundary, not exact-event counts. The PNG embeds run IDs, report paths, UTC windows, denominator counts, process CPU seconds and arm/mask in an `iTXt` chunk. L4→L5 includes different batching and production semantics, not just one extra stage. |

## Validate (no benchmark)

```bash
npx --no-install tsc --project experiments/collector-isolation/tsconfig.json
npx --no-install tsx --test experiments/collector-isolation/src/*.test.ts
```
