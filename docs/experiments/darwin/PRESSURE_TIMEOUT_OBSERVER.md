# Pressure timeout evidence

`projects/arrowcol-oat/diagnostic-pressure-sampler.ts` is a standalone Node 24 sampler. For a future approved probe, use this file in place of the scratch sampler **only**; retain the runner's discovery of the single inspected collector and its gateway port. No collector, guard, deadline, retry, or baseline settings change. Run from a clean, pinned checkout. Verify the SHA-256 of `OUTPUT.observer.ts` against both the raw JSON and `git show HEAD:projects/arrowcol-oat/diagnostic-pressure-sampler.ts | sha256sum`. Preserve both files even on failure.

| Field | Interpretation |
|---|---|
| `started_at`, `start_monotonic_ms`, `late_start_ms` | Wall time for alignment; process-monotonic start and lag from intended next start. Lag includes any overlong previous request. |
| `trace.offset_ms` | Monotonic offsets to Undici create, socket dispatch, first observed headers, trailers (parser completion), and error. Not timestamps on the wire. |
| `fetch_headers_ms`, `body_complete_ms` | When the caller resumed with a Response or finished consuming its body, not when the server sent bytes. |
| `connection_id/use`, ports | Process-local socket identity. `new` needs a matching connected event and first dispatch; `reused` needs an earlier correlated dispatch on that socket. Never infer reuse from a port alone. |
| `observer` | Attempt-local process CPU microseconds, voluntary/involuntary switches, event-loop utilization and 10-ms-resolution loop-delay histogram. No histogram sample means unknown, not zero delay. These are whole-process metrics, not per-request CPU or proof of descheduling. |
| `validity` | Missing/inconsistent channels are unknown. Null is unobserved. Even an observed sequence ending in error identifies only the last observed phase, not a server or network cause. |

The observer never serializes event payloads, headers, bodies, credentials, exception messages or other requests. As in the original sampler, `response.arrayBuffer()` drains and buffers the full successful HTTP response before the 1 MiB size check. No body bytes are written to evidence; the limit is not a peak-memory bound. One outstanding GET, default `fetch` pooling, 3 s timeout, 1 Hz attempt cadence (no catch-up burst), 1 MiB decoded limit and the original zero-error / successful max <1.5 s verdict. Missing trace data cannot rescue a failed pressure result or fail a successful HTTP verdict.

## Discriminating probe for review only

One distinct scratch root, unchanged 7aca workload, 15 s lead, 15 s warmup and 60 s measured. Only after independent clearance: `node --import tsx SAMPLER http://172.17.0.1:PORT/v1/pressure 85 OUTPUT`. Keep prior host/roster/CPU/memory gates, independent v2 validation, clock metadata, full logs and stop/terminal evidence. No automatic retry or reference; unknown evidence stays unknown. The sampler accepts at most 120 s of starts plus the last outstanding request's timeout (subject to runtime scheduling).

Client timestamps alone cannot separate observer delay, queued transport, network delay and server processing. For independent review, propose bounded metadata-only observations at both ends: socket cookie/4-tuple, direction, length, monotonic send/receive and retransmit timestamps on the verified observer/collector flow; collector handler entry/exit and response-write timestamps for the pressure route only, if already available. No packet contents, headers, bodies, broad traffic capture, or correlation headers. Map both sides of bridge/NAT, process/container lifetimes and clock domain/uncertainty; require one-to-one attempt mapping on reused sockets. Record event drops, gaps, overhead and a hard capture-duration/record cap. Ambiguous mappings, unavailable server markers, or delays within clock uncertainty cannot localize a timeout. Do not enable new instrumentation or change the frozen server based on this design alone.

The transferred 7aca log/result show terminal guard status. Per-request guard HTTP timing is unavailable in **that retained local evidence**; this does not establish what the untransferred original contains.
