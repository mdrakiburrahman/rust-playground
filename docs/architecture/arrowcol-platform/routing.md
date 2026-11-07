# Routing, trust, and ownership

[Platform diagram](diagrams/platform.drawio) | [Architecture index](README.md)

## Flight metadata

Ingest uses gRPC `DoPut` over HTTP/2, not an HTTP JSON upload:

| Initial metadata | Value |
| --- | --- |
| `x-arrowcol-protocol-version` | Canonical decimal `1` |
| `x-arrowcol-implementation-pool` | `dataflow` |
| `x-arrowcol-catalog` | Validated catalog component |
| `x-arrowcol-schema` | Validated schema component |
| `x-arrowcol-table` | Validated table component |
| `x-arrowcol-schema-fingerprint` | `sha256:` plus 64 lowercase hexadecimal digits |

Flight SQL control requests carry an implementation pool and
`x-arrowcol-control-operation: catalog_read | catalog_write | new_table`.
`new_table` does not require an existing table lease.

The fingerprint is an **untrusted claim**: the router validates its shape; the
collector decodes the schema, recomputes its fingerprint, and verifies the exact
registered layout [before admission](catalog.md#schema-admission).

## Trust boundary and streaming

```text
External client
  -> trusted edge: terminate TLS; sign time-bounded request metadata
  -> router: verify edge HMAC; remove proof; sign internal context
  -> collector: verify internal HMAC context and authoritative lease
```

The router checks timestamp, nonce, signature, method, path, content type, and
routing/control metadata. Edge and internal-hop keys are different. The
pluggable authenticator is currently `allow_all` **after** valid edge proof;
it is not an anonymous bypass. A real identity provider and internal mTLS are
extension points, not checked-in production guarantees.

| Edge-to-router proof header | Contract |
| --- | --- |
| `x-arrowcol-edge-timestamp` | Canonical unsigned decimal Unix milliseconds; within configured age/skew bound |
| `x-arrowcol-edge-nonce` | Fresh 1..64-byte ASCII alphanumeric/`.`/`_`/`-` nonce; replay is rejected |
| `x-arrowcol-edge-signature` | Base64url without padding, HMAC-SHA256 over canonical request facts |

Trusted edges/tests can use `insert_edge_signature_headers` in the
[auth module](../../../projects/arrowcol-router-rust/src/auth.rs). Keep edge
secrets at the trusted boundary, not in generated SDKs. Poll retries need fresh
proof. External requests must not supply `x-arrowcol-internal-auth-*` headers.

HTTP/2 metadata, frames, trailers, flow control, and cancellation stay on the
selected backend. The router neither retries `DoPut` nor switches collectors
midstream. Bounded decoding and batch channels belong to collectors.

## Leases and pressure

Ownership is unique for the Dataflow `(catalog, schema, table)` key. More DFE
collectors scale across tables, not concurrent writers to one hot table.

| Condition | Router behavior |
| --- | --- |
| Unexpired cached active lease | Route without SQL on the ingest hot path |
| Missing/expired lease | Serializable SQL compare-and-set |
| SQL refresh fails | Use an existing unexpired cached lease only; fail closed after expiry |
| Pressure/staleness reassignment | Mark draining; reject new streams with retryable error; leave existing streams in place |
| Safe transfer | Require fresh post-drain snapshot with zero active streams, buffered bytes/rows/batches, and pending commits; issue new lease ID and greater epoch/version |
| Crash succession | A holder whose registration expired or that reported `stopped`/`failed` can never prove drain. If a live, eligible collector in the same pool shares its `instance_key` (a restarted process), move the active or draining lease directly to it with a new lease ID and greater epoch/version. A configured durable buffer can replay the predecessor's persisted backlog; without it, in-memory batches are not recoverable. Different-key collectors still require one |

Pressure reports CPU basis points, RSS/limit, active streams, cumulative received
bytes/rows/batches, buffered counts, permits, pending commits, oldest age,
Delta/DFE/Quiver readiness, and per-table stream/buffer/age/commit observations.

Collectors recheck the authoritative publication fence immediately before each
Delta log commit. A stale owner may finish Parquet work but cannot publish a
fenced Delta version. After expiry, reusing a stable instance key requires a
greater generation; the predecessor is fenced from heartbeat and publication.

Sources: [router](../../../projects/arrowcol-router-rust/src/router.rs),
[routing engine](../../../crates/arrowcol-routing/src/engine.rs),
[authenticator](../../../projects/arrowcol-router-rust/src/auth.rs).
