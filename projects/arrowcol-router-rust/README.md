# Arrowcol production router

This project is the production Pingora router for Arrow Flight collectors. The
standalone `spike/` remains protocol and RSS evidence; the application in this
directory is the workspace-integrated implementation.

## Data path

The router accepts cleartext HTTP/2 behind an external TLS terminator. Generated
clients put all routing fields in initial gRPC metadata:

```text
x-arrowcol-protocol-version: 1
x-arrowcol-implementation-pool: dataflow
x-arrowcol-catalog: telemetry
x-arrowcol-schema: prod
x-arrowcol-table: traces
x-arrowcol-schema-fingerprint: sha256:<64 lowercase hex digits>
```

Only `POST /arrow.flight.protocol.FlightService/DoPut` uses table leases.
`arrowcol-protocol::IngestMetadata` performs bounded canonical validation before
the body is forwarded. The schema fingerprint is only a client claim; the
selected collector must decode the Flight stream and revalidate the actual Arrow
schema against the catalog.

The proxy does not implement `request_body_filter`, does not decode `FlightData`,
does not retry `DoPut`, and keeps the chosen backend for the lifetime of the
stream. Pingora forwards response metadata, message bodies, trailers, flow
control, and cancellation. Upstream connections use H2 multiplexing and reuse.
SIGTERM triggers Pingora's H2 GOAWAY path and bounded graceful runtime drain.
There is no router-side Flight body buffer; bounded schema/message decoding and
batch channels live in the selected backend.

## Routing ownership

`SqlServerRoutingStore` stores only routing registrations, pressure, and fenced
leases in the V0008 `routing_*` tables. It is not a receipt or catalog store.
`arrowcol-routing` loads collector registrations, rich pressure snapshots, and
fenced leases into an immutable cache. A valid cached lease performs no SQL.
Missing/expired leases use serializable SQL compare-and-set. Router replicas
therefore converge on one owner for each implementation-pool/table key.

Pressure or staleness reassignment is two phase:

1. CAS the active lease to `draining`; new streams receive a retryable failure.
2. Existing streams stay on their selected upstream.
3. After a fresh post-drain snapshot reports zero active streams, buffered
   bytes/rows/batches, and pending commits for the table, CAS a transfer with a
   new lease ID and greater fencing epoch/version.

If SQL refresh fails, unexpired cached leases remain usable. The router never
creates ownership from stale cache data and fails closed after lease expiry.

## Flight SQL control routing

Flight service RPCs other than `DoPut` require:

```text
x-arrowcol-control-operation: catalog_read | catalog_write | new_table
x-arrowcol-implementation-pool: dataflow
```

They route through deterministic cached selection to a fresh, healthy,
catalog-capable backend. `new_table` does not require a pre-existing table lease.

## External TLS and identity

TLS terminates at a trusted edge. Every non-public request must carry:

```text
x-arrowcol-edge-timestamp
x-arrowcol-edge-nonce
x-arrowcol-edge-signature
```

The signature is HMAC-SHA256 over the timestamp, nonce, method, path, content
type, routing metadata (including `x-arrowcol-oat-run-id`), and control metadata.
Router and edge-signing clients must be upgraded together when this signed
header set changes, including requests without an OAT run ID.
Assertions are time-bounded and
nonces are replay-protected in process. Direct traffic, tampering, replay, and
externally supplied `x-arrowcol-internal-auth-*` headers are rejected.

Replay memory defaults to 65,536 nonces per router process; configure
`auth.edge_replay_max_nonces` from 1 to 1,048,576 before a higher-rate run.
Expired assertions
are removed before admission; a full cache rejects new assertions with gRPC
`RESOURCE_EXHAUSTED` rather than growing or evicting live nonces. Nonces remain
protected through the assertion's last valid millisecond, including accepted
future clock skew. Only successfully verified signatures consume capacity.
At the default 30-second age, 65,536 entries cover about 2,184 requests/s with
current timestamps, or 1,092 requests/s with the maximum accepted future skew.
For peak signed-request rate R, size capacity above `R * (2 * age_seconds + 1)`
including bursts and all control requests. E4's 128 clients are not a request-rate
bound: 2,184 requests/s is only 17 requests/s per client if one router receives
the entire fleet. Measure signed-request rate before qualification and increase
capacity or bound the edge rate; do not treat capacity rejection as throughput.
Replay protection is process-local, not shared across replicas.

Startup requires an explicit `auth.authenticator: dev_only_allow_all`. Missing,
legacy `allow_all`, and unknown modes are rejected before listeners start.
This development-only mode grants the same principal to every **edge-verified**
request; it never bypasses the mandatory edge HMAC. Checked-in local, Compose,
image-smoke and generated Kubernetes OAT configurations explicitly select it.
These are development/qualification deployments, not production identity
authentication. No production authenticator is implemented.

After authentication the router strips edge proof and signs a bounded serialized `AuthContext` for the
collector:

```text
x-arrowcol-internal-auth-context
x-arrowcol-internal-auth-expires
x-arrowcol-internal-auth-signature
```

Edge and internal signing keys must be different. Production collectors must
verify the internal signature/expiry and reject direct external traffic. This is
the signed-metadata option. External TLS termination, a production identity
provider, and internal mTLS are explicit deployment extensions rather than
capabilities claimed by the checked-in router.

## HTTP APIs

Public operational APIs:

- `GET /healthz`
- `GET /readyz`
- `GET /v1/capabilities`
- `GET /metrics`

Receipt lookup and status endpoints have been retired. A DFE durable-buffer
acknowledgement indicates durable payload storage, not completed Delta publication.

## Configuration

- `config.yaml`: local Docker-infrastructure defaults.
- `config.docker.yaml`: container-ready paths and internal TLS defaults.
- `config.smoke.yaml`: dependency-free image startup settings with ephemeral
  runtime-generated secrets and an intentionally unavailable SQL endpoint.
- `Dockerfile`: pinned multi-stage production image running as UID/GID `10001`
  with read-only configs and writable Pingora state.

All YAML structs use `deny_unknown_fields`. Validate without contacting SQL:

```bash
target/debug/arrowcol-router-rust \
  --validate-and-exit \
  --config projects/arrowcol-router-rust/config.yaml
```

The image smoke validates both embedded configs, starts Pingora without cloud
credentials, proves HTTP/2 prior-knowledge capability serving, checks health
and fail-closed readiness, and then requires bounded shutdown:

```bash
npx --no-install nx run arrowcol-router-rust:image-smoke
npx --no-install nx run arrowcol-router-rust:publish
```

Publication is clean-worktree guarded and writes the exact tag manifest under
`target/images/`.

Create two independent, random, single-line secrets before local execution:

```bash
mkdir -p projects/arrowcol-router-rust/secrets
openssl rand -base64 48 > projects/arrowcol-router-rust/secrets/edge-hmac.txt
openssl rand -base64 48 > projects/arrowcol-router-rust/secrets/internal-hmac.txt
```

The secrets directory is ignored by Git.

## Metrics

Metrics deliberately have no table-ID labels and expose only the `dataflow` pool:

- `arrowcol_router_decisions_total`
- `arrowcol_router_failures_total`
- `arrowcol_router_cache_refresh_total`
- `arrowcol_router_lease_events_total`
- `arrowcol_router_upstream_connections_total`
- `arrowcol_router_active_streams`
- `arrowcol_router_cached_leases`
- `arrowcol_router_maximum_collector_pressure_bps`

## Qualification

```bash
cargo fmt --package arrowcol-router-rust -- --check
cargo clippy --locked --package arrowcol-router-rust --all-targets -- -D warnings
cargo test --locked --package arrowcol-router-rust
RUSTDOCFLAGS="-D warnings" cargo doc --locked --package arrowcol-router-rust --no-deps
npx nx run arrowcol-router-rust:verify

# Multiprocess production Pingora H2/auth/control/error/cancellation/RSS test.
npx nx run arrowcol-router-rust:pingora-integration

# Verified spike regression: routing, trailers, connection reuse, drain, and RSS.
npx nx run arrowcol-router-rust:rss-regression

# Starts SQL routing infrastructure, exercises live multi-router SQL races, and
# stops the Compose project in a trap.
npx nx run arrowcol-router-rust:live-integration
```

## Remaining collector integration contract

Collectors still need to post `CollectorRegistration` and
`RichPressureSnapshot` documents, verify signed internal identity and routing
lease headers, revalidate the received Arrow schema, and report per-table active
streams during drain. Routing lease fencing is separate from the collector's
publication fence; collectors must continue enforcing the authoritative
publication lease before Delta commit.
