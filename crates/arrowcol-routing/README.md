# arrowcol-routing

`arrowcol-routing` is the shared production routing control plane used by every
Arrowcol router replica. It keeps SQL out of the hot path when an authoritative
cached lease is valid.

## Guarantees

- one current owner for each `(implementation_pool, catalog, schema, table)`;
- monotonically increasing fencing epoch and compare-and-set lease version;
- two-phase reassignment: stop admitting new streams, require a post-drain
  snapshot with zero table streams, buffered bytes/rows/batches, and pending
  commits, then transfer ownership;
- crash succession: a valid lease whose holder registration expired or reached
  `stopped`/`failed` moves directly to the live, eligible restart registered
  with the same `instance_key` in the same pool, with a new lease ID and greater
  epoch/version; no drain proof is needed because the restart replays the
  predecessor's durable backlog, and different-key collectors still need one;
- deterministic weighted rendezvous selection with stable fallbacks;
- exclusion of expired registrations, stale snapshots, draining collectors,
  explicit subsystem blocks, and hard pressure;
- warm-up weighting for newly registered collectors;
- SQL outage behavior that uses only unexpired cached leases and fails closed
  after expiry;
- immutable OAT run pins are initialized only when the owner lease covers the
  full run lifetime;
- no body or Arrow dependency: all inputs are bounded control-plane values.

## Main APIs

```rust
use arrowcol_routing::{
    CollectorRegistration, RichPressureSnapshot, RoutingEngine, RoutingPolicy,
    RoutingStore, SqlServerRoutingConfig, SqlServerRoutingStore,
};
```

- `RoutingStore`: registration, heartbeat, rich pressure publication, immutable
  view loading, lease claim, drain CAS, fenced transfer CAS, and
  same-instance-key crash-succession CAS (`transfer_to_successor`).
- `SqlServerRoutingStore`: SQL Server implementation over migration `V0008`.
- `MemoryRoutingStore`: deterministic unit/race-test implementation with
  operation counters.
- `RoutingEngine::route_ingest`: cache-only for a valid lease; SQL CAS only for
  a missing or expired lease.
- `RoutingEngine::route_control`: lease-free selection of a healthy,
  catalog-capable backend for explicit Flight SQL control metadata.
- `RoutingEngine::rebalance`: refresh, crash succession to a same-instance-key
  restart, and two-phase pressure/staleness drain and transfer.

## Rich pressure snapshot

`RichPressureSnapshot` validates:

- CPU basis points and RSS/limit;
- active streams;
- cumulative received bytes, rows, and batches;
- buffered bytes, rows, and batches;
- available permits and pending commits;
- oldest buffered age;
- Delta, Quiver, and DFE pressure and readiness state;
- bounded table-level stream/buffer/commit observations.

Collectors remain responsible for reporting truthful observations. Routers
validate bounds and freshness before using them for admission decisions.

## Validation

```bash
cargo fmt --package arrowcol-routing -- --check
cargo clippy --locked --package arrowcol-routing --all-targets -- -D warnings
cargo test --locked --package arrowcol-routing
RUSTDOCFLAGS="-D warnings" cargo doc --locked --package arrowcol-routing --no-deps
npx nx run arrowcol-routing:verify

# Starts arrowcol-infra, runs ignored SQL race tests, and always stops the stack.
npx nx run arrowcol-routing:live-integration
```
