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
- SQL outage behavior that uses only fresh cached views and unexpired leases,
  failing closed after either bound;
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
- `DiskRoutingStore`: opt-in single-writer local benchmark authority, checksummed
  versioned atomic snapshots, central least-pressure reservations, sticky leases
  and explicit durable migration phases. It does not run automatic succession.
- `RemoteRoutingStore` and `serve_control`: bounded private authenticated control
  transport with collector stable-key/process binding, data-router and operator
  roles. Followers poll full views and never become writers.
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

Direct collectors report actual process CPU plus cgroup CPU usage normalized to
effective affinity/quota capacity, actual RSS, and separate unclamped cgroup
memory usage/limit when present. Failed measurements are not reported as idle.

## Local disk-backed controller mode

The [finite routing harness](../../projects/loadtest/routing-benchmark/README.md)
runs the sole writer inside router 1, not a separate authority process. Every
new collector RPC checks authoritative routing state even when its SQL
publication handle is cached. Already-admitted source work may renew only its
exact still-valid Active/Draining route lineage during a controlled migration.
The source drains and releases SQL publication authority before the controller
durably transfers a greater routing epoch. The target acquires SQL authority
while its route remains Draining and acknowledges activation before new RPCs.

Snapshots are bounded, versioned and checksummed. A writer lock, sync of the
pending file, atomic replacement and parent-directory sync precede exposure/ACK.
CAS failures, corrupt/unsupported/missing initialized snapshots and uncertain
I/O do not silently fall back or reset epochs. Collector health is not restored
from disk; retirement intent and minimal exact migration progress are retained.
Progress retains the last operation for each of at most 256 tables, with one
move in flight globally, so moving another table cannot erase reconciliation
of a lost target-activation response. Reaching the bound fails closed.
Only the same authenticated still-running process can re-register after an
authority restart. A routing/SQL lease that expired, changed lineage, or has an
uncertain release can leave work blocked and needs explicit operator recovery;
there is no automatic failover or exactly-once claim.

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
