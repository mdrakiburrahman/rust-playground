# Opt-in SQL-authorized ordinary Delta publication gate

This is the **first bounded gate**, not a routing integration or benchmark.
`arrowcol-authority::sql_publication` and the `publication-gate` executable are
opt-in. Existing Flight/manifest/etcd behavior is unchanged. Neither router uses
this module yet. The original Pingora worktree is not modified.

## Authority and storage contract

```text
unprivileged collector          trusted publisher                 SQL authority
Arrow -> staged Parquet --read/copy/validate--> private native preparation
  staging RW mount                    |                          |
  destination RO mount         immutable final data objects      |
  no private/SQL access               |-- exact proposal ------>|
                                      |        owner-row lock: identity +
                                      |        DB expiry + expected version
                                      |<-- immutable accepted decision
                                      |    (NOT final receipt / client ACK)
                               contiguous exact log materialization
                               private candidate -> fsync -> hard link
                               no overwrite -> directory fsync
                                      |-- final receipt -------->|
ordinary Delta reader <--------- _delta_log + Parquet
```

Decision acceptance and ownership transfer lock the **same SQL owner row**.
The tuple includes controller/generation, membership, process/local generation,
and ownership epoch. The accepted JSON binds that tuple, table/operation,
predecessor/version, canonical native actions, and each data name/size/SHA-256.
An operation retry must match the *entire* accepted decision. No uncertain SQL
response is treated as proof of rejection. Recovery before the first accepted
CREATE uses the verified empty prefix (version -1); recovery after acceptance
must materialize CREATE before activating its successor.

```text
active owner A --transfer under SQL lock--> recovering owner B
   | late A proposals rejected                 | no new decisions
   | earlier accepted A decisions remain valid |
   +--> finish accepted prefix + final receipts + native snapshot
                                               |
                                     SQL compare-and-activate B
```

Only trusted code may call the preparation/acceptance APIs or hold SQL
credentials. The CLI identities are operator fixtures, **not authentication**.
The bounded prototype stores the exact per-table identity and one DB-clock
deadline; it does not implement member registration, heartbeats, controller
election, distributed placement, or an etcd adapter. Those are integration gates.
Do not expose arbitrary proposal JSON or privileged filesystem access to a
collector. The initial implementation uses a dedicated local SQL instance and
its administrative login; production SQL privilege separation is not qualified.

## Native Delta and durability

Preparation reuses `arrow-deltalake::DeltaTable` / pinned Kernel **0.29.0** on a
private table copied from the verified published prefix. Native initialization,
nullable schema evolution, normalization, statistics, Parquet encoding and
append actions are reused; no custom manifest is read by consumers. Exactly one
native version becomes one SQL decision. Schema changes are separate decisions.
The native private-table commit is **not** a final destination commit.

The publisher re-encodes validated bounded staging bytes into its own private
objects and promotes them into readonly final files. Collector edits, including
chmod-and-rewrite of its own staging file, cannot modify a final object's inode.
The collector has no writable destination/private mount and no SQL credential.
Host processes sharing the publisher's UID and host root are trusted, not
adversarial principals.

SQL acceptance follows data/file-directory synchronization. Materialization
loads durable decisions, verifies bound data, and writes a private candidate
outside `_delta_log`. Only after the entire candidate is synchronized is its
inode installed using a same-filesystem atomic, no-overwrite hard link. The
destination directory is synchronized before final SQL receipt recording.
Competing workers verify exact existing bytes, never overwrite them. Partial or
conflicting pre-existing final logs fail closed; they are not silently repaired.
Leftover incomplete private candidates are invisible to Delta readers.

Bounds: 8 MiB encoded staging per file, 65,536 rows / 32 MiB logical or declared
uncompressed data, one data reference and 256 KiB actions per decision, 16
pending decisions, 128 retained decisions per table and 128 preparation
workspaces per publisher root. Reaching these limits stops admission; there is
no history truncation/version reuse. The finite driver bounds staging attempts.
Retention/GC, sustained staging quotas, checkpointing and group commit remain
future work, not performance claims.

## Reproduce this gate only

From the ADR worktree, using the existing cached task images and Python reader:

```bash
export CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0
cargo build -p arrowcol-authority --bin publication-gate
mkdir -p target/publication-gate/test-scratch
export TMPDIR="$PWD/target/publication-gate/test-scratch"
cargo test -p arrowcol-authority --lib --test contracts
cargo test -p arrow-deltalake --test writer
cargo fmt -p arrowcol-authority -p arrow-deltalake -- --check
cargo clippy -p arrowcol-authority --lib --bin publication-gate -- -D warnings
target/adr0001/venv/bin/python projects/loadtest/adr0001/publication_gate.py \
  --project publication-gate-UNIQUE --subnet FREE_PRIVATE_IPV4_24
```

Choose a new safe lowercase project suffix and verified free private `/24`.
The driver checks Docker networks and host routes before allocating, uses
loopback-only dynamic SQL ports, caps SQL at 2 CPUs/3 GiB and the collector at
1 CPU/512 MiB, and never prunes. It stops only its named containers in `finally`;
SQL volumes, networks, state and failed attempts remain. Each run is fresh;
there is no destructive resume/bootstrap shortcut.

Evidence is under `target/publication-gate/<project>/`: exact commands and exit
codes, subprocess logs/PIDs, passed assertions, proposals, private candidates,
real Delta logs/data, final SQL status, source/binary hashes and stopped Docker
inventory. `deltalake` **1.2.1** scans the final files in independent processes,
without this journal or any reader modification. Scans disable Arrow prefetch
and threaded workers, as the existing native benchmark reader does, to avoid
its Python/C++ shutdown race; scan failures are still failures, never ignored.

Live cases cover stale tuple/expiry/partition rejection; real SIGSTOP/SIGCONT
around SQL and log boundaries; accepted-before-transfer recovery; concurrent
acceptance and duplicate materializers; SIGKILL during private log writing and
after publication; SQL COMMIT followed by lost response; receipt failure and
lost final response; SQL restart; real storage permission failures; successor
snapshot/receipt barriers; schema evolution; mount denial and staging mutation.

**Scope:** same-host process/network/local-filesystem failures with functioning
fsync and atomic local hard links. Not host/power loss, arbitrary corruption,
cloud/object storage, production HA, arbitrary external Delta writers, or
exactly-once client delivery. External writers must remain fenced by filesystem
permissions. No catalog-managed Delta feature or modified reader contract is
introduced. SQL receipts here are the gate projection, not yet the production
Flight/receipt API. Full SQL/etcd adapters, both routing integrations, load
benchmarks and historical-report updates are deliberately deferred.
