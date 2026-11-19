# Remote storage measurement method

Storage calls and an embedded engine answer different questions. The Rust driver measures Quiver on a filesystem and Azure Blob storage primitives independently, so a fast primitive does not stand in for an implemented, qualified engine backend.

## Measurements

| Measurement | Definition |
| --- | --- |
| Post-ingest acknowledgement latency | Successful `ingest` completion to successful covering `sync_wal` completion |
| End-to-end acknowledgement latency | `ingest` entry to covering `sync_wal` completion, including inline work |
| Commit latency | Filesystem write plus fsync, block staging plus cumulative block-list commit, or all append requests in a group |
| Durable-call latency | Fsync, block-list commit, or the sum of append requests |
| Fence-stat proxy | The same filesystem metadata operation on the current generation file, measured separately from engine throughput |
| Throughput | Logical bytes and bundles over elapsed time, reported per driver process |
| CPU | Process user plus system CPU per logical GiB, where the operating system supports it |

Every latency distribution reports its sample count. A p99.9 estimate from fewer than 1,000 observations is flagged. Bundle latencies from a shared commit are correlated; a large bundle count does not establish an independently sampled commit tail. Run cold setup separately, repeat steady-state runs, and inspect host, share, and network limits before attributing a plateau to a backend.

Forced generation-file opens are included in ingest and sync timings. The fence-stat proxy measures only the metadata lookup; traced open/close durations are supporting diagnostics from correctness runs and exclude some engine overhead.

The interval sweep includes immediate sync and intervals through one second. Larger groups can reduce request overhead while adding waiting time. There is no built-in latency SLO: use the measured latency/throughput frontier to choose a policy.

## Recovery oracle

The writer carries a sequence ID and a deterministic payload in each bundle. It records IDs only after a successful covering WAL sync, and fsyncs each oracle group on a separate data disk with host caching disabled. Recovery checks payload contents and requires every complete oracle receipt to be present; duplicates are allowed. A missing, empty, malformed, or mismatched oracle cannot produce a passing result. Only an incomplete final receipt is ignored.

The fault actors coordinate through a separate authenticated control store. Kill and abrupt-reboot cases stop between an ingest and its covering sync, after earlier groups have complete receipts. Pause and partition cases stop before another ingest, so a resumed writer can exercise appends after the replacement's claim. Metadata caches are warmed with a maintained owner before the pause. The replacement retries ownership acquisition and emits a claim timestamp. The original writer is resumed or healed while the replacement still owns the directory, and the final comparison includes any additional acknowledgements from that original writer.

For a restart before downstream acknowledgement, `recover --pause-before-drain <gate>` stops immediately after opening and replaying, before flushing or polling the subscriber. The fault request's `restartBeforeDrain` option keeps this replacement alive while the original writer resumes, records the retained rotated WAL, then kills and restarts the replacement on the same job directory before draining. An absent rotated WAL makes the check untested and stops the job. `intervalMs` selects the fault writer's and replacement's WAL interval, including zero. Traces include generation-file opens and closes, and the evidence includes segment listings and both recovery stages.

Directory listings are non-atomic observations. A name removed between enumeration and stat is retained with `disappeared: true`; other errors stop collection. A vanished WAL cannot serve as a retained-WAL witness. Use syscall traces alongside the listing to establish publication and cleanup ordering.

An abrupt-reboot case leaves the writer behind the barrier, so its independent receipt snapshot contains all acknowledged groups. Verify the guest boot ID changed before accepting the case. Cross-host wall-clock measurements also require synchronized clocks; preserve the timestamp method and its uncertainty.

> ⚠️ Any missing acknowledged ID stops the experiment. Preserve the oracle, recovered IDs, engine output, source revision, mount options, and environment before further investigation. Do not repair engine code inside the measurement run.

## Filesystem options

The predefined cache profiles retain server-side locks: defaults, `lookupcache=none`, `lookupcache=positive,actimeo=1`, and `actimeo=30`. The runner rejects effective options containing `nolock`, `local_lock=flock`, or `local_lock=all`. Transport starts with AzNFS TLS; a plain-NFS comparison is a separate, isolated run if required.

Record the effective mount rather than just the requested options, the AzNFS package version, kernel, read mode, machine size, and exact image version. Standard segment reads are the default, with mmap explicitly selectable.

Each guest job uses a private mount namespace and verifies that isolation before mounting. Cache-profile changes therefore do not depend on globally unmounting a previous job's share. Environment and initial status snapshots are flushed, with their job directory and its parent, before workload launch so they remain available after an abrupt reboot. Setup failures notify the peer even when they happen before the writer/recovery coordination starts. Preserve those failures as incomplete attempts and use fresh job identities for a retry.

## Blob measurements

The block path stages a block and commits the cumulative block list with lease and ETag conditions. The append path uses a stable service API and splits larger groups into requests of at most 4 MiB, acknowledging the group only after every request succeeds. Leases are renewed and blobs rotate before their service limits. Setup, renewal, rotation, request counts, and list sizes remain distinct from the final durable call.

The race test requires one lease winner, rejects the other writer, and reads back the accepted content. These checks cover primitive fencing. They leave WAL integration, release semantics, recovery, and Windows runtime qualification to the actual backend implementation.

## Private evidence

The lab's configuration and investigation output belong in an ignored directory. Public documentation contains the method and generic code only. Resource inventories and full findings stay with the investigator. Deallocation stops owned VMs and retains the lab for another iteration; deleting resources is a separate operation.
