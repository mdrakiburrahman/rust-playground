# Tenant stream

Standalone, checkpointed Delta fork for one customer: snapshot first, then tail committed appends.

| Task | Command |
| --- | --- |
| Build | `cargo build -p arrow-tenant-stream` |
| Snapshot / resume once | `target/debug/arrow-tenant-stream --source /data/source --target /data/tenant --checkpoint /data/checkpoint --customer-id customer-0000 --once` |
| Continuous tail | Same command without `--once`. |
| Test | `cargo test -p arrow-tenant-stream` |

| Behavior | Contract |
| --- | --- |
| Filtering | Equality on UTF-8 `customer_id`; projection retains the key and partition columns. |
| Resume | Reuse the same target and checkpoint; committed batches are deduplicated. |
| Source maintenance | Ignore OPTIMIZE rewrites; copied rows survive source PURGE/VACUUM. |
| Unsupported changes | UPDATE/MERGE/overwrite, schema evolution and unsupported Delta features fail explicitly. |
| Retention | Source history and files must outlive consumer lag and recovery. |
| Azure | Shared publisher authority; crashed cloud ownership requires explicit operator recovery. |
| Detailed contracts | [Checkpointing, limits, configuration and failure handling](../../docs/operations/TENANT_STREAM.md) |
