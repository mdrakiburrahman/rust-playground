# Delta maintenance

Standalone Delta Kernel service; separate from collector appends. Dry run by default.

| Task | Command |
| --- | --- |
| Build | `cargo build -p delta-maintenance` |
| Plan | `target/debug/delta-maintenance --root file:///data/delta --table events --granularity minute --operation optimize` |
| Catalog policy | `target/debug/delta-maintenance --catalog-endpoint http://catalog:8817 --operation purge --operation optimize --operation vacuum` |
| Apply | Add `--apply` to the plan command. |
| Test | `cargo test -p delta-maintenance` |

| Operation | Effect |
| --- | --- |
| `optimize` | Compact closed Hive partitions; atomic `dataChange=false` rewrite. |
| `purge` | Remove expired ingestion partitions with an atomic retention watermark. |
| `vacuum` | VACUUM LITE: delete expired tombstones using the log, not directory scans. |
| `checkpoint` | Publish a classic checkpoint to bound snapshot replay. |
| `prune-log` | Delete old history only after checkpoint equivalence and VACUUM progress are proven. |

| Safety | Contract |
| --- | --- |
| No work | Negative probes avoid replay and idle metadata writes; no empty Delta commit. |
| Table policy | [Typed TBLPROPERTIES registry](../../docs/reference/TABLE_PROPERTIES.md); table values override process defaults. |
| Concurrent appends | Revalidate readsets; bounded commit retries. |
| VACUUM retention | Seven-day minimum; shorter values require the disposable-test override. |
| Reset coordination | Azure mutations use the shared publisher session. |
| Log size | Pruning bounds historical file count, not total checkpoint bytes. |
| Detailed contracts | [Configuration, pruning proofs, limits and failure handling](../../docs/operations/DELTA_MAINTENANCE.md) |
