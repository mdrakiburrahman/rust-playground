# quiver-cli

`quiver-cli` writes real SQL Server telemetry to Quiver exactly as the Arrowcol
Direct collector does: the same bundle and manifest, the same table schema, and
an fsync per bundle. It then shows what each file in a Quiver directory holds.

Run it from the workspace root:

```bash
npx nx run quiver-cli:run -- generate
npx nx run quiver-cli:run -- tree
```

The equivalent Cargo command is:

```bash
cargo run --locked --package quiver-cli -- tree
```

Every command takes an optional directory, defaulting to the gitignored
`onelake/quiver-demo`.

| Command | Shows | Writes |
| --- | --- | --- |
| `generate` | CSV rows to bundles, WAL fsyncs, and segments | Yes |
| `tree` | Every file and what it means | No |
| `segments` | Sealed segments, their rows, and acknowledgements | No |
| `show` | One bundle's manifest and rows, from a segment or the WAL | No |
| `wal` | WAL entries, the cursor, and what replays on open | No |
| `consume` | Acknowledgements and segment deletion | Yes |
| `crash` | Bundles fsynced by a child process killed before a seal | Yes |
| `recover` | WAL replay into a new segment | Yes |

The read-only commands also open the load test's Direct and Dataflow Quiver
directories. The writers refuse any directory without the `quiver-cli.json`
marker that `generate` leaves, so they never touch a collector's directory.

The default data is [`sqlserver_wait_stats.csv`](data/sqlserver_wait_stats.csv),
3,546 real `sys.dm_os_wait_stats` rows captured with
[`capture-wait-stats.sh`](data/capture-wait-stats.sh). Its schema comes from
the load test DDL in `projects/loadtest/schemas/sqlserver.sql`. The leading
`customer_id` cycles through 16 synthetic labels (`customer-0000` through
`customer-0015`); the original wait-statistic values are unchanged. The capture
script adds the same labels when refreshing the fixture. Custom CSV input must
include this required first column as well.

The guided tour is [Quiver on disk](../../docs/quiver/README.md). Its
walkthrough also runs as an integration test:

```bash
npx nx run quiver-cli:verify
```
