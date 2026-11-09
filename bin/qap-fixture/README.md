# Bounded Azure QAP fixture

| Purpose | Command | Result |
| --- | --- | --- |
| Verify producer and physical oracle | `npx nx run qap-fixture:verify` | Formatting, strict Clippy and real bounded local seed/inspect tests. |
| Build the Azure QAP experiment tools | `npx nx run azure-qap-experiment:build` | Release `qap-fixture` and `delta-maintenance` binaries. |
| Run QAP maintenance observations | [Azure QAP experiment](../../experiments/azure-qap/README.md) | Supported seeding, acceleration, maintenance, exact oracle and evidence. |

This dedicated experiment binary exposes only `seed-scale` and `inspect-scale`.
It retains the existing native Delta fixture producer, resumable manifest and
bounded physical Parquet oracle. Generic seed/append commands, Direct collector
receipt/lease paths and general-purpose ingestion are not exposed.

| Safety invariant | Required behavior |
| --- | --- |
| Scope | Explicit `--table` equals `--allow-table`; local root is absolute, cloud root exactly matches private `ARROWCOL_DELTA_ROOT`. Minute ingestion partitioning is required. |
| Bounds | Seed payload 1-32 MiB, copy concurrency 1-8, inventory at most 1024 files and 8 GiB; physical oracle projects only keys with independent byte and row bounds. |
| Cloud fencing | Captured generation and supported shared publisher protect every mutation. Unknown closure is failure, never clean success. |
| Retry | Durable manifest binds the seed plan and records server-side copies. Conflicting plans, foreign paths or altered seed payload fail rather than overwrite. |
| Cancellation | SIGTERM/SIGINT and soft deadline request supported drain; hard deadline never reports clean publisher closure. |
| Exactness | Active Delta files and projected physical values supply the oracle, not nominal file counts. Deletion vectors and column mapping remain disabled. |
| Tombstones | VACUUM fixtures first PURGE the expired partition so only already-unreferenced files are deleted later. |

Cloud fixture creation and maintenance incur Azure charges and mutate only the
experiment's explicitly allowlisted run prefix. Use the existing experiment
launcher and approved private authentication; never use this producer to reset
authority, recreate readiness or write the production loadtest roots.
