# arrowcol-buffer

Shared cloud-generation, telemetry, and safe diagnostic utilities for Arrowcol.
Direct buffering, Quiver receipt replay, and SQL-backed publication coordination
have been retired.

| Export | Purpose |
| --- | --- |
| `CloudGeneration` | Validated cloud reset epoch before DFE durable admission and recovery |
| `MetricsGuard` | OpenTelemetry metrics initialization and shutdown |
| `safe_error_message` | Redact diagnostics before DFE warnings and NACKs |
| Validation | `npx --no-install nx run arrowcol-buffer:verify` |

`safe_error_message(&str) -> String` replaces credentials, URIs (including Azure
storage/SAS URLs), absolute filesystem paths, JWT-like tokens, and UUID-like
private identifiers with `[REDACTED]`. It strips controls and limits output to
1,024 UTF-8 bytes while preserving readable error kinds and schema/type causes.
It changes only diagnostics, never acknowledgement or retry decisions.
