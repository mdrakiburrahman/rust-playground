# Native ArrowCol runtime on Windows

The [Windows release](https://github.com/mdrakiburrahman/rust-playground/releases/tag/arrowcol-windows-latest)
contains one current Windows x64 ZIP with `arrowcol-catalog-rust.exe`,
`arrowcol-dataflow-rust.exe` the canonical `config.windows.yaml` and `manifest.json`. The executables use the static
MSVC CRT. From `0.1.0-alpha.4` the `Rust.Playground` .NET package also carries
both executables, verified against this manifest when packed, so .NET consumers
need not repackage them.

| Operation | Contract |
| --- | --- |
| Discover | Look up release tag `arrowcol-windows-latest` (not the repository-wide latest release). Its `Current asset:` line names `arrowcol-windows-x64-<full-source-sha>.zip`. During replacement two assets can briefly coexist. |
| Verify | Match the ZIP's SHA-256 to the GitHub release asset `digest`. Inside the ZIP, require `manifestVersion: 1`, `target: x86_64-pc-windows-msvc`, and `sourceSha` matching the filename; compare each payload file's SHA-256 to its entry in `files`. The rolling tag itself is not a version identifier. |
| Prepare | Provision service-owned state and warehouse directories on local NTFS. Use the same warehouse directory for both processes: a `file:///C:/...` URI with percent-escaped spaces for the catalog, and an absolute path with forward slashes for DFE. Create one ACL-protected key file readable only by the service account and administrators: generate 32 random bytes with a CSPRNG and write their hex encoding as ASCII without a BOM or line terminator. The programs use the literal bytes, trim trailing CR/LF, and reject interior CR/LF or fewer than 32 bytes. |
| Start catalog | `arrowcol-catalog-rust.exe --listen 127.0.0.1:8817 --warehouse-root <warehouse-file-uri> --state-dir <state-directory> --ddl-dir <sdk-publish-directory>/arrow-ddl --ddl-auth-mode signed_identity --ingestion-layout --shutdown-on-stdin-eof`. Set `ARROWCOL_CATALOG_DDL_IDENTITY_SECRET_FILE` to the key's path. Redirect stdin and hold it open. |
| Check catalog | Run `arrowcol-catalog-rust.exe --listen 127.0.0.1:8817 --ddl-auth-mode signed_identity --healthcheck` with the same key path. Exit 0 confirms a signed catalog RPC succeeded. |
| Start DFE | Run `arrowcol-dataflow-rust.exe --config <absolute-config-path>` with the included configuration (see the [Windows topology](../../../projects/arrowcol-dataflow-rust/config.windows.yaml)). Inherit the same key path; set `ARROWCOL_DELTA_ROOT` to the absolute path (e.g. `C:/ProgramData/ArrowCol/warehouse`) and `ARROWCOL_PARTITION_GRANULARITY=hour` for the catalog's ingestion layout. The sample provides best-effort admission, not durable delivery. |
| Check DFE | Wait for `GET http://127.0.0.1:32311/readyz` to return 200 (`ready` and `accepting_deliveries`); 503 means not ready. Verify `GET http://127.0.0.1:32312/api/v1/status` before relying on shutdown. Point the SDK at `http://127.0.0.1:32310`. |
| Drain | Stop producers and finish their streams. POST `http://127.0.0.1:32312/api/v1/groups/shutdown?wait=false&timeout_secs=60`; HTTP 202 acknowledges the request, not completion. Wait for the owned DFE process to exit 0, then close the owned catalog process's stdin and wait for exit 0. Bound both waits; a timeout, failed exit or forced kill is not a successful drain. |
| Build/publish | `gh workflow run publish-images.yml --ref <branch> -f windows_only=true` dispatches just this runtime. Relevant pushes to `main` run the same workflow. Publication follows a smoke test of the extracted ZIP, signed catalog readiness and DDL, Flight ingestion, and drain to Delta. |

Use these defaults only in a **trusted Windows guest**. Loopback is not caller
authentication. Ordinary receiver Flight ingestion accepts local callers
subject to ArrowCol protocol and schema validation. Readiness and the DFE admin
API do not authenticate local callers; a local peer able to reach the admin
listener can request shutdown. Catalog request signing does not secure those
endpoints. Never bind any of the four ports to a non-loopback interface or
include the key value in arguments, logs or release assets.

## Packaged .NET supervisor

RID-less and `win-*` builds copy the runtime to `<output>/arrowcol-runtime/`:
the two executables, `config.sdk.yaml` (`config.windows.yaml` with admission
limits widened for one collector serving every DMV table) and `runtime.json`
(the `sourceSha` and each file's SHA-256). `WindowsLocalRuntime.StartAsync`
applies the contract above under one service-owned `Root`:

| Step | Behaviour |
| --- | --- |
| Own | Takes an exclusive `Root/owner.lock`, rejects reparse points and refuses to start while any of the four ports is in use |
| Verify | Hashes and holds open each runtime file against `runtime.json`; a mismatch throws `InvalidDataException` |
| Prepare | Creates `state/`, `ddl/`, `contracts/`, `logs/` and `delta/`, an ACL-protected `identity.key`, and one DDL file per catalog table. Compiles missing contracts into `contracts/`. |
| Start | Starts the catalog and then the collector in a kill-on-close job object, each after the previous one is ready. A crash of the owning process takes both down. |
| Run | `Endpoint` is the Flight URI and `DeltaRoot` the warehouse. `Completion` faults if either process exits unexpectedly. |
| Stop | `StopAsync` drains as above, bounded by `DrainTimeout`, and throws `ArrowcolException` unless both exit 0 |
