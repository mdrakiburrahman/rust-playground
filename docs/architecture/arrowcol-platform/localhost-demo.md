# Localhost catalogue and ingestion demo

| Component | Local address | Purpose |
| --- | --- | --- |
| Catalogue UI | <http://localhost:4173/lakehouse> | Explore contracts, build SDKs and download artifacts |
| Local / hosted simulation | `18817` / `18818` | Two independent, persistent catalogue authorities |
| Regional Arrowcol | `18830` | Admit SDK batches and commit Delta/Parquet |
| Optional on-node Arrowcol | `18832` | Admit and forward to regional using the same catalogue authority |
| Optional Kusto emulator | `18090` | Query the committed Delta table, without copying rows |

Use the repository devcontainer for the complete demo. Rust, Node/Nx, protoc and
Docker must be available there. C++ additionally requires CMake and protobuf C++
development libraries; C# requires the .NET 8 SDK. Allocate at least 6 GiB to the
optional Kusto container, in addition to the development and catalogue services.
Follow the repository's devcontainer setup before these commands.

For the optional C++/C# samples in the Ubuntu 24.04 devcontainer:

```bash
sudo apt-get update
sudo apt-get install -y cmake libprotobuf-dev dotnet-sdk-8.0
```

**Keep `.temp/contract-builder` and Docker metadata volumes.** They contain real
local contracts, history, releases and data. Do not run broad clean/reset targets
against a demo you need to keep. These services are unauthenticated local
development services; never expose their ports to the public internet.

## 1. Start the catalogue

In the devcontainer terminal:

```bash
npx --no-install nx run arrowcol-sdk-gen:catalog-ui
```

Keep it running and forward port `4173` in your editor. `/`, `/fabric` and
`/lakehouse` remain separate frontends. Select a registry, browse its tables, or
use **Create and build**. Click a workflow run for its pipeline, logs, pinned
version and downloadable archive. The host-visible path is also displayed.

The default hosted choice is a second local service, not a cloud deployment.
For actual local Unity/Hive metadata services, follow
[native catalogue setup](catalog.md#optional-local-unity-catalog-and-hive-services).
After enabling builds and restarting the idle UI, select those authorities from
the same dropdown. Their initial contract scope is flat, unpartitioned v1 tables
and exact rebuilds; this does not adopt arbitrary foreign tables.

## 2. Build the editable sample's SDK

In a second devcontainer terminal:

```bash
npx --no-install nx run arrowcol-protoc:build-contract --registry=local --definition=examples/arrowcol-stream/schema.sql
```

This creates `demo.public.events`: required `event_id`, required `observed_at`
(UTC microseconds), and nullable `payload`. Copy the successful run's `id`.
The workflow compiles and validates the SDK before registering the new contract.
It does not silently overwrite an existing table.

For an existing, intentionally reused sample contract, rebuild instead:

```bash
npx --no-install nx run arrowcol-protoc:build-contract --registry=local --table=demo.public.events --schema-version=1
```

Do not use the fixture generator as a substitute for this committed build.
Changing values in the sample app needs no schema rebuild. Changing the schema
does: use a new contract rather than assuming this CREATE-only demo performs ALTER.

## 3. Prepare and start collectors

Replace `RUN_UUID` with the successful run ID:

```bash
export RUN=RUN_UUID
npx --no-install nx run arrowcol-sdk-gen:stream-demo --action=prepare --run-id="$RUN"
npx --no-install nx run arrowcol-sdk-gen:stream-demo --action=serve --run-id="$RUN" --node
```

Leave `serve` running. Omit `--node` for regional-only ingestion. Both collectors
resolve the selected authority themselves; SDK clients never receive a registry
endpoint. Configurations, logs, extracted SDK and sample build projects are under
`.temp/contract-builder/demos/RUN_UUID/`.

The on-node exporter uses explicit `registration: shared_catalog`: it does not
replay CREATE/ALTER into the regional collector. Existing independent-catalogue
forwarding retains its default DDL-replay behavior. This minimal topology is
best-effort admission; it does not enable Quiver crash-durable buffering or claim
exactly-once delivery. SDK batching is enabled; the node forwards admitted batches
without promising an additional timed aggregation stage.

## 4. Emit data for five minutes

In another terminal:

```bash
export RUN=RUN_UUID
npx --no-install nx run arrowcol-sdk-gen:stream-demo --action=emit --run-id="$RUN"
```

| Option | Meaning |
| --- | --- |
| `--language=rust` | Default; edit `examples/arrowcol-stream/rust/main.rs` |
| `--language=cpp` | Edit `examples/arrowcol-stream/cpp/main.cpp` |
| `--language=csharp` | Edit `examples/arrowcol-stream/csharp/Program.cs` |
| `--duration-seconds=300` | Default five-minute application loop, not an SDK stream lifetime limit |
| `--interval-ms=1000` | Default one event per second |
| `--node` | Send to the optional node instead of directly to regional |
| `--duration-seconds=5` | Short smoke run |

The wrapper compiles the chosen sample and runs it against the active demo.
C++ and C# first build the generated native bridge to the shared Rust runtime.
Build output is retained outside the immutable release archive.
Use `--action=build --language=cpp` (or `rust`/`csharp`) to compile without sending.
Samples use deterministic synthetic timestamps and values for exact comparison;
edit the event construction to use your own values or current timestamps.

Run the app directly if you need its own Ctrl+C handler to flush before exit;
the wrapper is primarily a bounded-duration runner. Killing a process does not
guarantee a final flush. All samples explicitly flush and close on normal exit.
See the [SDK developer guide](sdk-developer-guide.md) for the minimal APIs.

### Windows Command Prompt

These are **CMD**, not PowerShell, commands. They invoke the existing Linux
devcontainer through WSL; they do not claim a native Windows collector build.
Replace the distribution/container names if yours differ. Run `serve` in a
separate CMD window, and keep the catalogue terminal open:

```bat
set RUN=RUN_UUID
wsl -d Ubuntu-24.04 -u root -- docker exec -u vscode -w /workspaces/rust-playground rust-playground_devcontainer-workspace-1 npx --no-install nx run arrowcol-sdk-gen:stream-demo --action=prepare --run-id=%RUN%
wsl -d Ubuntu-24.04 -u root -- docker exec -u vscode -w /workspaces/rust-playground rust-playground_devcontainer-workspace-1 npx --no-install nx run arrowcol-sdk-gen:stream-demo --action=serve --run-id=%RUN% --node
```

In another CMD window:

```bat
set RUN=RUN_UUID
wsl -d Ubuntu-24.04 -u root -- docker exec -u vscode -w /workspaces/rust-playground rust-playground_devcontainer-workspace-1 npx --no-install nx run arrowcol-sdk-gen:stream-demo --action=emit --run-id=%RUN% --language=rust --node
```

The same wrapper accepts `--language=cpp` or `--language=csharp`. Source edits in
the Windows checkout are visible through its devcontainer bind mount.

## 5. Inspect committed Delta and optional Kusto

After an emitter completes, allow the regional Delta export to commit:

```bash
npx --no-install nx run arrowcol-sdk-gen:stream-demo --action=inspect --run-id="$RUN"
```

This reads the active Parquet files named by Delta Add/Remove actions, not every
file in the directory. It records `committed-files.json` and `committed-rows.json`.
Inspecting immediately after admission may show fewer rows; admission is not a
sink-visibility barrier. Repeated emit runs append data and can repeat sample IDs.

**`kusto-up` starts a local 6 GiB emulator with `ACCEPT_EULA=Y`; run it only if you
accept the emulator license.** It mounts the selected warehouse read-only and
retains a named Kusto data volume:

```bash
npx --no-install nx run arrowcol-sdk-gen:stream-demo --action=kusto-up --run-id="$RUN"
npx --no-install nx run arrowcol-sdk-gen:stream-demo --action=kusto-query --run-id="$RUN"
```

The helper uses the same pinned emulator image as the upstream loadtest. It
creates `ArrowcolEvents` as a native Delta external table in `NetDefaultDB`;
it does not copy events into a separate Kusto ingestion pipeline.
The query result is also saved as `kusto-rows.json`. Use the printed endpoint
with a KQL client:

```kusto
external_table("ArrowcolEvents") | take 100
```

The optional Kusto Explorer is not required for ingestion. Follow the
[Kusto Explorer README](../../../projects/kusto-explorer/README.md#standalone-container);
attach it to the emulator's Docker network and configure the printed internal
endpoint, with default database `NetDefaultDB`.

## Stop and troubleshoot

| Symptom/action | Guidance |
| --- | --- |
| Stop collectors | Ctrl+C in `serve`; it stops only its child processes and retains all data |
| Stop Kusto | `npx --no-install nx run arrowcol-sdk-gen:stream-demo --action=kusto-stop --run-id="$RUN"` |
| Port conflict | Use `serve --port-base=18930`; Kusto uses independent `--kusto-port=18091` |
| Table already exists | Rebuild its pinned version; do not replay CREATE or delete the warehouse |
| Missing generated stream adapter | Rebuild with the new generator; initial protobuf stream support is flat scalars |
| Native compiler/toolchain missing | Install the missing CMake/protobuf C++ or .NET 8 SDK in the devcontainer |
| `outcome_unknown` | Retain source records and reconcile committed data; do not blindly retry |
| Stale `serve.lock` after a crash | Inspect its PID and logs before removing that exact lock; never kill unrelated processes |
| Fewer Delta/Kusto rows than admitted | Check regional logs and wait for committed output; an acknowledgement is not proof of persistence |

For repeatable qualification, use a fresh, empty `events` table build and run
`CONTRACT_STREAM_RUN=RUN_UUID npx --no-install nx run arrowcol-sdk-gen:stream-demo-e2e`.
It compiles all three languages before sending, exercises both direct and node
paths, compares every committed value in Delta and Kusto, then stops only its own
collectors/emulator. It retains data and evidence and refuses a nonempty table.
