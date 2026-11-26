# Typed Arrowcol SDK developer guide

Start with the [localhost walkthrough](localhost-demo.md). It builds a registered
contract, runs editable Rust/C++/C# emitters, and inspects actual Delta/Kusto output.

## Two dependencies per application

| Package | Contents | Versioned independently |
| --- | --- | --- |
| Arrowcol SDK, per language | Generic `ArrowcolStream<Event>`, contract loading, validation and transport | SDK release; shared across schemas |
| Schema models, per language | Ordinary protobuf `Events` classes plus the exact embedded contract | Contract release |
| Standalone neutral contract | `.proto`, option definitions/imports, descriptors, Arrow schema and manifest | Same contract release; usable without Arrowcol |

An application references **its language SDK and its language models package**.
The models package embeds the neutral contract and depends only on protobuf,
so it also works in applications that do not use Arrowcol. Rust models include
protobuf reflection metadata. C++ and C# share a schema-independent native Rust
library. The neutral bundle is available as a separate download for other tools.

Contract workflow distributions are immutable **source archives with Cargo, .NET and
CMake project manifests**. Use the local project/path references below and build
native SDK binaries for the application's OS/architecture, or use the C# NuGet
runtime below. Identical SDK source produces
identical SDK package digests across contracts; contract/model digests change
independently. `packages.json` records the standalone neutral bundle digest.

Applications construct protobuf events; the SDK validates them, builds Arrow
batches and sends them over Arrow Flight.

Use `ArrowcolContract` with `ArrowcolStream<Events>` or `ArrowcolStream<Demo>`.
`Events` and `Demo` are ordinary protobuf classes. Each stream accepts one model
type and binds to one pinned contract.

## Build and consume contract packages

```bash
npx --no-install nx run arrowcol-protoc:build-contract --registry=local --definition=examples/arrowcol-stream/schema.sql
```

Download the archive from its workflow run or use the reported local path.
The workflow exposes seven individual archives for a supported flat schema:
`contract`, `models-rust/cpp/csharp`, and `sdk-rust/cpp/csharp`. Extract your
language's SDK and models archives into the same `sdk/` directory. Alternatively,
extract the complete developer archive, retaining its `sdk/` tree:

| Path | Integration |
| --- | --- |
| `models/rust/Cargo.toml` | Models crate; actual package name is in `models/rust/package.json` |
| `models/cpp/CMakeLists.txt` | Models library compiled with standard protoc |
| `models/csharp/Models.csproj` | Models assembly compiled with Grpc.Tools |
| `models/<language>/contract/` | Byte-identical embedded neutral contract |
| `runtime/arrowcol-sdk-runtime/` | Reusable Rust SDK crate |
| `runtime/cpp/`, `runtime/csharp/` | Reusable C++ / .NET SDK projects |
| `runtime/arrowcol-sdk-ffi/` | Shared native SDK, independent of event types |
| `STREAMS.md`, `sdk-manifest.json` | Complete developer bundle's integration/capability metadata |

The protobuf-backed stream API supports **flat scalar fields**.
For nested or collection schemas, use the generated Rust or TypeScript Arrow
builders. Those schemas include a neutral bundle and a stream capability note.

### Local catalog seed

Run the standalone tool next to a running local catalog. With no `--ddl` it
registers the 32 canonical public DMV tables; supply a repeatable `--ddl <file>`
for explicit managed `CREATE TABLE` definitions, one statement per file instead.
Set `ARROWCOL_CATALOG_DDL_IDENTITY_SECRET_FILE` for the signed catalog, seed
tool, and collector. Use the same writable warehouse path in both services.
If the catalog uses `--ingestion-layout`, set `ARROWCOL_PARTITION_GRANULARITY`
on the collector to `minute`, `hour`, or `day`; otherwise the collector can
admit rows but cannot commit the partitioned table.

```bash
arrowcol-sdk-gen catalog seed --endpoint http://127.0.0.1:8817 --out target/dmv-contracts
```

The tool is also included in the catalog image (`--entrypoint
/usr/local/bin/arrowcol-sdk-gen`). It writes one SDK directory per
`catalog.schema.table`; the JSON result lists the pinned v1 fingerprint and
`models/csharp` path for each. Existing exact definitions are reused, while
schema drift is rejected. Each table is committed separately; if the operation
fails, correct the failure and rerun. Mount the output directory inside the
tool's working directory; keep private DDL and its output local.

### C# NuGet runtime

`Rust.Playground` provides the `Arrowcol.Client` runtime for .NET 8,
with native assets for Windows x64 and Linux x64 (glibc 2.34 or later).
Reference a version from your approved Azure Artifacts source and reference the
generated `Models.csproj` separately; no Rust toolchain is needed by consumers.
The model's `contract/` directory must travel with the application.

| Producer command | Requirements / result |
| --- | --- |
| `npx --no-install nx run arrowcol-sdk-gen-tool:nuget-pack -- --package-version 0.1.0-alpha.1` | Linux x64 devcontainer, .NET 8 SDK, `x86_64-pc-windows-gnu` Rust target and MinGW POSIX linker; outputs the exact `.nupkg` under `target/arrowcol-nuget/` |
| `npx --no-install nx run arrowcol-sdk-gen-tool:nuget-publish -- --package-version 0.1.0-alpha.1` | Explicit upload of a previously packed version. Supply the Azure Artifacts NuGet v3 `NUGET_URL` and packaging-write `NUGET_PAT` using private devbox settings or the ignored `.env`; do not commit them. The publishing identity needs feed publisher access. |

To check the package without a source project reference, run
`examples/arrowcol-stream/csharp/nuget-smoke/NuGetSmoke.csproj` with
`-p:ArrowcolPackageVersion=<version>` and an authorized source or the local
package directory. This checks native library resolution on the executing RID,
not collector admission or Delta commit.

## Minimal application APIs

### Stream, connection and contract

| Input or object | Meaning |
| --- | --- |
| `ArrowcolContract` | The pinned table identity, schema version, fingerprint and schema artifacts used to validate and route events |
| Contract path passed to `load` / `Load` | A local **directory** from the extracted models package, such as `sdk/models/rust/contract/`; not a registry URL, archive or single `.proto` file |
| Endpoint passed to `connect` / `ConnectAsync` / `Connect` | The collector's **Arrow Flight/gRPC service URI**, such as `http://127.0.0.1:18830`; not the catalogue UI or a website |
| `ArrowcolStream<Events>` | A producer for one model type and pinned contract, with buffering and an internally owned transport connection |

A connection and a stream are different concepts: the connection reaches a
collector, while the stream identifies what table/schema its events belong to.
**The current SDK combines their creation in `ArrowcolStream.connect(...)`.**
It checks the compiled model against the contract and opens an internal gRPC
channel. There is no separate public `ArrowcolConnection` to create first or
share between streams. Create another stream for another contract or independent
producer. `push`/`PushAsync` accepts events of that stream's model; it does not
take a new contract or endpoint on each call.

The contract directory must contain these files together:

| File | Runtime use |
| --- | --- |
| `manifest.json` | Table identity, pinned schema version, fingerprint and field mappings |
| `schema.arrow` | Serialized Arrow schema |
| `descriptor.pb` | Compiled protobuf descriptors and contract annotations |

The bundle also includes `.proto` sources for tooling, but a `.proto` alone is
not the runtime contract. Deploy the matching `contract/` directory alongside
the application or mount it read-only. Loading it does not fetch anything from
the registry. Use an absolute path where possible: relative paths resolve from
the application's **working directory**, not its executable or models assembly.
Inside a container, use the path visible inside that container.

The SDK takes the endpoint string and loaded contract object as arguments.
`ARROWCOL_ENDPOINT` and `ARROWCOL_CONTRACT` are configuration names chosen by
these examples, not SDK-defined globals or automatically populated settings.
The application reads them, then passes their values to the SDK. You can instead
read your own configuration file or deployment settings.

### Application layout

In each language below, keep the extracted `sdk/` tree beside your `app/`
directory. A source-level `use`, `using` or `#include` does not replace the
Cargo, .NET or CMake dependency declaration. Set both
`ARROWCOL_ENDPOINT` and `ARROWCOL_CONTRACT` before running these examples; see
[endpoint configuration](#configure-the-collector-connection) before running.

### Rust

Create `app/Cargo.toml` with the following content. Replace
`GENERATED_MODELS_PACKAGE` with `name` from `sdk/models/rust/package.json`;
`models` is the application's local import name:

```toml
[package]
name = "events-demo"
version = "0.1.0"
edition = "2024"

[workspace]

[dependencies]
models = { package = "GENERATED_MODELS_PACKAGE", path = "../sdk/models/rust" }
arrowcol-sdk-runtime = { path = "../sdk/runtime/arrowcol-sdk-runtime" }
tokio = { version = "=1.53.2", features = ["macros", "rt-multi-thread", "signal"] }
```

In `app/src/main.rs`, import the ordinary prost event and the reusable SDK.
Cargo compiles the two separate packages:

```rust
use arrowcol_sdk_runtime::{ArrowcolContract, ArrowcolStream, StreamOptions};
use models::Events;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = std::env::var("ARROWCOL_ENDPOINT")?;
    let contract = ArrowcolContract::load(std::env::var("ARROWCOL_CONTRACT")?)?;
    let mut stream = ArrowcolStream::<Events>::connect(
        &endpoint,
        contract,
        StreamOptions { batch_rows: 1024, ..Default::default() },
    ).await?;
    stream.push(Events {
        event_id: Some(42),
        observed_at: Some(1_700_000_000_000_000),
        payload: Some("example".to_owned()),
    }).await?;
    stream.flush().await?;
    stream.close().await?;
    Ok(())
}
```

From the directory containing `app/` and `sdk/`, run
`cargo run --manifest-path app/Cargo.toml`.
`push_batch(&events)` queues several protobuf events at once. The sample app's
complete `main` and error handling are in `examples/arrowcol-stream/rust/main.rs`.
You can copy that loop into `app/src/main.rs` instead.

### C# (.NET)

Keep the extracted `sdk/` directory beside your application's `app/` directory:

```text
demo/
  sdk/                 # Extracted SDK and models packages: runtime/ and models/
  app/
    Emitter.csproj
    Program.cs
```

Add these project references in `app/Emitter.csproj`, adjusting their relative
paths if your directory layout differs:

```xml
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <OutputType>Exe</OutputType>
    <TargetFramework>net8.0</TargetFramework>
    <Nullable>enable</Nullable>
  </PropertyGroup>
  <ItemGroup>
    <ProjectReference Include="../sdk/models/csharp/Models.csproj" />
    <ProjectReference Include="../sdk/runtime/csharp/Arrowcol.Sdk.csproj" />
  </ItemGroup>
</Project>
```

These reference two assemblies: `Events` models and the Arrowcol SDK.
Building the application invokes Grpc.Tools to generate and compile the protobuf
classes in the models project.

From `demo/`, build the native library first:

```bash
cargo build --release --manifest-path sdk/runtime/arrowcol-sdk-ffi/Cargo.toml
dotnet build app/Emitter.csproj
```

The native binary must match the application's OS/architecture. Do not override
its target directory for these manual commands; the project copies the matching
library from `runtime/arrowcol-sdk-ffi/target/release`. The Nx sample wrapper manages its shared
build cache and copies that library for you.

For the walkthrough's `demo.public.events` table, put this in `app/Program.cs`.
`Events` is the ordinary protobuf class; the SDK API comes from `Arrowcol.Client`:

```csharp
using System;
using Arrowcol.Client;
using Arrowcol.Demo.Public;

var endpoint = Environment.GetEnvironmentVariable("ARROWCOL_ENDPOINT")
    ?? throw new InvalidOperationException("Set ARROWCOL_ENDPOINT");
var contract = ArrowcolContract.Load(Environment.GetEnvironmentVariable("ARROWCOL_CONTRACT")
    ?? throw new InvalidOperationException("Set ARROWCOL_CONTRACT"));
await using var stream = await ArrowcolStream<Events>.ConnectAsync(
    endpoint,
    contract,
    new StreamOptions { BatchRows = 1024 });
await stream.PushAsync(new Events {
    EventId = 42, ObservedAt = 1_700_000_000_000_000, Payload = "example"
});
await stream.FlushAsync();
await stream.CloseAsync();
```

With the local collector running, run `dotnet run --project app/Emitter.csproj`
from `demo/`. For a different table, use the event type named in
`models/csharp/package.json` instead of these demo-specific imports.

`PushBatchAsync(events)` accepts multiple events. Required SQL fields must have
protobuf presence; leaving a required field unset fails even if its default value
would be zero. `DisposeAsync` releases resources; it does not hide a final flush.

The checked-in loop sample is `examples/arrowcol-stream/csharp/Program.cs`.
The Nx demo's `prepare` action generates its `Emitter.csproj` and a `Schema.cs`
containing `global using Events = global::Arrowcol.Demo.Public.Events;` under
`.temp/contract-builder/demos/<run-id>/apps/csharp/`. Those project files are
local, Git-ignored outputs, not checked-in examples. When copying the loop into
your own project, add `using Arrowcol.Demo.Public;` to `Program.cs` as above, or
put the event alias in `Schema.cs`.

### C++

Build the shared native SDK as above, then create `app/CMakeLists.txt`:
replace `GENERATED_MODELS_TARGET` with `name` from `sdk/models/cpp/package.json`.
In `main.cpp`, replace `GENERATED_MODELS_HEADER` with `header` from that same file.
Package and generated file names include table identity so same-shaped tables
in different namespaces do not collide.

```cmake
cmake_minimum_required(VERSION 3.20)
project(events_demo LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)
add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/../sdk/models/cpp" models)
add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/../sdk/runtime/cpp" arrowcol)
add_executable(emitter main.cpp)
target_link_libraries(emitter PRIVATE GENERATED_MODELS_TARGET arrowcol_sdk)
if(WIN32)
  add_custom_command(TARGET emitter POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_if_different
      $<TARGET_FILE:arrowcol_native> $<TARGET_FILE_DIR:emitter>)
endif()
```

The contract-specific CMake target supplies protobuf models; `arrowcol_sdk` supplies the
generic header and native SDK. CMake invokes protoc to compile the model.
Reserved C++ namespace components are escaped: SQL schema `public` becomes
`public_`. Use the fully qualified model name from `models/cpp/package.json`.

Use this `app/main.cpp`, or copy the editable loop from
`examples/arrowcol-stream/cpp/main.cpp`:

```cpp
#include "arrowcol_client.hpp"
#include "GENERATED_MODELS_HEADER"
#include <cstdlib>
#include <iostream>
#include <stdexcept>

int main() {
    try {
        const char* endpoint = std::getenv("ARROWCOL_ENDPOINT");
        if (!endpoint || !*endpoint)
            throw std::invalid_argument("Set ARROWCOL_ENDPOINT");
        const char* contract = std::getenv("ARROWCOL_CONTRACT");
        if (!contract || !*contract)
            throw std::invalid_argument("Set ARROWCOL_CONTRACT");
        using Events = ::arrowcol::demo::public_::Events;
        auto stream = arrowcol::ArrowcolStream<Events>::Connect(
            endpoint, arrowcol::ArrowcolContract::Load(contract));
        Events event;
        event.set_event_id(42);
        event.set_observed_at(1700000000000000);
        event.set_payload("example");
        stream.Push(event);
        stream.Flush();
        stream.Close();
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
```

`Events` is an alias for the protobuf class.
The loop sample uses a `Schema.hpp` alias generated by `stream-demo prepare`;
when copying that loop, provide this alias or use the fully qualified model name.
From the directory containing `app/` and `sdk/`, build and run on Linux:

```bash
cmake -S app -B app/build
cmake --build app/build --config Release
./app/build/emitter
```

`PushBatch(events)` accepts a vector of events. The wrapper is move-only and
throws on errors; the sample catches them and exits nonzero. Its destructor
releases resources without an implicit flush. Serialize access to each stream.
On Windows, deploy its matching DLL alongside the application; do not mix a
Linux `.so` with a Windows .NET/C++ process.

## Configure the collector connection

Keep deployment addresses out of event definitions and generated code. Pass a
configured Flight URI into `connect`/`ConnectAsync`/`Connect`; the shared runtime
does not read environment variables or discover a collector on your behalf.

### Where configuration is defined

| Setting | Who supplies it | Who reads it |
| --- | --- | --- |
| `ARROWCOL_ENDPOINT` | You, in the application's launch environment; value is the reachable collector URI | The application, before creating a stream |
| `ARROWCOL_CONTRACT` | You, in the same environment; value is the matching extracted `contract/` directory path | The application, before calling `ArrowcolContract.load` / `Load` |
| Nx demo arguments | `stream-demo --action=emit` supplies `--endpoint` from the run's `endpoints.json` and `--contract` from its extracted models package | The selected sample application; no environment-variable setup is needed for this wrapper |

The three checked-in loop apps use **`--endpoint` first, then `ARROWCOL_ENDPOINT`**.
Similarly, **`--contract` takes precedence over `ARROWCOL_CONTRACT`**. Their
configuration readers are in
[`rust/main.rs`](../../../examples/arrowcol-stream/rust/main.rs),
[`csharp/Program.cs`](../../../examples/arrowcol-stream/csharp/Program.cs) and
[`cpp/main.cpp`](../../../examples/arrowcol-stream/cpp/main.cpp).
The minimal examples above read environment variables only.
Pin packages at build/restore time; do not
silently fetch a changing registry "latest" during stream creation.
Missing or blank configuration fails explicitly; there is no fallback to another
collector. In the Nx wrapper, its explicit active-demo endpoint deliberately
overrides the environment, so a localhost demo cannot accidentally publish to a
hosted destination. Run the application directly when configuring a remote collector.

### What an endpoint looks like

Use **`http://HOST:PORT` or `https://HOST:PORT`**. `HOST` can be a DNS name or IP
address reachable from the application. The port must serve Arrow Flight/gRPC
over HTTP/2, not the collector's admin/health API. Although it looks like a website
URL, this is a service address for the SDK, not a page to open in a browser.
Do not append a table name, contract path, `/lakehouse` or REST ingestion path.

| Collector location relative to the application | Example endpoint | Required setup |
| --- | --- | --- |
| Same VM and network namespace | `http://127.0.0.1:18830` | Collector listens on loopback port `18830`; `localhost` is also valid if it resolves to the listening address |
| Same VM, optional forwarding collector in the demo | `http://127.0.0.1:18832` | Start the demo with `--node`; use `--node` on wrapper emission too |
| Another container on a shared Docker network | `http://arrowcol-regional:18830` | Configure that service name and a listener reachable from the app container; the name is illustrative |
| Remote VM behind private TLS ingress | `https://collector.example.com:443` | Operator supplies DNS, a trusted TLS certificate and HTTP/2 gRPC ingress forwarding to the collector; this hostname is illustrative, not a running service |

Ports `18830` and `18832` are demo defaults, not protocol requirements. An IP
address can replace DNS; for HTTPS, the certificate must also be valid for that
IP. DNS does not configure a collector or add authentication.

### Application and collector on the same VM

Start the collector first. From the directory containing `app/` and `sdk/`,
set configuration in the shell that actually launches the application:

```bash
export ARROWCOL_ENDPOINT=http://127.0.0.1:18830
export ARROWCOL_CONTRACT="$PWD/sdk/models/rust/contract"
cargo run --manifest-path app/Cargo.toml
# For C# or C++, point ARROWCOL_CONTRACT at models/csharp/contract or models/cpp/contract.
```

For a Windows-native application and a collector reachable on Windows loopback,
set both variables in **Windows CMD**:

```bat
set "ARROWCOL_ENDPOINT=http://127.0.0.1:18830"
set "ARROWCOL_CONTRACT=%CD%\sdk\models\rust\contract"
cargo run --manifest-path app\Cargo.toml
```

`localhost`/`127.0.0.1` means the application's own network namespace, not
necessarily every process on the VM. The walkthrough's collectors bind to
loopback **inside the devcontainer**. Run the app there, or intentionally set up
a host port forward before using a Windows-native app. For separate containers,
configure a shared network and reachable listener; changing only the client's
hostname does not expose a loopback-only collector. `docker exec` does not
automatically forward host variables: pass both with `-e` if needed, using a
container-visible contract path.

### Application and collector on different VMs

On the **collector VM**, the operator must configure its Arrow Flight receiver
with a reachable listener (`config.listening_addr`, for example
`10.20.0.15:18830` on a private interface), its catalogue and its storage or
forwarding destination. Keep the admin listener private. For this HTTPS example,
configure TLS ingress on port `443` forwarding gRPC/HTTP/2 to that receiver,
with DNS and a trusted certificate for `collector.example.com`. Restrict access
to the intended applications using private networking and firewall rules.
The localhost demo wrapper does not provision this deployment.

On the **application VM**, deploy the application and its matching models
contract. Configure the reachable ingress address, not `localhost` and not the
collector VM's contract filesystem path:

```bash
# Illustrative remote deployment; replace both values with your real configuration.
export ARROWCOL_ENDPOINT=https://collector.example.com:443
export ARROWCOL_CONTRACT=/opt/events-app/sdk/models/rust/contract
```

Launch the compiled app in that environment. Its stream-creation code is unchanged.
The collector must resolve the contract's table in the intended catalogue
authority; an unrelated registry with a same-named table is not interchangeable.
To send through a same-VM forwarding collector instead, change the app's endpoint
to that local listener. The forwarding collector's configuration, not the SDK,
chooses the remote destination.

HTTPS uses native trusted roots and hostname verification. This SDK does not
support bearer-token or mTLS configuration. Use an endpoint compatible with its
authentication capabilities, keep TLS validation enabled, and never expose
unauthenticated demo collectors publicly. Plain HTTP is for trusted local
development; DNS or private addressing alone does not encrypt traffic.

## Options, schema and delivery semantics

| Contract | Behavior |
| --- | --- |
| Endpoint | Only node/regional Flight endpoint; no catalogue argument |
| Schema version | Pinned to the loaded contract; compiled model descriptors must match; collector reconciles compatible nullable trailing columns |
| Optional DDL properties | Preserved in .proto and compiled descriptors; recorded, not enforced by the SDK |
| SQL required fields | Checked from ordinary protobuf field presence |
| Timestamp | Signed UTC microseconds |
| Decimal | Exact numeric decimal string, e.g. `"12.34"`; no floating-point rounding |
| Narrow/fixed-width values | Range/length validation before enqueue |
| Push | Copies/validates and waits for bounded queue capacity, not a network ack per event |
| Batch defaults | 1,024 rows or 1 MiB Arrow buffers, with a 1-second flush interval |
| Buffer default | 32 MiB retained Arrow buffers; not a bound on total RSS or transient encoding |
| Timeout default | 30 seconds for capacity, each RPC and a whole flush barrier |
| Oversized call | Explicit failure before enqueue; split the input rather than silently dropping rows |
| Flush | Validated admission of preceding events by the selected collector |
| Close | Explicit final flush plus worker cleanup; errors remain observable |
| Network failure | Latched error, no automatic resend; reconcile unknown outcomes before retry |
| TLS | HTTPS uses native roots; HTTP is only for trusted local development |

Sending to node instead of regional changes only the endpoint. Node admission
does not prove regional admission; neither alone proves a Delta commit or Kusto
visibility. A durable-admission response means the admitting collector's local
durable buffer, not every downstream sink. This SDK does not implement cloud
authentication, registry provisioning or collector lifecycle policy enforcement.

An SDK does not have to send newly added nullable columns: older supported
contracts still emit their pinned subset, and collectors supply trailing nulls.
`v1`, `v2`, etc. identify registered schema revisions, not a separate SDK SemVer
minor-version negotiation protocol.
