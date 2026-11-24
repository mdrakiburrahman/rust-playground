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
The neutral bundle is also independently downloadable; it is not a third required
dependency for Arrowcol applications. Models depend on protobuf, not the Arrowcol
SDK, so non-Arrowcol applications can reuse them. Rust models include protobuf
reflection metadata. C++ and C# use one schema-independent native Rust library;
adding a schema does not require recompiling or releasing that SDK library.

The current distributions are immutable **source archives with Cargo, .NET and
CMake project manifests**, not packages uploaded to public feeds or universal
precompiled binaries. Use local project/path references below. Build native SDK
binaries for the application's OS/architecture. Identical SDK source produces
identical SDK package digests across contracts; contract/model digests change
independently. `packages.json` records the standalone neutral bundle digest.

Protobuf is the application model/schema contract. The wire is Arrow Flight;
the shared runtime builds exact Arrow batches internally. Applications do not implement
interfaces on their events or manually map fields to Arrow arrays.

Ordinary protobuf classes stay `Events` or `Demo`. The new APIs use
`ArrowcolContract` and `ArrowcolStream<Events>` / `ArrowcolStream<Demo>`, not a new
Arrowcol helper class for every event. One stream binds one model and one pinned
contract; it does not accept arbitrary unrelated objects. Existing immutable
archives and the legacy generated helpers remain available for compatibility.

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

The original Rust Arrow row/builders and TypeScript APIs remain available.
The new protobuf-backed stream API initially supports **flat scalar fields**.
Nested/collection schemas still generate their previous APIs and a neutral bundle, with an explicit
stream capability note; no lossy repeated-field/null-container mapping is hidden.

## Minimal application APIs

In each language below, keep the extracted `sdk/` tree beside your `app/`
directory. A source-level `use`, `using` or `#include` does not replace the
Cargo, .NET or CMake dependency declaration. These examples read
`ARROWCOL_ENDPOINT` from application configuration; see
[endpoint configuration](#configure-the-collector-connection) before running.

### Rust

Create `app/Cargo.toml` with the following content. Replace
`GENERATED_MODELS_PACKAGE` with `name` from `sdk/models/rust/package.json`;
do not guess a version/fingerprint. `models` is the application's local import name:

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
You can copy that loop into `app/src/main.rs` instead. Its generic parameter is the
ordinary protobuf event, not an Arrowcol-generated adapter.

### C# (.NET)

Keep the extracted `sdk/` directory beside your application's `app/` directory:

```text
demo/
  sdk/                 # Extracted SDK and models packages: runtime/ and models/
  app/
    Emitter.csproj
    Program.cs
```

Add this project reference in `app/Emitter.csproj` (or add the `ProjectReference`
to your existing project, adjusting its relative path):

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

These reference two assemblies: ordinary `Events` models and the reusable SDK.
Building the application invokes Grpc.Tools to generate and compile the protobuf
classes in the models project; no
manual copy of `Events.cs` or precompiled managed DLL is needed. A C# `using`
directive alone does **not** add an assembly reference.

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
put the event alias in `Schema.cs`. Aliases only shorten names; they do not
generate a new event class.

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
generic header and native SDK. CMake invokes ordinary protoc for the model;
`#include` directives alone are not enough to compile/link an application.
For C++ reserved package components such as SQL schema `public`, the bundled
`models/cpp/model/contract_<table-id>.proto` escapes the language namespace to `public_`. Field
numbers, types and encoded event bytes are unchanged; canonical descriptors and
the registry identity remain untouched. This avoids protoc 3.x generating an
invalid C++ namespace.

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

`Events` is an alias for the ordinary protoc class, not an Arrowcol-prefixed type.
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

| Running the application | Endpoint configuration |
| --- | --- |
| Same host/network namespace as local regional | `ARROWCOL_ENDPOINT=http://localhost:18830` |
| Same host/network namespace as local node | `ARROWCOL_ENDPOINT=http://localhost:18832` |
| Nx localhost wrapper | Passes the active run's endpoint and exact extracted `--contract`; `--node` chooses the hop |
| Containers on a shared network | Use a real Compose/service DNS name and its internal port, e.g. `http://arrowcol-regional:18830`, only if that service/listener is configured |
| Hosted deployment | Use the operator-provided stable DNS endpoint, e.g. `https://ingest.example.com:443`; this example hostname is not a provisioned service |

The three checked-in loop apps use **`--endpoint` first, then `ARROWCOL_ENDPOINT`**.
They also require **`--contract` or `ARROWCOL_CONTRACT`**, pointing to the matching
models package's `contract/` directory. Pin packages at build/restore time; do not
silently fetch a changing registry "latest" during stream creation.
Missing or blank configuration fails explicitly; there is no fallback to another
collector. In the Nx wrapper, its explicit active-demo endpoint deliberately
overrides the environment, so a localhost demo cannot accidentally publish to a
hosted destination.

Set configuration in the shell that actually launches the application:

```bash
export ARROWCOL_ENDPOINT=http://localhost:18830
export ARROWCOL_CONTRACT="$PWD/sdk/models/rust/contract"
cargo run --manifest-path app/Cargo.toml
# For C# or C++, point ARROWCOL_CONTRACT at models/csharp/contract or models/cpp/contract.
```

In Windows CMD, use `set ARROWCOL_ENDPOINT=http://localhost:18830` for an application
built for Windows. A host environment variable is not automatically forwarded by
`docker exec`: pass `-e ARROWCOL_ENDPOINT=...` when launching inside a container.
`localhost` always means the application's own host/container. The documented
collectors bind to loopback inside the devcontainer; publishing/forwarding a port
or changing the listener requires an intentional network configuration, not
substituting a container IP.

DNS keeps an address stable when service IPs change, but does not add authentication,
retry safety or service discovery. Hosted ingress must support Arrow Flight/gRPC
over HTTP/2. HTTPS uses native trusted roots and hostname verification. This SDK
does not yet supply bearer-token/mTLS configuration, so do not claim support for
a hosted endpoint that requires those credentials, disable TLS validation, or
expose unauthenticated demo collectors publicly.

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

## Legacy consumers

The complete developer archive retains generated `rust/`, `typescript/`,
`cpp/`, `csharp/` and `native/` trees. Existing `ArrowcolEventsContract`,
`ArrowcolEventsStream`, `EventsContract` and `EventsStream` helpers belong to that
compatibility path, not to the new independent models packages. Existing releases
are immutable and remain usable with their bundled runtime. Rebuild a committed
version to obtain the new layout; never modify a retained archive in place.
