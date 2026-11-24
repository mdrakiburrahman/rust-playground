# Typed Arrowcol SDK developer guide

Start with the [localhost walkthrough](localhost-demo.md). It builds a registered
contract, runs editable Rust/C++/C# emitters, and inspects actual Delta/Kusto output.

## Two transformations, one shared stream engine

| Piece | Produced by | Responsibility |
| --- | --- | --- |
| `Events` | Ordinary protobuf compilation: prost, protoc C++ or Grpc.Tools C# | Event fields and protobuf presence |
| `ArrowcolEventsContract` | Arrowcol protoc plugin | Pinned schema/identity, validation and protobuf-to-Arrow conversion |
| `ArrowcolEventsStream` | Generated convenience alias/wrapper | Connect the event contract to the shared engine |
| `ArrowcolStream` | Handwritten reusable runtime | Connection, bounded buffering, batching, push, flush and close |

The generated archive bundles the shared runtime source unchanged so it can be
built without publishing a runtime package first. It does **not** generate a new
transport engine for every table. C++ and C# use a table-specific native bridge
into the same Rust engine, following the thin-binding approach used by
[Zerobus C++](https://github.com/databricks/zerobus-sdk/tree/main/cpp) and
[Zerobus .NET](https://github.com/databricks/zerobus-sdk/tree/main/dotnet).
Its guarantees are Arrowcol's, not Zerobus's.

Protobuf is the application model/schema contract. The wire is Arrow Flight;
the adapter builds exact Arrow batches internally. Applications do not implement
interfaces on their events or manually map fields to Arrow arrays.

Only the Arrowcol-specific helpers receive the `Arrowcol` prefix. Ordinary
protobuf classes stay `Events`, reusable independently of the Arrowcol SDK.
The pattern is `Arrowcol<MessageName>Contract` and `Arrowcol<MessageName>Stream`:
a protobuf message named `Demo` produces `ArrowcolDemoContract` and
`ArrowcolDemoStream`, while its ordinary event type remains `Demo`.
The existing protobuf packages, descriptors and schema fingerprints do not change.
New SDK builds retain `EventsContract` and `EventsStream` as compatibility names;
existing archives are immutable, so rebuild a pinned version to obtain the new
helpers rather than editing an old archive.

## Build and consume an SDK

```bash
npx --no-install nx run arrowcol-protoc:build-contract --registry=local --definition=examples/arrowcol-stream/schema.sql
```

Download the archive from its workflow run or use the reported local path.
Extract it to a development directory, retaining the whole `sdk/` tree:

| Path | Integration |
| --- | --- |
| `rust/Cargo.toml` | Cargo path dependency; actual package name is in `sdk-manifest.json` |
| `rust/src/proto.rs` | Ordinary prost-generated event model |
| `cpp/contract.hpp`, `cpp/CMakeLists.txt` | Generated C++ contract plus standard protobuf compilation |
| `csharp/Contract.csproj` | Generated C# contract plus Grpc.Tools protobuf compilation |
| `native/Cargo.toml` | Unique per-table/version native library used by C++ and C# |
| `runtime/` | Shared source libraries and thin language bindings |
| `STREAMS.md` | Contract-specific stream capability/build instructions |
| `descriptors/` | Canonical schema, descriptor set and properties |

The original Rust Arrow row/builders and TypeScript APIs remain available.
The new protobuf-backed stream API initially supports **flat scalar fields**.
Nested/collection schemas still generate their previous APIs, with an explicit
stream capability note; no lossy repeated-field/null-container mapping is hidden.

## Minimal application APIs

In each language below, keep the extracted `sdk/` tree beside your `app/`
directory. A source-level `use`, `using` or `#include` does not replace the
Cargo, .NET or CMake dependency declaration. These examples read
`ARROWCOL_ENDPOINT` from application configuration; see
[endpoint configuration](#configure-the-collector-connection) before running.

### Rust

Create `app/Cargo.toml` with the following content. Replace
`GENERATED_RUST_PACKAGE` with `packages.rust.name` from `sdk/sdk-manifest.json`;
do not guess a version/fingerprint. `sdk` is the application's local import name:

```toml
[package]
name = "events-demo"
version = "0.1.0"
edition = "2024"

[workspace]

[dependencies]
sdk = { package = "GENERATED_RUST_PACKAGE", path = "../sdk/rust" }
tokio = { version = "=1.53.2", features = ["macros", "rt-multi-thread", "signal"] }
```

In `app/src/main.rs`, import the ordinary prost event and the Arrowcol helper.
Cargo compiles and links the generated SDK and bundled runtime automatically;
the generated SDK already contains `rust/src/proto.rs`:

```rust
use sdk::{ArrowcolEventsStream, StreamOptions, proto::Events};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = std::env::var("ARROWCOL_ENDPOINT")?;
    let mut stream = ArrowcolEventsStream::connect(
        &endpoint,
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
You can copy that loop into `app/src/main.rs` instead. The generic engine is
`ArrowcolStream<ArrowcolEventsContract>`; `ArrowcolEventsStream` hides that contract
parameter without requiring traits on consumer-authored event classes.

### C# (.NET)

Keep the extracted `sdk/` directory beside your application's `app/` directory:

```text
demo/
  sdk/                 # Entire extracted SDK, including native/ and runtime/
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
    <ProjectReference Include="../sdk/csharp/Contract.csproj" />
  </ItemGroup>
</Project>
```

This references the SDK assembly, including its generated `Events` message,
contract and stream helper. Building the application invokes Grpc.Tools to
generate and compile the protobuf classes in the referenced SDK project; no
manual copy of `Events.cs` or precompiled managed DLL is needed. A C# `using`
directive alone does **not** add an assembly reference.

From `demo/`, build the native library first:

```bash
cargo build --release --manifest-path sdk/native/Cargo.toml
dotnet build app/Emitter.csproj
```

The native binary must match the application's OS/architecture. Do not override
its target directory for these manual commands; the project copies the matching
library from `native/target/release`. The Nx sample wrapper manages its shared
build cache and copies that library for you.

For the walkthrough's `demo.public.events` table, put this in `app/Program.cs`.
`Events` is the ordinary protobuf class; the helper comes from `Arrowcol.Generated`:

```csharp
using System;
using Arrowcol.Client;
using Arrowcol.Demo.Public;
using Arrowcol.Generated;

var endpoint = Environment.GetEnvironmentVariable("ARROWCOL_ENDPOINT")
    ?? throw new InvalidOperationException("Set ARROWCOL_ENDPOINT");
await using var stream = await ArrowcolEventsStream.ConnectAsync(
    endpoint,
    new StreamOptions { BatchRows = 1024 });
await stream.PushAsync(new Events {
    EventId = 42, ObservedAt = 1_700_000_000_000_000, Payload = "example"
});
await stream.FlushAsync();
await stream.CloseAsync();
```

With the local collector running, run `dotnet run --project app/Emitter.csproj`
from `demo/`. For a different table, use the event type and stream helper named
in your SDK's `csharp/Contract.cs` instead of these demo-specific imports.

`ArrowcolEventsStream.ConnectAsync(endpoint)` is shorthand for
`ArrowcolStream<Events>.ConnectAsync(endpoint, ArrowcolEventsContract.Instance)`.
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

Build `native/Cargo.toml` as above, then create `app/CMakeLists.txt`:

```cmake
cmake_minimum_required(VERSION 3.20)
project(events_demo LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)
add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/../sdk/cpp" contract)
add_executable(emitter main.cpp)
target_link_libraries(emitter PRIVATE arrowcol_contract)
if(WIN32)
  add_custom_command(TARGET emitter POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_if_different
      $<TARGET_FILE:contract_native> $<TARGET_FILE_DIR:emitter>)
endif()
```

`arrowcol_contract` supplies headers, compiled protobuf models, libprotobuf and
the native Arrowcol bridge. CMake invokes ordinary protoc for the model;
`#include "contract.hpp"` alone is not enough to compile/link an application.
For C++ reserved package components such as SQL schema `public`, the bundled
`cpp/model/schema.proto` escapes the language namespace to `public_`. Field
numbers, types and encoded event bytes are unchanged; canonical descriptors and
the registry identity remain untouched. This avoids protoc 3.x generating an
invalid C++ namespace.

Use this `app/main.cpp`, or copy the editable loop from
`examples/arrowcol-stream/cpp/main.cpp`:

```cpp
#include "contract.hpp"
#include <cstdlib>
#include <iostream>
#include <stdexcept>

int main() {
    try {
        const char* endpoint = std::getenv("ARROWCOL_ENDPOINT");
        if (!endpoint || !*endpoint)
            throw std::invalid_argument("Set ARROWCOL_ENDPOINT");
        auto stream = arrowcol_generated::ArrowcolEventsStream::Connect(endpoint);
        arrowcol_generated::ArrowcolEventsContract::Event event;
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

`ArrowcolEventsContract::Event` is an alias for the ordinary protoc class
`::arrowcol::demo::public_::Events`, not an Arrowcol-prefixed event type.
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
| Nx localhost wrapper | Uses the active run's `endpoints.json` and passes `--endpoint`; `--node` chooses the hop |
| Containers on a shared network | Use a real Compose/service DNS name and its internal port, e.g. `http://arrowcol-regional:18830`, only if that service/listener is configured |
| Hosted deployment | Use the operator-provided stable DNS endpoint, e.g. `https://ingest.example.com:443`; this example hostname is not a provisioned service |

The three checked-in loop apps use **`--endpoint` first, then `ARROWCOL_ENDPOINT`**.
Missing or blank configuration fails explicitly; there is no fallback to another
collector. In the Nx wrapper, its explicit active-demo endpoint deliberately
overrides the environment, so a localhost demo cannot accidentally publish to a
hosted destination.

Set configuration in the shell that actually launches the application:

```bash
export ARROWCOL_ENDPOINT=http://localhost:18830
cargo run --manifest-path app/Cargo.toml
# Or: dotnet run --project app/Emitter.csproj
# Or: ./app/build/emitter
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
| Schema version | Pinned to the generated contract; the collector reconciles compatible nullable trailing columns |
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
