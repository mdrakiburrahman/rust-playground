# Typed Arrowcol SDK developer guide

Start with the [localhost walkthrough](localhost-demo.md). It builds a registered
contract, runs editable Rust/C++/C# emitters, and inspects actual Delta/Kusto output.

## Two transformations, one shared stream engine

| Piece | Produced by | Responsibility |
| --- | --- | --- |
| `Events` | Ordinary protobuf compilation: prost, protoc C++ or Grpc.Tools C# | Event fields and protobuf presence |
| `EventsContract` | Arrowcol protoc plugin | Pinned schema/identity, validation and protobuf-to-Arrow conversion |
| `EventsStream` | Generated convenience alias/wrapper | Connect the event contract to the shared engine |
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

### Rust

Alias the generated package as `sdk` in your Cargo manifest. Use its generated
package name, not a guessed version/fingerprint:

```rust
use sdk::{EventsStream, StreamOptions, proto::Events};

let mut stream = EventsStream::connect(
    "http://127.0.0.1:18830",
    StreamOptions { batch_rows: 1024, ..Default::default() },
).await?;

stream.push(Events {
    event_id: Some(42),
    observed_at: Some(1_700_000_000_000_000),
    payload: Some("example".to_owned()),
}).await?;
stream.flush().await?;
stream.close().await?;
```

`push_batch(&events)` queues several protobuf events at once. The sample app's
complete `main` and error handling are in `examples/arrowcol-stream/rust/main.rs`.
The generic engine is `ArrowcolStream<EventsContract>`; `EventsStream` hides that
contract parameter without requiring traits on consumer-authored event classes.

### C# (.NET)

Build the native library first, then reference `csharp/Contract.csproj`:

```bash
cargo build --release --manifest-path sdk/native/Cargo.toml
dotnet build sdk/csharp/Contract.csproj
```

The native binary must match the application's OS/architecture. Do not override
its target directory for these manual commands; the project copies the matching
library from `native/target/release`. The Nx sample wrapper manages its shared
build cache and copies that library for you.

```csharp
using Arrowcol.Client;
using Arrowcol.Generated;
using Arrowcol.Demo.Public;

await using var stream = await ArrowcolStream<Events>.ConnectAsync(
    "http://127.0.0.1:18830", EventsContract.Instance,
    new StreamOptions { BatchRows = 1024 });
await stream.PushAsync(new Events {
    EventId = 42, ObservedAt = 1_700_000_000_000_000, Payload = "example"
});
await stream.FlushAsync();
await stream.CloseAsync();
```

`EventsStream.ConnectAsync(endpoint)` is the shorter generated equivalent.
`PushBatchAsync(events)` accepts multiple events. Required SQL fields must have
protobuf presence; leaving a required field unset fails even if its default value
would be zero. `DisposeAsync` releases resources; it does not hide a final flush.

### C++

Build `native/Cargo.toml` as above, add the generated `cpp/` CMake subdirectory,
and link `arrowcol_contract`. CMake invokes ordinary protoc for the model.
For C++ reserved package components such as SQL schema `public`, the bundled
`cpp/model/schema.proto` escapes the language namespace to `public_`. Field
numbers, types and encoded event bytes are unchanged; canonical descriptors and
the registry identity remain untouched. This avoids protoc 3.x generating an
invalid C++ namespace.

```cpp
#include "contract.hpp"

auto stream = arrowcol_generated::EventsStream::Connect("http://127.0.0.1:18830");
arrowcol_generated::EventsContract::Event event;
event.set_event_id(42);
event.set_observed_at(1700000000000000);
event.set_payload("example");
stream.Push(event);
stream.Flush();
stream.Close();
```

`PushBatch(events)` accepts a vector of events. The wrapper is move-only and
throws on errors; the sample catches them and exits nonzero. Its destructor
releases resources without an implicit flush. Serialize access to each stream.
On Windows, deploy its matching DLL alongside the application; do not mix a
Linux `.so` with a Windows .NET/C++ process.

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
