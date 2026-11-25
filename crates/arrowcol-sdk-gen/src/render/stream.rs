use super::{RenderContext, pascal_identifier, rust_identifier};
use crate::{ArtifactBundle, SdkGenError, SdkGenResult};
use arrowcol_catalog::CatalogType;
use std::{collections::BTreeMap, path::PathBuf};

pub(super) fn supported(context: &RenderContext<'_>) -> bool {
    context.schema.fields().iter().all(|field| {
        !matches!(
            field.mapping().arrow(),
            CatalogType::Struct { .. }
                | CatalogType::List { .. }
                | CatalogType::Map { .. }
                | CatalogType::Dictionary { .. }
        )
    })
}

pub(super) fn render(context: &RenderContext<'_>, bundle: &ArtifactBundle) -> SdkGenResult<BTreeMap<PathBuf, Vec<u8>>> {
    let mut files = super::runtime::render();
    if !supported(context) {
        files.insert(PathBuf::from("STREAMS.md"), b"# Typed streams\n\nThe new protobuf-backed stream adapters currently support flat scalar schemas only.\nThis schema has collections or nested fields. Existing Rust/TypeScript Arrow builders remain available;\nno lossy protobuf-to-Arrow adapter is generated (repeated fields cannot represent nullable collections).\n".to_vec());
        return Ok(files);
    }
    let proto = bundle
        .descriptors()
        .file
        .iter()
        .find(|file| file.name() == "schema.proto")
        .ok_or_else(|| SdkGenError::Artifact("missing table protobuf file".into()))?;
    let event = proto
        .message_type
        .last()
        .ok_or_else(|| SdkGenError::Artifact("missing event message".into()))?
        .name();
    let contract = format!("Arrowcol{event}Contract");
    let stream = format!("Arrowcol{event}Stream");
    let prefix = format!(
        "arrowcol_{}_v{}",
        context.schema.table_id().to_string().replace('-', ""),
        context.schema.version()
    );
    let mut conversions = String::new();
    for field in context.schema.fields() {
        let name = rust_identifier(field.name());
        let value = match field.mapping().arrow() {
            CatalogType::Int8 => format!("event.{name}.map(|v| i8::try_from(v).map_err(Error::invalid)).transpose()?"),
            CatalogType::Int16 => {
                format!("event.{name}.map(|v| i16::try_from(v).map_err(Error::invalid)).transpose()?")
            }
            CatalogType::UInt8 => format!("event.{name}.map(|v| u8::try_from(v).map_err(Error::invalid)).transpose()?"),
            CatalogType::UInt16 => {
                format!("event.{name}.map(|v| u16::try_from(v).map_err(Error::invalid)).transpose()?")
            }
            CatalogType::Decimal128 { precision, scale } => format!(
                "event.{name}.as_deref().map(|v| arrowcol_sdk_runtime::decimal128(v, {precision}, {scale})).transpose()?"
            ),
            CatalogType::Binary
            | CatalogType::LargeBinary
            | CatalogType::BinaryView
            | CatalogType::FixedSizeBinary { .. }
            | CatalogType::Utf8
            | CatalogType::LargeUtf8
            | CatalogType::Utf8View => format!("event.{name}.clone()"),
            _ => format!("event.{name}"),
        };
        let value = if field.is_nullable() {
            value
        } else {
            format!(
                "{value}.ok_or_else(|| Error::invalid({:?}))?",
                format!("required column {} was not supplied", field.name())
            )
        };
        conversions.push_str(&format!("{name}: {value},\n"));
    }
    let rust = format!(
        r#"use arrowcol_sdk_runtime::{{Contract, Error, Result}};
use super::*;
pub use arrowcol_sdk_runtime::{{GeneratedStream as ArrowcolStream, StreamOptions}};
pub struct {contract};
pub type {stream} = ArrowcolStream<{contract}>;
pub use {contract} as {event}Contract;
pub type {event}Stream = {stream};
impl Contract for {contract} {{
    type Event = proto::{event};
    fn schema() -> arrow_schema::SchemaRef {{ arrow_schema() }}
    fn record_batch(events: &[Self::Event]) -> Result<arrow_array::RecordBatch> {{
        let rows = events.iter().map(|event| Ok({row} {{
            {conversions}
        }})).collect::<Result<Vec<_>>>()?;
        record_batch(&rows).map_err(Error::invalid)
    }}
    fn descriptor() -> Vec<String> {{ flight_descriptor().path }}
    fn metadata() -> Vec<(&'static str, String)> {{
        RoutingMetadata {{ pool: ImplementationPool::Dataflow }}.ascii_pairs().to_vec()
    }}
    fn fingerprint() -> &'static str {{ SCHEMA_FINGERPRINT }}
    fn version() -> u64 {{ SCHEMA_VERSION }}
}}
"#,
        row = context.row_name
    );
    files.insert(
        "rust/src/stream.rs".into(),
        super::rust::format_rust(&rust)?.into_bytes(),
    );
    files.insert(
        "native/Cargo.toml".into(),
        format!(
            r#"[package]
name = "{prefix}"
version = "0.1.0"
edition = "2024"
publish = false
[workspace]
[lib]
crate-type = ["cdylib", "staticlib"]
[dependencies]
sdk = {{ package = "{package}", path = "../rust" }}
arrowcol-sdk-ffi = {{ path = "../runtime/arrowcol-sdk-ffi" }}
"#,
            package = context.rust_package
        )
        .into_bytes(),
    );
    let native = format!(
        r#"use std::ffi::{{c_char, c_void}};
#[unsafe(no_mangle)]
pub unsafe extern "C" fn {prefix}_open(endpoint: *const c_char, options: *const c_char, out: *mut *mut c_void) -> *mut c_char {{
    unsafe {{ arrowcol_sdk_ffi::open::<sdk::{contract}>(endpoint, options, out) }}
}}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn {prefix}_push(handle: *mut c_void, data: *const u8, len: usize, batch: u8) -> *mut c_char {{
    unsafe {{ arrowcol_sdk_ffi::push(handle, data, len, batch != 0) }}
}}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn {prefix}_flush(handle: *mut c_void) -> *mut c_char {{ unsafe {{ arrowcol_sdk_ffi::flush(handle) }} }}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn {prefix}_close(handle: *mut c_void) -> *mut c_char {{ unsafe {{ arrowcol_sdk_ffi::close(handle) }} }}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn {prefix}_destroy(handle: *mut c_void) {{ unsafe {{ arrowcol_sdk_ffi::destroy(handle) }} }}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn {prefix}_free_error(error: *mut c_char) {{ unsafe {{ arrowcol_sdk_ffi::free_error(error) }} }}
"#
    );
    files.insert(
        "native/src/lib.rs".into(),
        super::rust::format_rust(&native)?.into_bytes(),
    );
    // protoc 3.x does not escape C++ keywords in protobuf package namespaces (notably SQL "public").
    let cpp_package = proto.package().split('.').map(|part| {
        const KEYWORDS: &str = "alignas alignof and and_eq asm auto bitand bitor bool break case catch char char8_t char16_t char32_t class compl concept const consteval constexpr constinit const_cast continue co_await co_return co_yield decltype default delete do double dynamic_cast else enum explicit export extern false float for friend goto if inline int long mutable namespace new noexcept not not_eq nullptr operator or or_eq private protected public register reinterpret_cast requires return short signed sizeof static static_assert static_cast struct switch template this thread_local throw true try typedef typeid typename union unsigned using virtual void volatile wchar_t while xor xor_eq";
        if KEYWORDS.split_ascii_whitespace().any(|keyword| keyword == part) { format!("{part}_") } else { part.to_owned() }
    }).collect::<Vec<_>>().join(".");
    let cpp_proto = std::str::from_utf8(bundle.protobuf_source())
        .map_err(|error| SdkGenError::Artifact(error.to_string()))?
        .replace(
            &format!("package {};", proto.package()),
            &format!("package {cpp_package};"),
        );
    files.insert("cpp/model/schema.proto".into(), cpp_proto.into_bytes());
    let cpp_event = format!("::{}::{event}", cpp_package.replace('.', "::"));
    let cpp = format!(
        r#"#pragma once
#include "arrowcol_stream.hpp"
#include "schema.pb.h"
extern "C" {{
char* {prefix}_open(const char*, const char*, void**);
char* {prefix}_push(void*, const std::uint8_t*, std::size_t, std::uint8_t);
char* {prefix}_flush(void*);
char* {prefix}_close(void*);
void {prefix}_destroy(void*);
void {prefix}_free_error(char*);
}}
namespace arrowcol_generated {{
struct {contract} {{
    using Event = {cpp_event};
    static char* Open(const char* endpoint, const char* options, void** handle) {{ return {prefix}_open(endpoint, options, handle); }}
    static char* Push(void* handle, const std::uint8_t* bytes, std::size_t len, std::uint8_t batch) {{ return {prefix}_push(handle, bytes, len, batch); }}
    static char* Flush(void* handle) {{ return {prefix}_flush(handle); }}
    static char* Close(void* handle) {{ return {prefix}_close(handle); }}
    static void Destroy(void* handle) {{ {prefix}_destroy(handle); }}
    static void FreeError(char* error) {{ {prefix}_free_error(error); }}
}};
using {stream} = arrowcol::GeneratedStream<{contract}>;
using {event}Contract = {contract};
using {event}Stream = {stream};
}}
"#
    );
    files.insert("cpp/contract.hpp".into(), cpp.into_bytes());
    let cs_event = format!(
        "global::{}.{event}",
        proto
            .package()
            .split('.')
            .map(pascal_identifier)
            .collect::<Vec<_>>()
            .join(".")
    );
    let cs = format!(
        r#"using System;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Google.Protobuf;
using Arrowcol.Client;
namespace Arrowcol.Generated;
public sealed class {contract} : IArrowcolContract<{cs_event}> {{
    public static {contract} Instance {{ get; }} = new();
    public byte[] Encode({cs_event} value) => value.ToByteArray();
    public NativeApi Native {{ get; }} = new(Open, Push, Flush, Close, Destroy, FreeError);
    [DllImport("{prefix}", EntryPoint="{prefix}_open", CallingConvention=CallingConvention.Cdecl)]
    private static extern IntPtr Open([MarshalAs(UnmanagedType.LPUTF8Str)] string endpoint, [MarshalAs(UnmanagedType.LPUTF8Str)] string options, out IntPtr handle);
    [DllImport("{prefix}", EntryPoint="{prefix}_push", CallingConvention=CallingConvention.Cdecl)]
    private static extern IntPtr Push(IntPtr handle, byte[] bytes, nuint len, byte batch);
    [DllImport("{prefix}", EntryPoint="{prefix}_flush", CallingConvention=CallingConvention.Cdecl)]
    private static extern IntPtr Flush(IntPtr handle);
    [DllImport("{prefix}", EntryPoint="{prefix}_close", CallingConvention=CallingConvention.Cdecl)]
    private static extern IntPtr Close(IntPtr handle);
    [DllImport("{prefix}", EntryPoint="{prefix}_destroy", CallingConvention=CallingConvention.Cdecl)]
    private static extern void Destroy(IntPtr handle);
    [DllImport("{prefix}", EntryPoint="{prefix}_free_error", CallingConvention=CallingConvention.Cdecl)]
    private static extern void FreeError(IntPtr error);
}}
public static class {stream} {{
    public static Task<ArrowcolStream<{cs_event}>> ConnectAsync(string endpoint, StreamOptions? options = null) =>
        ArrowcolStream<{cs_event}>.ConnectAsync(endpoint, {contract}.Instance, options);
}}
public sealed class {event}Contract : IArrowcolContract<{cs_event}> {{
    public static {event}Contract Instance {{ get; }} = new();
    public byte[] Encode({cs_event} value) => {contract}.Instance.Encode(value);
    public NativeApi Native => {contract}.Instance.Native;
}}
public static class {event}Stream {{
    public static Task<ArrowcolStream<{cs_event}>> ConnectAsync(string endpoint, StreamOptions? options = null) =>
        {stream}.ConnectAsync(endpoint, options);
}}
"#
    );
    files.insert("csharp/Contract.cs".into(), cs.into_bytes());
    files.insert("csharp/Contract.csproj".into(), format!(r#"<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <TargetFramework>net8.0</TargetFramework><Nullable>enable</Nullable>
    <AssemblyName>{prefix}</AssemblyName>
    <NativeFile Condition="$([MSBuild]::IsOSPlatform('Windows'))">../native/target/release/{prefix}.dll</NativeFile>
    <NativeFile Condition="$([MSBuild]::IsOSPlatform('Linux'))">../native/target/release/lib{prefix}.so</NativeFile>
    <NativeFile Condition="$([MSBuild]::IsOSPlatform('OSX'))">../native/target/release/lib{prefix}.dylib</NativeFile>
  </PropertyGroup>
  <ItemGroup>
    <PackageReference Include="Google.Protobuf" Version="3.29.3" />
    <PackageReference Include="Grpc.Tools" Version="2.67.0" PrivateAssets="All" />
    <Protobuf Include="../descriptors/schema.proto;../descriptors/options.proto" ProtoRoot="../descriptors" GrpcServices="None" />
    <Compile Include="../runtime/arrowcol-sdk-runtime/bindings/dotnet/ArrowcolStream.cs" Link="ArrowcolStream.cs" />
    <None Include="$(NativeFile)" Link="%(Filename)%(Extension)" CopyToOutputDirectory="PreserveNewest" CopyToPublishDirectory="PreserveNewest" />
  </ItemGroup>
  <Target Name="RequireNativeAdapter" BeforeTargets="PrepareForBuild">
    <Error Condition="!Exists('$(NativeFile)')" Text="Build the matching native adapter first: cargo build --release --manifest-path ../native/Cargo.toml (do not override its target directory)." />
  </Target>
</Project>
"#).into_bytes());
    files.insert("cpp/CMakeLists.txt".into(), format!(r#"cmake_minimum_required(VERSION 3.20)
project(arrowcol_contract LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)
find_package(Protobuf REQUIRED)
set(Protobuf_IMPORT_DIRS "${{CMAKE_CURRENT_SOURCE_DIR}}/../descriptors")
protobuf_generate_cpp(PROTO_SOURCES PROTO_HEADERS model/schema.proto ../descriptors/options.proto)
set(NATIVE_ROOT "${{CMAKE_CURRENT_SOURCE_DIR}}/../native/target/release")
add_library(contract_native SHARED IMPORTED)
if(WIN32)
  set_target_properties(contract_native PROPERTIES IMPORTED_LOCATION "${{NATIVE_ROOT}}/{prefix}.dll" IMPORTED_IMPLIB "${{NATIVE_ROOT}}/{prefix}.dll.lib")
elseif(APPLE)
  set_target_properties(contract_native PROPERTIES IMPORTED_LOCATION "${{NATIVE_ROOT}}/lib{prefix}.dylib")
else()
  set_target_properties(contract_native PROPERTIES IMPORTED_LOCATION "${{NATIVE_ROOT}}/lib{prefix}.so")
endif()
add_library(arrowcol_contract ${{PROTO_SOURCES}})
target_include_directories(arrowcol_contract PUBLIC "${{CMAKE_CURRENT_BINARY_DIR}}" "${{CMAKE_CURRENT_SOURCE_DIR}}" "${{CMAKE_CURRENT_SOURCE_DIR}}/../runtime/arrowcol-sdk-runtime/bindings/cpp")
target_link_libraries(arrowcol_contract PUBLIC protobuf::libprotobuf contract_native)
"#).into_bytes());
    files.insert("STREAMS.md".into(), br#"# Descriptor-driven typed streams

| Package | Contents | Dependency |
| --- | --- | --- |
| Neutral contract (`descriptors/`) | Canonical .proto, custom options/imports, descriptors, Arrow schema, manifest | No Arrowcol SDK |
| Models (`models/rust`, `models/cpp`, `models/csharp`) | Ordinary protobuf event types and an identical embedded contract | Protobuf only; no Arrowcol SDK |
| Rust SDK (`runtime/arrowcol-sdk-runtime`) | Generic typed streams, loaded contract validation/conversion, Flight transport | Schema-independent |
| C++ SDK (`runtime/cpp`) / C# SDK (`runtime/csharp`) | Typed bindings to `runtime/arrowcol-sdk-ffi` | One native library for all schemas |

Reference your language SDK and models package separately. Load the models' `contract/`
directory and connect `ArrowcolStream<Event>` with the configured collector endpoint.
No event-specific Arrowcol helper class or native library is needed. Native SDK sources
must be built for your OS/architecture:

```sh
cargo build --release --manifest-path runtime/arrowcol-sdk-ffi/Cargo.toml
```

The Nx workflow also publishes each source package and the neutral contract as an
independently downloadable immutable archive. Package feeds/prebuilt native binaries
are not configured. C# uses `Models.csproj` and `Arrowcol.Sdk.csproj` project references;
C++ links the target named in `models/cpp/package.json` and `arrowcol_sdk`; Rust references the models crate and
`arrowcol-sdk-runtime`. SDK source is identical across schemas. C# and C++ compile models
with their normal protobuf compilers; Rust models include protobuf reflection metadata.

The typed runtime supports flat scalars, validates the compiled message against the
loaded contract before connection, and validates values before enqueueing. Required
presence, integer ranges, decimal precision/scale and temporal units are preserved.
Decimal strings express numeric values, not unscaled integers. Optional DDL properties
remain in .proto/descriptors as metadata; the SDK does not enforce backend policies.

Configure the collector with a stable DNS name where appropriate. Samples accept
`--endpoint` / `ARROWCOL_ENDPOINT` and `--contract` / `ARROWCOL_CONTRACT`; the SDK accepts
explicit values and does not read environment variables or discover services.

Push means bounded enqueue, not per-event network acknowledgment. Flush/Close confirm
collector admission, not Delta commit, node-to-regional delivery or Kusto visibility.
There is no automatic replay or exactly-once promise. Reconcile uncertain outcomes
before retrying. Disposal alone does not flush. One logical writer per stream.

Defaults: 1,024 rows, 1 MiB batches, 32 MiB retained Arrow buffers, one-second flush
interval and 30-second deadlines. This is not a total RSS limit. HTTPS uses native
trusted roots; bearer-token and mTLS configuration are not provided.

## Compatibility

The old generated `rust/`, `typescript/`, `cpp/`, `csharp/` and `native/` trees remain
for existing consumers. Their event-specific helpers/native adapters are legacy,
not dependencies of the new models or SDK packages. Existing immutable releases
are never rewritten. New demo applications use only the independent packages.
"#.to_vec());
    Ok(files)
}
