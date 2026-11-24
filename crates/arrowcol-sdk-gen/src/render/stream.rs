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
    let mut files = BTreeMap::new();
    for (name, source) in [
        (
            "runtime/arrowcol-sdk-runtime/Cargo.toml",
            include_str!("../../../arrowcol-sdk-runtime/Cargo.toml"),
        ),
        (
            "runtime/arrowcol-sdk-runtime/src/lib.rs",
            include_str!("../../../arrowcol-sdk-runtime/src/lib.rs"),
        ),
        (
            "runtime/arrowcol-sdk-runtime/bindings/cpp/arrowcol_stream.hpp",
            include_str!("../../../arrowcol-sdk-runtime/bindings/cpp/arrowcol_stream.hpp"),
        ),
        (
            "runtime/arrowcol-sdk-runtime/bindings/dotnet/ArrowcolStream.cs",
            include_str!("../../../arrowcol-sdk-runtime/bindings/dotnet/ArrowcolStream.cs"),
        ),
        (
            "runtime/arrowcol-sdk-ffi/Cargo.toml",
            include_str!("../../../arrowcol-sdk-ffi/Cargo.toml"),
        ),
        (
            "runtime/arrowcol-sdk-ffi/src/lib.rs",
            include_str!("../../../arrowcol-sdk-ffi/src/lib.rs"),
        ),
    ] {
        files.insert(PathBuf::from(name), source.as_bytes().to_vec());
    }
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
    let contract = format!("{event}Contract");
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
pub use arrowcol_sdk_runtime::{{ArrowcolStream, StreamOptions}};
pub struct {contract};
pub type {event}Stream = ArrowcolStream<{contract}>;
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
using {event}Stream = arrowcol::ArrowcolStream<{contract}>;
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
public static class {event}Stream {{
    public static Task<ArrowcolStream<{cs_event}>> ConnectAsync(string endpoint, StreamOptions? options = null) =>
        ArrowcolStream<{cs_event}>.ConnectAsync(endpoint, {contract}.Instance, options);
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
    files.insert("STREAMS.md".into(), format!("# Typed streams\n\n| Piece | Source |\n| --- | --- |\n| Event `{event}` | Standard protobuf compiler (prost, protoc C++ or Grpc.Tools C#) |\n| `{contract}` | Arrowcol plugin: validates presence/types and maps to the committed Arrow schema |\n| Stream engine | Shared `arrowcol-sdk-runtime`, copied verbatim under `runtime/` for an offline source bundle |\n| C++ / C# bridge | Table-specific `native/` adapter to the same Rust engine, not another transport |\n\nUse Rust `{event}Stream::connect(endpoint, Default::default()).await?`, C# `Arrowcol.Generated.{event}Stream.ConnectAsync(endpoint)`, or C++ `arrowcol_generated::{event}Stream::Connect(endpoint)`. Push normal generated protobuf events, then explicitly flush and close. A collector endpoint is the only destination configuration; there is no registry argument.\n\nBuild `native/Cargo.toml` with `cargo build --release --manifest-path native/Cargo.toml` before C++/C# builds. Do not override its target directory. C# `dotnet build csharp/Contract.csproj` and CMake `cpp/` invoke standard language protobuf generation; do not edit their generated message classes. Use matching native binaries for your OS/architecture. Native adapters are source-distributed, not prebuilt binaries.\n\nThe initial stream surface supports flat scalar schemas. Required fields preserve protobuf presence, integers are range checked, and decimal strings express numeric values (for example `12.34`, not an unscaled integer). Existing direct Arrow builders also support their previous nested types.\n\nPush queues a copied snapshot, with bounded backpressure; it does not wait for a per-record acknowledgement. Flush waits for validated collector admission. Neither flush nor on-node admission proves regional delivery or Delta/Kusto publication. There are no automatic network retries or exactly-once promises. Retain the application's source events for recovery; uncertain failures must be reconciled, not blindly replayed. Explicit Close surfaces errors; destruction/disposal alone does not flush. One logical writer per stream; C# serializes calls and C++ callers must serialize access.\n\nDefaults: 1,024 rows, 1 MiB Arrow batches, 32 MiB retained Arrow buffers, 1-second flush interval and 30-second per-RPC/capacity timeouts. The buffer budget is not total RSS. Oversized single calls fail before enqueueing and must be split. TLS uses native roots for HTTPS; HTTP is for trusted local development. This SDK does not provision authentication or enable public collector exposure.\n").into_bytes());
    Ok(files)
}
