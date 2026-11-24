use std::{collections::BTreeMap, path::PathBuf};

pub(super) fn render() -> BTreeMap<PathBuf, Vec<u8>> {
    let mut files = BTreeMap::new();
    for (name, source) in [
        (
            "arrowcol-sdk-runtime/Cargo.toml",
            include_str!("../../../arrowcol-sdk-runtime/Cargo.toml"),
        ),
        (
            "arrowcol-sdk-runtime/src/lib.rs",
            include_str!("../../../arrowcol-sdk-runtime/src/lib.rs"),
        ),
        (
            "arrowcol-sdk-runtime/src/contract.rs",
            include_str!("../../../arrowcol-sdk-runtime/src/contract.rs"),
        ),
        (
            "arrowcol-sdk-runtime/src/fingerprint.rs",
            include_str!("../../../arrowcol-sdk-runtime/src/fingerprint.rs"),
        ),
        (
            "arrowcol-sdk-runtime/bindings/cpp/arrowcol_stream.hpp",
            include_str!("../../../arrowcol-sdk-runtime/bindings/cpp/arrowcol_stream.hpp"),
        ),
        (
            "arrowcol-sdk-runtime/bindings/cpp/arrowcol_client.hpp",
            include_str!("../../../arrowcol-sdk-runtime/bindings/cpp/arrowcol_client.hpp"),
        ),
        (
            "arrowcol-sdk-runtime/bindings/dotnet/ArrowcolStream.cs",
            include_str!("../../../arrowcol-sdk-runtime/bindings/dotnet/ArrowcolStream.cs"),
        ),
        (
            "arrowcol-sdk-ffi/Cargo.toml",
            include_str!("../../../arrowcol-sdk-ffi/Cargo.toml"),
        ),
        (
            "arrowcol-sdk-ffi/src/lib.rs",
            include_str!("../../../arrowcol-sdk-ffi/src/lib.rs"),
        ),
    ] {
        let source = if name.ends_with("Cargo.toml") {
            format!("{source}\n[workspace]\n")
        } else {
            source.to_owned()
        };
        files.insert(PathBuf::from("runtime").join(name), source.into_bytes());
    }
    files.insert("runtime/csharp/Arrowcol.Sdk.csproj".into(), br#"<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <TargetFramework>net8.0</TargetFramework><Nullable>enable</Nullable>
    <PackageId>Arrowcol.Sdk</PackageId><Version>0.1.0</Version>
    <NativeFile Condition="$([MSBuild]::IsOSPlatform('Windows'))">../arrowcol-sdk-ffi/target/release/arrowcol_sdk_ffi.dll</NativeFile>
    <NativeFile Condition="$([MSBuild]::IsOSPlatform('Linux'))">../arrowcol-sdk-ffi/target/release/libarrowcol_sdk_ffi.so</NativeFile>
    <NativeFile Condition="$([MSBuild]::IsOSPlatform('OSX'))">../arrowcol-sdk-ffi/target/release/libarrowcol_sdk_ffi.dylib</NativeFile>
  </PropertyGroup>
  <ItemGroup>
    <PackageReference Include="Google.Protobuf" Version="3.29.3" />
    <Compile Include="../arrowcol-sdk-runtime/bindings/dotnet/*.cs" />
    <None Include="$(NativeFile)" Link="%(Filename)%(Extension)" CopyToOutputDirectory="PreserveNewest" CopyToPublishDirectory="PreserveNewest" />
  </ItemGroup>
  <Target Name="RequireNativeRuntime" BeforeTargets="PrepareForBuild">
    <Error Condition="!Exists('$(NativeFile)')" Text="Build the shared native runtime: cargo build --release --manifest-path ../arrowcol-sdk-ffi/Cargo.toml" />
  </Target>
</Project>
"#.to_vec());
    files.insert("runtime/cpp/CMakeLists.txt".into(), br#"cmake_minimum_required(VERSION 3.20)
project(arrowcol_sdk LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)
find_package(Protobuf REQUIRED)
set(NATIVE_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../arrowcol-sdk-ffi/target/release")
add_library(arrowcol_native SHARED IMPORTED GLOBAL)
if(WIN32)
  set_target_properties(arrowcol_native PROPERTIES IMPORTED_LOCATION "${NATIVE_ROOT}/arrowcol_sdk_ffi.dll" IMPORTED_IMPLIB "${NATIVE_ROOT}/arrowcol_sdk_ffi.dll.lib")
elseif(APPLE)
  set_target_properties(arrowcol_native PROPERTIES IMPORTED_LOCATION "${NATIVE_ROOT}/libarrowcol_sdk_ffi.dylib")
else()
  set_target_properties(arrowcol_native PROPERTIES IMPORTED_LOCATION "${NATIVE_ROOT}/libarrowcol_sdk_ffi.so")
endif()
add_library(arrowcol_sdk INTERFACE)
target_include_directories(arrowcol_sdk INTERFACE "${CMAKE_CURRENT_SOURCE_DIR}/../arrowcol-sdk-runtime/bindings/cpp")
target_link_libraries(arrowcol_sdk INTERFACE protobuf::libprotobuf arrowcol_native)
"#.to_vec());
    files
}
