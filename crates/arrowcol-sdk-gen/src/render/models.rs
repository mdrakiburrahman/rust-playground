use std::{collections::BTreeMap, path::PathBuf};

use super::{RenderContext, pascal_identifier, rust};
use crate::{ArtifactBundle, SdkGenError, SdkGenResult};

pub(super) fn render(
    context: &RenderContext<'_>,
    bundle: &ArtifactBundle,
    cpp_source: &[u8],
) -> SdkGenResult<BTreeMap<PathBuf, Vec<u8>>> {
    let mut files = BTreeMap::new();
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
    let message = format!("{}.{event}", proto.package());
    let identity = context.schema.table_id().to_string().replace('-', "");
    let name = format!(
        "{}-{identity}",
        context.rust_package.replacen("arrowcol-sdk-", "contracts-", 1)
    );
    let cpp_file = format!("contract_{identity}");
    let cs_message = format!(
        "{}.{event}",
        proto
            .package()
            .split('.')
            .map(pascal_identifier)
            .collect::<Vec<_>>()
            .join(".")
    );
    let cpp_package = std::str::from_utf8(cpp_source)
        .map_err(|error| SdkGenError::Artifact(error.to_string()))?
        .lines()
        .find_map(|line| line.strip_prefix("package ").and_then(|line| line.strip_suffix(';')))
        .ok_or_else(|| SdkGenError::Artifact("missing C++ protobuf package".into()))?;
    let cpp_message = format!("::{}::{event}", cpp_package.replace('.', "::"));
    let mut config = prost_build::Config::new();
    config.type_attribute(
        &message,
        format!(
            "#[derive(::prost_reflect::ReflectMessage)]\n\
             #[prost_reflect(descriptor_pool = \"crate::descriptor_pool()\", message_name = \"{message}\")]"
        ),
    );
    let generated = config
        .generate(vec![(
            prost_build::Module::from_protobuf_package_name(proto.package()),
            proto.clone(),
        )])
        .map_err(|error| SdkGenError::Artifact(format!("protobuf models generation: {error}")))?;
    if generated.len() != 1 {
        return Err(SdkGenError::Artifact("expected one models package".into()));
    }
    let source = generated
        .into_values()
        .next()
        .ok_or_else(|| SdkGenError::Artifact("missing model".into()))?;
    files.insert(
        "models/rust/src/lib.rs".into(),
        rust::format_rust(&format!(
            "{source}\n\
             fn descriptor_pool() -> prost_reflect::DescriptorPool {{\n\
             static POOL: std::sync::LazyLock<prost_reflect::DescriptorPool> = std::sync::LazyLock::new(||\n\
             prost_reflect::DescriptorPool::decode(include_bytes!(\"../contract/descriptor.pb\").as_ref())\n\
             .unwrap_or_else(|error| panic!(\"Invalid embedded contract descriptor: {{error}}\")));\n\
             POOL.clone()\n\
             }}\n"
        ))?
        .into_bytes(),
    );
    files.insert(
        "models/rust/Cargo.toml".into(),
        format!(
            "[package]\nname = \"{name}\"\nversion = \"{}.0.0\"\nedition = \"2024\"\n\
             license = \"MIT\"\n[workspace]\n[dependencies]\nprost = \"0.14\"\n\
             prost-reflect = {{ version = \"0.16\", features = [\"derive\"] }}\n",
            context.schema.version()
        )
        .into_bytes(),
    );
    files.insert(
        "models/csharp/Models.csproj".into(),
        format!(
            r#"<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <TargetFramework>net8.0</TargetFramework><Nullable>enable</Nullable>
    <PackageId>{name}</PackageId><AssemblyName>{name}</AssemblyName><Version>{version}.0.0</Version>
  </PropertyGroup>
  <ItemGroup>
    <PackageReference Include="Google.Protobuf" Version="3.29.3" />
    <PackageReference Include="Grpc.Tools" Version="2.67.0" PrivateAssets="All" />
    <Protobuf Include="contract/schema.proto;contract/options.proto" ProtoRoot="contract" GrpcServices="None" />
    <None Update="contract/**/*" Pack="true" PackagePath="contentFiles/any/any/contracts/{table}/v{version}/%(RecursiveDir)%(Filename)%(Extension)" />
  </ItemGroup>
</Project>
"#,
            version = context.schema.version(),
            table = context.schema.table_id(),
        )
        .into_bytes(),
    );
    files.insert(
        PathBuf::from(format!("models/cpp/model/{cpp_file}.proto")),
        cpp_source.to_vec(),
    );
    files.insert(
        "models/cpp/CMakeLists.txt".into(),
        format!(
            r#"cmake_minimum_required(VERSION 3.20)
project({name} LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)
find_package(Protobuf REQUIRED)
set(Protobuf_IMPORT_DIRS "${{CMAKE_CURRENT_SOURCE_DIR}}/contract")
protobuf_generate_cpp(PROTO_SOURCES PROTO_HEADERS model/{cpp_file}.proto contract/options.proto)
add_library({name} ${{PROTO_SOURCES}})
target_include_directories({name} PUBLIC "${{CMAKE_CURRENT_BINARY_DIR}}")
target_link_libraries({name} PUBLIC protobuf::libprotobuf)
"#
        )
        .into_bytes(),
    );
    let mut contract = BTreeMap::from([
        ("manifest.json".to_owned(), bundle.manifest().to_vec()),
        ("schema.arrow".to_owned(), bundle.arrow_schema().to_vec()),
        ("descriptor.pb".to_owned(), bundle.protobuf_descriptor().to_vec()),
        ("schema.proto".to_owned(), bundle.protobuf_source().to_vec()),
    ]);
    contract.extend(
        bundle
            .protobuf_imports()
            .iter()
            .map(|(path, bytes)| (path.clone(), bytes.clone())),
    );
    for language in ["rust", "csharp", "cpp"] {
        for (path, bytes) in &contract {
            files.insert(
                PathBuf::from(format!("models/{language}/contract/{path}")),
                bytes.clone(),
            );
        }
        files.insert(
            PathBuf::from(format!("models/{language}/README.md")),
            format!("# {name}\n\nStandard protobuf models for `{message}`, contract version {}.\n\n\
                This package contains its exact language-neutral contract under `contract/` and does not depend on the Arrowcol SDK. \
                Optional DDL properties are recorded in the protobuf options; this package does not enforce policies.\n",
                context.schema.version()).into_bytes(),
        );
    }
    let manifest = serde_json::json!({
        "formatVersion": 1,
        "tableId": context.schema.table_id().to_string(),
        "version": context.schema.version().get(),
        "fingerprint": context.schema.fingerprint().to_string(),
        "protobufMessage": message,
        "languages": {
            "rust": { "name": name, "root": "models/rust", "message": event },
            "csharp": { "name": name, "root": "models/csharp", "message": cs_message },
            "cpp": { "name": name, "root": "models/cpp", "message": cpp_message, "header": format!("{cpp_file}.pb.h") }
        }
    });
    for language in ["rust", "cpp", "csharp"] {
        files.insert(
            PathBuf::from(format!("models/{language}/package.json")),
            serde_json::to_vec_pretty(&manifest["languages"][language])?,
        );
    }
    files.insert("models/manifest.json".into(), serde_json::to_vec_pretty(&manifest)?);
    Ok(files)
}
