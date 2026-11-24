//! Opt-in, descriptor-only frontend for the existing Arrowcol SDK renderer.

use std::{collections::BTreeMap, fmt::Write, path::Path};

use arrowcol_catalog::{CatalogSchemaVersion, protobuf_option_definitions};
use arrowcol_sdk_gen::{ArtifactBundle, render_sdk_files};
use prost::Message;
use prost_types::FileDescriptorProto;
use serde::Deserialize;

/// Compiler contract messages generated from the public protobuf definition.
pub mod abi {
    include!(concat!(env!("OUT_DIR"), "/arrowcol.codegen.v1.rs"));
}

/// Result of compiler contract validation or rendering.
pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

/// Standard protoc request, retaining descriptor bytes (including custom options).
#[derive(Clone, PartialEq, Message)]
pub struct CodeGeneratorRequest {
    /// Root files selected by protoc, not every imported definition.
    #[prost(string, repeated, tag = "1")]
    pub file_to_generate: Vec<String>,
    /// Generator-specific options. This first adapter accepts no extra parameters.
    #[prost(string, optional, tag = "2")]
    pub parameter: Option<String>,
    /// Raw descriptors prevent prost-types from discarding custom extensions.
    #[prost(bytes = "vec", repeated, tag = "15")]
    pub proto_file: Vec<Vec<u8>>,
}

/// Standard protoc response.
#[derive(Clone, PartialEq, Message)]
pub struct CodeGeneratorResponse {
    /// Schema or generation error, reported without emitting partial output.
    #[prost(string, optional, tag = "1")]
    pub error: Option<String>,
    /// Generated text files.
    #[prost(message, repeated, tag = "15")]
    pub file: Vec<GeneratedFile>,
}

/// Standard CodeGeneratorResponse.File.
#[derive(Clone, PartialEq, Message)]
pub struct GeneratedFile {
    /// Portable slash-separated path relative to protoc's output directory.
    #[prost(string, optional, tag = "1")]
    pub name: Option<String>,
    /// UTF-8 generated source. Binary artifacts are packaged separately by Nx.
    #[prost(string, optional, tag = "15")]
    pub content: Option<String>,
}

#[derive(Clone, PartialEq, Message)]
struct RawSet {
    #[prost(bytes = "vec", repeated, tag = "1")]
    file: Vec<Vec<u8>>,
}

#[derive(Clone, PartialEq, Message)]
struct FileOptions {
    #[prost(message, optional, tag = "51000")]
    compiler: Option<abi::CompilerContract>,
}

#[derive(Clone, PartialEq, Message)]
struct FileCarrier {
    #[prost(message, optional, tag = "8")]
    options: Option<FileOptions>,
}

#[derive(Deserialize)]
struct Manifest {
    artifact_manifest_version: u32,
    schema: CatalogSchemaVersion,
}

const CODEGEN_SOURCE: &str = include_str!("../proto/arrowcol_codegen.proto");
const CODEGEN_DESCRIPTORS: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/codegen.pb"));
const CODEGEN_IMPORT: &str = "arrowcol_codegen.proto";

/// Checks a compiler entry file name without permitting traversal or import shadowing.
///
/// # Errors
///
/// Rejects paths, unsupported characters and names owned by imported definitions.
pub fn validate_entry(entry: &str) -> Result<()> {
    if !entry.ends_with(".proto")
        || !entry
            .bytes()
            .next()
            .is_some_and(|c| c.is_ascii_alphanumeric() || c == b'_')
        || !entry.bytes().all(|c| c.is_ascii_alphanumeric() || b"_.-".contains(&c))
        || matches!(entry, CODEGEN_IMPORT | "options.proto" | "descriptor.proto")
    {
        return Err("entry must be a non-reserved .proto basename".into());
    }
    Ok(())
}

fn api_binding(bundle: &ArtifactBundle) -> abi::ApiBinding {
    let schema = bundle.schema();
    abi::ApiBinding {
        operation: abi::Operation::ArrowFlightDoPut as i32,
        grpc_path: "/arrow.flight.protocol.FlightService/DoPut".to_owned(),
        descriptor_path: vec![
            schema.identity().catalog().to_string(),
            schema.identity().schema().to_string(),
            schema.identity().table().to_string(),
            format!("v{}", schema.version()),
        ],
        metadata: [
            ("x-arrowcol-protocol-version", "1".to_owned()),
            ("x-arrowcol-implementation-pool", "dataflow".to_owned()),
            ("x-arrowcol-catalog", schema.identity().catalog().to_string()),
            ("x-arrowcol-schema", schema.identity().schema().to_string()),
            ("x-arrowcol-table", schema.identity().table().to_string()),
            ("x-arrowcol-schema-fingerprint", schema.fingerprint().to_string()),
        ]
        .into_iter()
        .map(|(key, value)| abi::MetadataBinding {
            key: key.to_owned(),
            value,
        })
        .collect(),
    }
}

fn quoted(bytes: &[u8]) -> String {
    let mut output = String::from("\"");
    for byte in bytes {
        match byte {
            b'"' => output.push_str("\\\""),
            b'\\' => output.push_str("\\\\"),
            0x20..=0x7e => output.push(char::from(*byte)),
            _ => output.push_str(&format!("\\{byte:03o}")),
        }
    }
    output.push('"');
    output
}

/// Creates self-contained compiler sources without changing canonical catalogue artifacts.
///
/// # Errors
///
/// Rejects an unsafe entry name, invalid definitions or non-UTF-8 canonical source.
pub fn compiler_sources(bundle: &ArtifactBundle, entry: &str) -> Result<BTreeMap<String, Vec<u8>>> {
    validate_entry(entry)?;
    let api = api_binding(bundle);
    let mut source = String::from_utf8(bundle.protobuf_source().to_vec())?;
    source.push_str(&format!(
        "\nimport \"{CODEGEN_IMPORT}\";\noption (arrowcol.codegen.v1.compiler) = {{\n\
         format_version: 1\ncatalog_manifest_json: {}\noption_definitions_sha256: {}\n\
         api: {{ operation: ARROW_FLIGHT_DO_PUT grpc_path: {}\n",
        quoted(bundle.manifest()),
        quoted(protobuf_option_definitions()?.definitions_sha256.as_bytes()),
        quoted(api.grpc_path.as_bytes()),
    ));
    for part in api.descriptor_path {
        writeln!(&mut source, "descriptor_path: {}", quoted(part.as_bytes()))?;
    }
    for item in api.metadata {
        writeln!(
            &mut source,
            "metadata: {{ key: {} value: {} }}",
            quoted(item.key.as_bytes()),
            quoted(item.value.as_bytes())
        )?;
    }
    source.push_str("}\n};\n");
    let mut files = bundle.protobuf_imports().clone();
    files.insert(entry.to_owned(), source.into_bytes());
    files.insert(CODEGEN_IMPORT.to_owned(), CODEGEN_SOURCE.as_bytes().to_vec());
    Ok(files)
}

// Inspect complete wire fields without decoding/re-encoding extension-bearing messages.
type WireField<'a> = (u32, &'a [u8], &'a [u8]);

fn fields(mut input: &[u8]) -> Result<Vec<WireField<'_>>> {
    let mut output = Vec::new();
    while !input.is_empty() {
        let start = input;
        let (tag, wire) = prost::encoding::decode_key(&mut input)?;
        let payload = if wire == prost::encoding::WireType::LengthDelimited {
            let length = usize::try_from(prost::encoding::decode_varint(&mut input)?)?;
            if length > input.len() {
                return Err("truncated descriptor field".into());
            }
            &input[..length]
        } else {
            &[]
        };
        // skip_field expects the length prefix for a length-delimited value.
        let mut tail = &start[start.len() - input.len()..];
        if wire == prost::encoding::WireType::LengthDelimited {
            tail = &input[payload.len()..];
        } else {
            prost::encoding::skip_field(wire, tag, &mut tail, prost::encoding::DecodeContext::default())?;
        }
        let consumed = start.len() - tail.len();
        output.push((tag, &start[..consumed], payload));
        input = tail;
    }
    Ok(output)
}

fn normalize_file(bytes: &[u8], root: bool) -> Result<Vec<u8>> {
    let mut output = Vec::new();
    for (tag, raw, payload) in fields(bytes)? {
        match tag {
            9 => {} // Source locations are not schema semantics.
            1 if root => prost::encoding::string::encode(1, &"schema.proto".to_owned(), &mut output),
            3 if root && payload == CODEGEN_IMPORT.as_bytes() => {}
            8 if root => {
                let options = fields(payload)?
                    .into_iter()
                    .filter(|(number, _, _)| *number != 51000)
                    .flat_map(|(_, bytes, _)| bytes.iter().copied())
                    .collect::<Vec<_>>();
                if !options.is_empty() {
                    prost::encoding::bytes::encode(8, &options, &mut output);
                }
            }
            _ => output.extend_from_slice(raw),
        }
    }
    // protoc may merge two FileOptions occurrences. Compare a canonical layout
    // while preserving the original custom option/message payloads.
    let mut entries = fields(&output)?;
    entries.sort_by_key(|(tag, _, _)| *tag);
    Ok(entries
        .into_iter()
        .flat_map(|(_, raw, _)| raw.iter().copied())
        .collect())
}

/// Validates a self-contained request and recovers the exact existing catalogue IR.
///
/// # Errors
///
/// Rejects changed row/options/dependency descriptors, unsupported API bindings,
/// ambiguous inputs, unknown parameters or incompatible compiler contracts.
pub fn request_bundle(request: &CodeGeneratorRequest) -> Result<ArtifactBundle> {
    let [entry] = request.file_to_generate.as_slice() else {
        return Err("exactly one table file must be selected for generation".into());
    };
    validate_entry(entry)?;
    if request.parameter.as_deref().is_some_and(|value| !value.is_empty()) {
        return Err("unsupported Arrowcol plugin parameter".into());
    }
    let mut actual = BTreeMap::new();
    for bytes in &request.proto_file {
        let descriptor = FileDescriptorProto::decode(bytes.as_slice())?;
        if actual.insert(descriptor.name().to_owned(), bytes.as_slice()).is_some() {
            return Err("duplicate descriptor name".into());
        }
    }
    let root = *actual.get(entry).ok_or("missing selected descriptor")?;
    let contract = FileCarrier::decode(root)?
        .options
        .and_then(|value| value.compiler)
        .ok_or("selected file lacks the Arrowcol compiler contract")?;
    if contract.format_version != 1 {
        return Err("unsupported compiler contract version".into());
    }
    if contract.option_definitions_sha256 != protobuf_option_definitions()?.definitions_sha256 {
        return Err("option definitions differ from this plugin version".into());
    }
    let manifest: Manifest = serde_json::from_slice(&contract.catalog_manifest_json)?;
    if manifest.artifact_manifest_version != 1 {
        return Err("unsupported catalogue IR version".into());
    }
    let bundle = ArtifactBundle::from_schema(manifest.schema)?;
    if bundle.manifest() != contract.catalog_manifest_json {
        return Err("compiler IR is not canonical".into());
    }
    if contract.api != api_binding(&bundle) {
        return Err("unsupported API binding: expected canonical Arrow Flight DoPut and routing metadata".into());
    }
    let mut expected = RawSet::decode(bundle.protobuf_descriptor())?.file;
    expected.extend(
        RawSet::decode(CODEGEN_DESCRIPTORS)?.file.into_iter().filter(|bytes| {
            FileDescriptorProto::decode(bytes.as_slice()).is_ok_and(|file| file.name() == CODEGEN_IMPORT)
        }),
    );
    if actual.len() != expected.len() {
        return Err("unexpected compiler dependency set".into());
    }
    for bytes in expected {
        let file = FileDescriptorProto::decode(bytes.as_slice())?;
        let is_root = file.name() == "schema.proto";
        let name = if is_root { entry.as_str() } else { file.name() };
        let provided = actual.get(name).ok_or("missing compiler dependency")?;
        if normalize_file(provided, is_root)? != normalize_file(&bytes, false)? {
            return Err(format!("compiler descriptor differs from the frozen IR: {name}").into());
        }
    }
    Ok(bundle)
}

/// Implements protoc generation through the existing renderer, with no side effects.
///
/// # Errors
///
/// Returns contract validation and renderer errors, or non-UTF-8 text output.
pub fn generate(request: &CodeGeneratorRequest) -> Result<CodeGeneratorResponse> {
    let bundle = request_bundle(request)?;
    let mut response = CodeGeneratorResponse::default();
    for (path, bytes) in render_sdk_files(&bundle)? {
        let name = path.to_string_lossy().replace('\\', "/");
        // CodeGeneratorResponse.File.content is UTF-8; Nx copies these originals.
        if name.ends_with("/descriptor.pb") || name.ends_with("/schema.arrow") {
            continue;
        }
        response.file.push(GeneratedFile {
            name: Some(name),
            content: Some(String::from_utf8(bytes)?),
        });
    }
    Ok(response)
}

/// Writes compiler sources into a fresh, safe staging directory.
///
/// # Errors
///
/// Rejects existing destinations and filesystem errors.
pub fn export(bundle: &ArtifactBundle, output: &Path, entry: &str) -> Result<()> {
    let files = compiler_sources(bundle, entry)?;
    std::fs::create_dir(output)?;
    for (name, bytes) in files {
        let path = output.join(name);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(path, bytes)?;
    }
    Ok(())
}
