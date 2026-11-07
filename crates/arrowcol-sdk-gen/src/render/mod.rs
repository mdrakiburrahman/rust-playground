mod rust;
mod typescript;

use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
};

use arrowcol_catalog::{CatalogField, CatalogSchemaVersion, CatalogType, ListKind};
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::{ArtifactBundle, SdkGenError, SdkGenResult};

pub(crate) struct RenderedSdk {
    pub(crate) files: BTreeMap<PathBuf, Vec<u8>>,
    pub(crate) rust_package: String,
    pub(crate) typescript_package: String,
}

#[derive(Clone)]
pub(crate) struct RenderContext<'a> {
    pub(crate) schema: &'a CatalogSchemaVersion,
    pub(crate) row_name: String,
    pub(crate) builder_name: String,
    pub(crate) named: Vec<NamedDefinition>,
    pub(crate) names_by_field_id: BTreeMap<String, String>,
    pub(crate) rust_package: String,
    pub(crate) typescript_package: String,
}

#[derive(Clone)]
pub(crate) struct NamedDefinition {
    pub(crate) name: String,
    pub(crate) kind: NamedKind,
}

#[derive(Clone)]
pub(crate) enum NamedKind {
    Struct(Vec<CatalogField>),
    Map {
        key: Box<CatalogField>,
        value: Box<CatalogField>,
    },
}

pub(crate) fn render_sdk(bundle: &ArtifactBundle) -> SdkGenResult<RenderedSdk> {
    let context = RenderContext::new(bundle.schema())?;
    let mut files = BTreeMap::new();
    files.extend(rust::render(&context)?);
    files.extend(typescript::render(&context)?);
    let requests = bundle
        .descriptors()
        .file
        .iter()
        .map(|file| {
            (
                prost_build::Module::from_protobuf_package_name(file.package()),
                file.clone(),
            )
        })
        .collect();
    let generated = prost_build::Config::new()
        .generate(requests)
        .map_err(|error| SdkGenError::Artifact(format!("prost type generation failed: {error}")))?;
    if generated.len() != 1 {
        return Err(SdkGenError::Artifact(
            "expected exactly one protobuf package per table".to_owned(),
        ));
    }
    for source in generated.into_values() {
        files.insert(
            PathBuf::from("rust/src/proto.rs"),
            rust::format_rust(&source)?.into_bytes(),
        );
    }
    files.insert(PathBuf::from("descriptors/manifest.json"), bundle.manifest().to_vec());
    files.insert(
        PathBuf::from("descriptors/schema.arrow"),
        bundle.arrow_schema().to_vec(),
    );
    files.insert(
        PathBuf::from("descriptors/descriptor.pb"),
        bundle.protobuf_descriptor().to_vec(),
    );
    files.insert(
        PathBuf::from("descriptors/schema.proto"),
        bundle.protobuf_source().to_vec(),
    );
    files.insert(PathBuf::from("README.md"), generated_root_readme(&context).into_bytes());

    let manifest = sdk_manifest(&context, bundle, &files);
    let mut manifest_bytes = serde_json::to_vec_pretty(&manifest)?;
    manifest_bytes.push(b'\n');
    files.insert(PathBuf::from("sdk-manifest.json"), manifest_bytes);
    Ok(RenderedSdk {
        files,
        rust_package: context.rust_package,
        typescript_package: context.typescript_package,
    })
}

impl<'a> RenderContext<'a> {
    fn new(schema: &'a CatalogSchemaVersion) -> SdkGenResult<Self> {
        for field in schema.fields() {
            validate_supported_field(field, field.name())?;
        }
        validate_field_identifiers("top-level row", schema.fields())?;

        let row_name = bounded_identifier(
            &format!("{}Row", pascal_identifier(schema.identity().table().as_str())),
            schema.identity().table().as_str(),
        );
        let builder_name = format!("{row_name}Builder");
        let mut names_by_field_id = BTreeMap::new();
        let mut named = Vec::new();
        let mut type_names = BTreeSet::from([row_name.clone(), builder_name.clone()]);
        for field in schema.fields() {
            collect_named(
                field,
                &row_name,
                field.name(),
                &mut names_by_field_id,
                &mut named,
                &mut type_names,
            )?;
        }

        let digest = schema.fingerprint().to_string();
        let digest = digest.strip_prefix("sha256:").unwrap_or(digest.as_str());
        let digest = &digest[..12];
        let slug = bounded_slug(schema.identity().table().as_str(), 64);
        let rust_package = format!("arrowcol-sdk-{slug}-{digest}");
        let typescript_package = format!("@arrowcol-sdk/{slug}-{digest}");
        Ok(Self {
            schema,
            row_name,
            builder_name,
            named,
            names_by_field_id,
            rust_package,
            typescript_package,
        })
    }

    pub(crate) fn named_type(&self, field: &CatalogField) -> SdkGenResult<&str> {
        self.names_by_field_id
            .get(&field.id().to_string())
            .map(String::as_str)
            .ok_or_else(|| SdkGenError::Artifact(format!("missing generated type name for field {}", field.name())))
    }
}

fn collect_named(
    field: &CatalogField,
    row_name: &str,
    path: &str,
    names_by_field_id: &mut BTreeMap<String, String>,
    named: &mut Vec<NamedDefinition>,
    type_names: &mut BTreeSet<String>,
) -> SdkGenResult<()> {
    match field.mapping().arrow() {
        CatalogType::Struct { fields } => {
            validate_field_identifiers(path, fields)?;
            let name = bounded_identifier(
                &format!("{row_name}{}", pascal_identifier(path)),
                &format!("{row_name}:{path}"),
            );
            insert_type_name(field, &name, names_by_field_id, type_names)?;
            named.push(NamedDefinition {
                name,
                kind: NamedKind::Struct(fields.clone()),
            });
            for child in fields {
                collect_named(
                    child,
                    row_name,
                    &format!("{path}/{}", child.name()),
                    names_by_field_id,
                    named,
                    type_names,
                )?;
            }
        }
        CatalogType::List { element, .. } => {
            collect_named(
                element,
                row_name,
                &format!("{path}/item"),
                names_by_field_id,
                named,
                type_names,
            )?;
        }
        CatalogType::Map { key, value, .. } => {
            validate_field_identifiers(path, &[key.as_ref().clone(), value.as_ref().clone()])?;
            let name = bounded_identifier(
                &format!("{row_name}{}Entry", pascal_identifier(path)),
                &format!("{row_name}:{path}:entry"),
            );
            insert_type_name(field, &name, names_by_field_id, type_names)?;
            named.push(NamedDefinition {
                name,
                kind: NamedKind::Map {
                    key: key.clone(),
                    value: value.clone(),
                },
            });
            collect_named(
                key,
                row_name,
                &format!("{path}/key"),
                names_by_field_id,
                named,
                type_names,
            )?;
            collect_named(
                value,
                row_name,
                &format!("{path}/value"),
                names_by_field_id,
                named,
                type_names,
            )?;
        }
        CatalogType::Dictionary { .. } => {
            return Err(SdkGenError::UnsupportedType {
                path: path.to_owned(),
                reason: "dictionary encoding is rejected because generated Rust and TypeScript builders cannot share one lossless dictionary assignment policy".to_owned(),
            });
        }
        _ => {}
    }
    Ok(())
}

fn insert_type_name(
    field: &CatalogField,
    name: &str,
    names_by_field_id: &mut BTreeMap<String, String>,
    type_names: &mut BTreeSet<String>,
) -> SdkGenResult<()> {
    for identifier in [name.to_owned(), format!("{name}Builder")] {
        if !type_names.insert(identifier.clone()) {
            return Err(SdkGenError::IdentifierCollision {
                scope: "generated type names".to_owned(),
                identifier,
            });
        }
    }
    names_by_field_id.insert(field.id().to_string(), name.to_owned());
    Ok(())
}

fn validate_supported_field(field: &CatalogField, path: &str) -> SdkGenResult<()> {
    match field.mapping().arrow() {
        CatalogType::List {
            list_kind: ListKind::List | ListKind::LargeList | ListKind::FixedSize(_),
            element,
        } => validate_supported_field(element, &format!("{path}/item")),
        CatalogType::List {
            list_kind: ListKind::ListView | ListKind::LargeListView,
            ..
        } => Err(SdkGenError::UnsupportedType {
            path: path.to_owned(),
            reason: "Apache Arrow JavaScript does not expose lossless ListView construction".to_owned(),
        }),
        CatalogType::Struct { fields } => {
            for child in fields {
                validate_supported_field(child, &format!("{path}/{}", child.name()))?;
            }
            Ok(())
        }
        CatalogType::Map { key, value, .. } => {
            validate_supported_field(key, &format!("{path}/key"))?;
            validate_supported_field(value, &format!("{path}/value"))
        }
        CatalogType::Dictionary { .. } => Err(SdkGenError::UnsupportedType {
            path: path.to_owned(),
            reason: "dictionary encoding is unsupported by the shared deterministic row builder".to_owned(),
        }),
        _ => Ok(()),
    }
}

fn validate_field_identifiers(scope: &str, fields: &[CatalogField]) -> SdkGenResult<()> {
    let mut rust = BTreeSet::new();
    let mut methods = BTreeSet::new();
    for field in fields {
        let rust_name = rust_identifier(field.name());
        if !rust.insert(rust_name.clone()) {
            return Err(SdkGenError::IdentifierCollision {
                scope: scope.to_owned(),
                identifier: rust_name,
            });
        }
        let method = format!("set{}", pascal_identifier(field.name()));
        if !methods.insert(method.clone()) {
            return Err(SdkGenError::IdentifierCollision {
                scope: scope.to_owned(),
                identifier: method,
            });
        }
    }
    Ok(())
}

pub(crate) fn rust_identifier(value: &str) -> String {
    let mut output = snake_identifier(value);
    if RUST_KEYWORDS.contains(&output.as_str()) {
        output.push_str("_field");
    }
    output
}

pub(crate) fn pascal_identifier(value: &str) -> String {
    let words = words(value);
    let mut output = words
        .iter()
        .map(|word| {
            let mut characters = word.chars();
            characters.next().map_or_else(String::new, |first| {
                format!("{}{}", first.to_ascii_uppercase(), characters.as_str())
            })
        })
        .collect::<String>();
    if output.is_empty() {
        output.push_str("Field");
    }
    if output.starts_with(|character: char| character.is_ascii_digit()) {
        output.insert(0, 'F');
    }
    if matches!(output.as_str(), "Self" | "Super" | "Crate") {
        output.push_str("Type");
    }
    output
}

fn snake_identifier(value: &str) -> String {
    let mut output = words(value).join("_");
    if output.is_empty() {
        output.push_str("field");
    }
    if output.starts_with(|character: char| character.is_ascii_digit()) {
        output.insert_str(0, "f_");
    }
    output
}

fn words(value: &str) -> Vec<String> {
    let mut words = Vec::new();
    let mut current = String::new();
    for character in value.chars() {
        if character.is_ascii_alphanumeric() {
            current.push(character.to_ascii_lowercase());
        } else if !current.is_empty() {
            words.push(std::mem::take(&mut current));
        }
    }
    if !current.is_empty() {
        words.push(current);
    }
    words
}

fn bounded_identifier(value: &str, source: &str) -> String {
    const MAX: usize = 120;
    if value.len() <= MAX {
        return value.to_owned();
    }
    let digest = hex::encode(Sha256::digest(source.as_bytes()));
    format!("{}{}", &value[..MAX - 13], &digest[..12])
}

fn bounded_slug(value: &str, maximum: usize) -> String {
    let mut slug = words(value).join("-");
    if slug.is_empty() {
        slug.push_str("table");
    }
    if slug.len() > maximum {
        let digest = hex::encode(Sha256::digest(value.as_bytes()));
        slug = format!("{}-{}", &slug[..maximum - 13], &digest[..12]);
    }
    slug
}

fn sdk_manifest<'a>(
    context: &'a RenderContext<'_>,
    bundle: &ArtifactBundle,
    files: &BTreeMap<PathBuf, Vec<u8>>,
) -> SdkManifest<'a> {
    let outputs = files
        .iter()
        .map(|(path, bytes)| OutputFile {
            path: path.to_string_lossy().replace('\\', "/"),
            bytes: bytes.len(),
            sha256: format!("sha256:{}", hex::encode(Sha256::digest(bytes))),
        })
        .collect();
    SdkManifest {
        manifest_version: 1,
        generator: GeneratorIdentity {
            name: env!("CARGO_PKG_NAME"),
            version: env!("CARGO_PKG_VERSION"),
        },
        table: TableManifest {
            catalog: context.schema.identity().catalog().as_str(),
            schema: context.schema.identity().schema().as_str(),
            table: context.schema.identity().table().as_str(),
            table_id: context.schema.table_id().to_string(),
            version: context.schema.version().get(),
            fingerprint: context.schema.fingerprint().to_string(),
        },
        artifacts: ArtifactManifest {
            catalog_manifest: "descriptors/manifest.json",
            arrow_schema: ArtifactFile {
                path: "descriptors/schema.arrow",
                sha256: format!("sha256:{}", hex::encode(Sha256::digest(bundle.arrow_schema()))),
            },
            protobuf_descriptor: ArtifactFile {
                path: "descriptors/descriptor.pb",
                sha256: format!("sha256:{}", hex::encode(Sha256::digest(bundle.protobuf_descriptor()))),
            },
            protobuf_source: ArtifactFile {
                path: "descriptors/schema.proto",
                sha256: format!("sha256:{}", hex::encode(Sha256::digest(bundle.protobuf_source()))),
            },
        },
        packages: PackageManifest {
            rust: LanguagePackage {
                name: &context.rust_package,
                root: "rust",
            },
            typescript: LanguagePackage {
                name: &context.typescript_package,
                root: "typescript",
            },
        },
        flight: FlightManifest {
            do_put_path: "/arrow.flight.protocol.FlightService/DoPut",
            descriptor_path: vec![
                context.schema.identity().catalog().as_str().to_owned(),
                context.schema.identity().schema().as_str().to_owned(),
                context.schema.identity().table().as_str().to_owned(),
                format!("v{}", context.schema.version().get()),
            ],
            routing_metadata: RoutingManifest {
                protocol_version: "1",
                implementation_pool_values: ["dataflow"],
                catalog: context.schema.identity().catalog().as_str(),
                schema: context.schema.identity().schema().as_str(),
                table: context.schema.identity().table().as_str(),
                schema_fingerprint: context.schema.fingerprint().to_string(),
            },
        },
        outputs,
    }
}

fn generated_root_readme(context: &RenderContext<'_>) -> String {
    format!(
        "# Generated Arrowcol SDK\n\n\
This directory is a deterministic artifact for `{}` schema version `{}`.\n\
It is replaced as one unit; do not edit files in place.\n\n\
- Rust package: `{}` under `rust/`\n\
- TypeScript package: `{}` under `typescript/`\n\
- Catalog descriptors: `descriptors/`\n\
- Machine-readable integration manifest: `sdk-manifest.json`\n",
        context.schema.identity(),
        context.schema.version(),
        context.rust_package,
        context.typescript_package,
    )
}

#[derive(Serialize)]
struct SdkManifest<'a> {
    manifest_version: u32,
    generator: GeneratorIdentity<'a>,
    table: TableManifest<'a>,
    artifacts: ArtifactManifest<'a>,
    packages: PackageManifest<'a>,
    flight: FlightManifest<'a>,
    outputs: Vec<OutputFile>,
}

#[derive(Serialize)]
struct GeneratorIdentity<'a> {
    name: &'a str,
    version: &'a str,
}

#[derive(Serialize)]
struct TableManifest<'a> {
    catalog: &'a str,
    schema: &'a str,
    table: &'a str,
    table_id: String,
    version: u64,
    fingerprint: String,
}

#[derive(Serialize)]
struct ArtifactManifest<'a> {
    catalog_manifest: &'a str,
    arrow_schema: ArtifactFile<'a>,
    protobuf_descriptor: ArtifactFile<'a>,
    protobuf_source: ArtifactFile<'a>,
}

#[derive(Serialize)]
struct ArtifactFile<'a> {
    path: &'a str,
    sha256: String,
}

#[derive(Serialize)]
struct PackageManifest<'a> {
    rust: LanguagePackage<'a>,
    typescript: LanguagePackage<'a>,
}

#[derive(Serialize)]
struct LanguagePackage<'a> {
    name: &'a str,
    root: &'a str,
}

#[derive(Serialize)]
struct FlightManifest<'a> {
    do_put_path: &'a str,
    descriptor_path: Vec<String>,
    routing_metadata: RoutingManifest<'a>,
}

#[derive(Serialize)]
struct RoutingManifest<'a> {
    protocol_version: &'a str,
    implementation_pool_values: [&'a str; 1],
    catalog: &'a str,
    schema: &'a str,
    table: &'a str,
    schema_fingerprint: String,
}

#[derive(Serialize)]
struct OutputFile {
    path: String,
    bytes: usize,
    sha256: String,
}

const RUST_KEYWORDS: &[&str] = &[
    "as", "break", "const", "continue", "crate", "else", "enum", "extern", "false", "fn", "for", "if", "impl", "in",
    "let", "loop", "match", "mod", "move", "mut", "pub", "ref", "return", "self", "Self", "static", "struct", "super",
    "trait", "true", "type", "unsafe", "use", "where", "while", "async", "await", "dyn", "abstract", "become", "box",
    "do", "final", "macro", "override", "priv", "typeof", "unsized", "virtual", "yield", "try",
];
