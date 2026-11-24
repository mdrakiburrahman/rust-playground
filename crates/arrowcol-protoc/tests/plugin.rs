use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use arrowcol_catalog::{
    CatalogSchemaVersion, DdlCommand, DdlParser, SchemaVersion, deterministic_table_id, materialize_protobuf_properties,
};
use arrowcol_protoc::{CodeGeneratorRequest, request_bundle};
use arrowcol_sdk_gen::{ArtifactBundle, render_sdk_files};
use prost::Message;

#[derive(Clone, PartialEq, Message)]
struct RawSet {
    #[prost(bytes = "vec", repeated, tag = "1")]
    file: Vec<Vec<u8>>,
}

struct Fixture {
    root: PathBuf,
    bundle: ArtifactBundle,
}

static NEXT_FIXTURE: AtomicU64 = AtomicU64::new(0);

impl Fixture {
    fn new() -> Self {
        let DdlCommand::CreateTable(create) = DdlParser::default()
            .parse(include_str!("../../arrowcol-sdk-gen/fixtures/telemetry-events.sql"))
            .unwrap_or_else(|error| panic!("{error}"))
        else {
            panic!("CREATE fixture required")
        };
        let mut metadata = create.metadata().clone();
        metadata.properties =
            materialize_protobuf_properties(&metadata.properties).unwrap_or_else(|error| panic!("{error}"));
        let schema = CatalogSchemaVersion::new(
            deterministic_table_id(create.identity()),
            create.identity().clone(),
            SchemaVersion::FIRST,
            create.fields().to_vec(),
            create.canonical_ddl(),
        )
        .and_then(|schema| schema.with_metadata(metadata))
        .unwrap_or_else(|error| panic!("{error}"));
        let bundle = ArtifactBundle::from_schema(schema).unwrap_or_else(|error| panic!("{error}"));
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let sequence = NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!("arrowcol-protoc-{}-{unique}-{sequence}", std::process::id()));
        fs::create_dir(&root).unwrap_or_else(|error| panic!("{error}"));
        arrowcol_protoc::export(&bundle, &root.join("compiler"), "events.proto")
            .unwrap_or_else(|error| panic!("{error}"));
        Self { root, bundle }
    }

    fn request(&self) -> CodeGeneratorRequest {
        let output = Command::new("protoc")
            .arg(format!("-I{}", self.root.join("compiler").display()))
            .args(["--include_imports", "--include_source_info"])
            .arg(format!("--descriptor_set_out={}", self.root.join("input.pb").display()))
            .arg("events.proto")
            .output()
            .unwrap_or_else(|error| panic!("{error}"));
        assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
        let bytes = fs::read(self.root.join("input.pb")).unwrap_or_else(|error| panic!("{error}"));
        CodeGeneratorRequest {
            file_to_generate: vec!["events.proto".to_owned()],
            parameter: None,
            proto_file: RawSet::decode(bytes.as_slice())
                .unwrap_or_else(|error| panic!("{error}"))
                .file,
        }
    }

    fn mutate(&self, path: &str, from: &str, to: &str) {
        let path = self.root.join("compiler").join(path);
        let text = fs::read_to_string(&path).unwrap_or_else(|error| panic!("{error}"));
        assert!(text.contains(from), "mutation must match");
        fs::write(path, text.replacen(from, to, 1)).unwrap_or_else(|error| panic!("{error}"));
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.root).unwrap_or_else(|error| panic!("test cleanup: {error}"));
    }
}

#[test]
fn descriptor_only_input_retains_full_ir_and_native_property_defaults() {
    let f = Fixture::new();
    let restored = request_bundle(&f.request()).unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(restored.schema(), f.bundle.schema());
    assert_eq!(restored.manifest(), f.bundle.manifest());
    assert_eq!(
        render_sdk_files(&restored).unwrap_or_else(|error| panic!("{error}")),
        render_sdk_files(&f.bundle).unwrap_or_else(|error| panic!("{error}"))
    );
}

#[test]
fn changed_row_or_property_definitions_cannot_override_the_frozen_ir() {
    let f = Fixture::new();
    f.mutate("events.proto", "event_id", "renamed_event");
    assert!(request_bundle(&f.request()).is_err_and(|error| error.to_string().contains("descriptor differs")));
    let f = Fixture::new();
    f.mutate(
        "options.proto",
        "default = INGEST_TIER_STANDARD",
        "default = INGEST_TIER_BULK",
    );
    assert!(request_bundle(&f.request()).is_err_and(|error| error.to_string().contains("descriptor differs")));
}

#[test]
fn invalid_plugin_requests_fail_closed() {
    let f = Fixture::new();
    let mut request = f.request();
    request.parameter = Some("unknown=true".to_owned());
    assert!(request_bundle(&request).is_err());
    request.parameter = None;
    request.file_to_generate.push("another.proto".to_owned());
    assert!(request_bundle(&request).is_err());
    for entry in [
        "../bad.proto",
        "options.proto",
        "/bad.proto",
        "x\\y.proto",
        "-bad.proto",
    ] {
        assert!(arrowcol_protoc::validate_entry(entry).is_err());
    }
}

#[test]
fn unsupported_api_binding_is_rejected() {
    let f = Fixture::new();
    f.mutate(
        "events.proto",
        "operation: ARROW_FLIGHT_DO_PUT",
        "operation: OPERATION_UNSPECIFIED",
    );
    assert!(request_bundle(&f.request()).is_err_and(|error| error.to_string().contains("unsupported API binding")));
}

#[test]
fn real_protoc_composes_two_plugins_and_preserves_direct_sdk_output() {
    let f = Fixture::new();
    let sdk = f.root.join("sdk");
    fs::create_dir(&sdk).unwrap_or_else(|error| panic!("{error}"));
    let output = Command::new("protoc")
        .arg(format!("-I{}", f.root.join("compiler").display()))
        .arg(format!(
            "--plugin=protoc-gen-arrowcol-sdk={}",
            env!("CARGO_BIN_EXE_protoc-gen-arrowcol-sdk")
        ))
        .arg(format!(
            "--plugin=protoc-gen-team={}",
            env!("CARGO_BIN_EXE_protoc-gen-arrowcol-example")
        ))
        .arg(format!("--arrowcol-sdk_out={}", sdk.display()))
        .arg(format!("--team_out={}", sdk.display()))
        .arg("events.proto")
        .output()
        .unwrap_or_else(|error| panic!("{error}"));
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    for (name, bytes) in render_sdk_files(&f.bundle).unwrap_or_else(|error| panic!("{error}")) {
        let portable_name = name.to_string_lossy().replace('\\', "/");
        if portable_name.ends_with("/descriptor.pb") || portable_name.ends_with("/schema.arrow") {
            continue;
        }
        assert_eq!(
            fs::read(sdk.join(&name)).unwrap_or_else(|error| panic!("{error}")),
            bytes,
            "{}",
            name.display()
        );
    }
    let extension =
        fs::read_to_string(sdk.join(Path::new("typescript/team.ts"))).unwrap_or_else(|error| panic!("{error}"));
    assert!(extension.contains("createDoPutPlan(recordBatch(rows))"));
}
