mod common;

use std::{fs, process::Stdio};

use arrowcol_sdk_gen::{ArtifactBundle, generate_sdk, render_sdk_files};

use common::{assert_success, bundle, command, reset, safe, schema_from_ddl, workspace};

#[test]
fn models_embed_the_neutral_contract_and_sdk_sources_are_schema_independent() {
    let first = ArtifactBundle::from_schema(schema_from_ddl(
        "CREATE TABLE demo.public.events (id BIGINT NOT NULL) USING DELTA",
    ))
    .unwrap_or_else(|error| panic!("first contract: {error}"));
    let second = ArtifactBundle::from_schema(schema_from_ddl(
        "CREATE TABLE other.public.demo (amount DECIMAL(8,3), text STRING) USING DELTA",
    ))
    .unwrap_or_else(|error| panic!("second contract: {error}"));
    let first_files = render_sdk_files(&first).unwrap_or_else(|error| panic!("first generation: {error}"));
    let second_files = render_sdk_files(&second).unwrap_or_else(|error| panic!("second generation: {error}"));
    let runtime = |files: &std::collections::BTreeMap<std::path::PathBuf, Vec<u8>>| {
        files
            .iter()
            .filter(|(path, _)| path.starts_with("runtime"))
            .map(|(path, bytes)| (path.clone(), bytes.clone()))
            .collect::<std::collections::BTreeMap<_, _>>()
    };
    assert!(!runtime(&first_files).is_empty());
    assert_eq!(
        runtime(&first_files),
        runtime(&second_files),
        "SDK packages cannot depend on the schema"
    );
    let same_shape = ArtifactBundle::from_schema(schema_from_ddl(
        "CREATE TABLE another.public.events (id BIGINT NOT NULL) USING DELTA",
    ))
    .unwrap_or_else(|error| panic!("same-shaped contract: {error}"));
    let same_shape_files =
        render_sdk_files(&same_shape).unwrap_or_else(|error| panic!("same-shaped generation: {error}"));
    for language in ["rust", "cpp", "csharp"] {
        let path = std::path::PathBuf::from(format!("models/{language}/package.json"));
        let first: serde_json::Value =
            serde_json::from_slice(&first_files[&path]).unwrap_or_else(|error| panic!("first metadata: {error}"));
        let other: serde_json::Value =
            serde_json::from_slice(&same_shape_files[&path]).unwrap_or_else(|error| panic!("other metadata: {error}"));
        assert_ne!(
            first["name"], other["name"],
            "same-shaped tables need distinct package identities"
        );
        if language == "cpp" {
            assert_ne!(first["header"], other["header"], "protoc file symbols must not collide");
        }
    }
    for (files, bundle) in [(&first_files, &first), (&second_files, &second)] {
        for language in ["rust", "cpp", "csharp"] {
            for (name, bytes) in [
                ("manifest.json", bundle.manifest()),
                ("schema.arrow", bundle.arrow_schema()),
                ("descriptor.pb", bundle.protobuf_descriptor()),
                ("schema.proto", bundle.protobuf_source()),
            ] {
                assert_eq!(
                    files[&std::path::PathBuf::from(format!("models/{language}/contract/{name}"))],
                    bytes
                );
            }
        }
        for manifest in [
            "models/rust/Cargo.toml",
            "models/csharp/Models.csproj",
            "models/cpp/CMakeLists.txt",
        ] {
            let source = std::str::from_utf8(&files[&std::path::PathBuf::from(manifest)])
                .unwrap_or_else(|error| panic!("{error}"));
            assert!(
                !source.contains("arrowcol-sdk") && !source.contains("Arrowcol.Sdk"),
                "models cannot depend on SDK"
            );
        }
    }
}

#[test]
fn protobuf_stream_adapter_checks_presence_ranges_and_decimal_values() {
    let output = safe("target/arrowcol-sdk-gen-tests/typed-stream");
    reset(&output);
    let bundle = ArtifactBundle::from_schema(schema_from_ddl(
        "CREATE TABLE demo.public.events (id TINYINT NOT NULL, price DECIMAL(5,2), observed_at TIMESTAMP NOT NULL, payload STRING) USING DELTA"
    )).unwrap_or_else(|error| panic!("artifacts: {error}"));
    generate_sdk(&bundle, &output).unwrap_or_else(|error| panic!("generation: {error}"));
    let model = fs::read_to_string(output.absolute().join("cpp/model/schema.proto"))
        .unwrap_or_else(|error| panic!("C++ model: {error}"));
    assert!(model.contains("package arrowcol.demo.public_;"));
    assert!(
        std::str::from_utf8(bundle.protobuf_source()).is_ok_and(|text| text.contains("package arrowcol.demo.public;"))
    );
    let package = fs::read_to_string(output.absolute().join("rust/Cargo.toml"))
        .unwrap_or_else(|error| panic!("package: {error}"));
    let name = package
        .lines()
        .find_map(|line| line.strip_prefix("name = \"").and_then(|value| value.strip_suffix('"')))
        .unwrap_or_else(|| panic!("package name"))
        .replace('-', "_");
    let test = format!(
        r#"use {name}::{{ArrowcolEventsContract, ArrowcolEventsStream, EventsContract, EventsStream, proto::Events}};
use arrowcol_sdk_runtime::{{ArrowcolContract, Contract}};
use prost::Message;
use arrow_array::{{Array, Decimal128Array}};
#[test]
fn conversion_preserves_values_and_rejects_loss() {{
    let contract = ArrowcolContract::load(concat!(env!("CARGO_MANIFEST_DIR"), "/../descriptors")).unwrap();
    let dynamic = |event: &Events| contract.record_batch(&[contract.decode(&event.encode_to_vec())?]);
    let mut event = Events {{ id: Some(127), price: Some("12.34".into()), observed_at: Some(1700000000000000), payload: None }};
    let batch = ArrowcolEventsContract::record_batch(&[event.clone()]).unwrap();
    assert_eq!(batch, dynamic(&event).unwrap(), "generic conversion must match the generated adapter");
    assert_eq!(batch, EventsContract::record_batch(&[event.clone()]).unwrap());
    let legacy: Option<EventsStream> = None;
    let _branded: Option<ArrowcolEventsStream> = legacy;
    assert_eq!(batch.column(1).as_any().downcast_ref::<Decimal128Array>().unwrap().value(0), 1234);
    assert!(batch.column(3).is_null(0));
    event.id = None;
    assert!(EventsContract::record_batch(&[event.clone()]).is_err());
    assert!(dynamic(&event).is_err());
    event.id = Some(128);
    assert!(EventsContract::record_batch(&[event.clone()]).is_err());
    assert!(dynamic(&event).is_err());
    event.id = Some(1);
    for value in ["12.345", "1000.00", "1e2"] {{
        event.price = Some(value.into());
        assert!(EventsContract::record_batch(&[event.clone()]).is_err());
        assert!(dynamic(&event).is_err());
    }}
}}
"#
    );
    fs::create_dir_all(output.absolute().join("rust/tests")).unwrap_or_else(|error| panic!("test directory: {error}"));
    fs::write(output.absolute().join("rust/tests/stream.rs"), test)
        .unwrap_or_else(|error| panic!("test source: {error}"));
    let result = command("cargo")
        .args(["test", "--quiet", "--manifest-path"])
        .arg(output.absolute().join("rust/Cargo.toml"))
        .args(["--test", "stream"])
        .env(
            "CARGO_TARGET_DIR",
            workspace().join("target/arrowcol-sdk-gen-tests/generated-cargo-target"),
        )
        .output()
        .unwrap_or_else(|error| panic!("run contract test: {error}"));
    assert_success(result, "protobuf adapter conversion");
    let result = command("cargo")
        .args(["test", "--quiet", "--manifest-path"])
        .arg(output.absolute().join("models/rust/Cargo.toml"))
        .env(
            "CARGO_TARGET_DIR",
            workspace().join("target/arrowcol-sdk-gen-tests/generated-cargo-target"),
        )
        .output()
        .unwrap_or_else(|error| panic!("compile independent protobuf models: {error}"));
    assert_success(result, "independent protobuf models compilation");
}

#[test]
fn generated_rust_compiles_and_typescript_typechecks_and_executes() {
    let output = safe("target/arrowcol-sdk-gen-tests/generated-packages");
    reset(&output);
    let schema = bundle().schema().clone();
    let mut metadata = schema.metadata().clone();
    metadata.properties = arrowcol_catalog::materialize_protobuf_properties(&metadata.properties)
        .unwrap_or_else(|error| panic!("native defaults: {error}"));
    let bundle = ArtifactBundle::from_schema(
        schema
            .with_metadata(metadata)
            .unwrap_or_else(|error| panic!("metadata: {error}")),
    )
    .unwrap_or_else(|error| panic!("native option artifacts: {error}"));
    generate_sdk(&bundle, &output).unwrap_or_else(|error| panic!("generated package creation failed: {error}"));

    let rust_manifest = output.absolute().join("rust/Cargo.toml");
    let cargo_target = workspace().join("target/arrowcol-sdk-gen-tests/generated-cargo-target");
    let rust = command("cargo")
        .args(["test", "--manifest-path"])
        .arg(&rust_manifest)
        .env("CARGO_TARGET_DIR", cargo_target)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .unwrap_or_else(|error| panic!("failed to start generated Rust tests: {error}"));
    assert_success(rust, "generated cargo test");

    let tsconfig = output.absolute().join("typescript/tsconfig.json");
    let typescript = command("npx")
        .args(["--no-install", "tsc", "--project"])
        .arg(&tsconfig)
        .arg("--noEmit")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .unwrap_or_else(|error| panic!("failed to start generated TypeScript check: {error}"));
    assert_success(typescript, "generated TypeScript typecheck");

    let test_file = output.absolute().join("typescript/test/generated.test.ts");
    let execution = command("npx")
        .args(["--no-install", "tsx", "--test"])
        .arg(&test_file)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .unwrap_or_else(|error| panic!("failed to start generated TypeScript test: {error}"));
    assert_success(execution, "generated TypeScript fixture");

    let rust_source = fs::read_to_string(output.absolute().join("rust/src/lib.rs"))
        .unwrap_or_else(|error| panic!("read generated Rust: {error}"));
    assert!(!rust_source.contains("unsafe "));
    assert!(!rust_source.contains(".unwrap("));
    assert!(!rust_source.contains(".expect("));
}
