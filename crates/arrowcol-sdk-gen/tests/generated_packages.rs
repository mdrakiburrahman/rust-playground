mod common;

use std::{fs, process::Stdio};

use arrowcol_sdk_gen::{ArtifactBundle, generate_sdk};

use common::{assert_success, bundle, command, reset, safe, schema_from_ddl, workspace};

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
        r#"use {name}::{{EventsContract, proto::Events}};
use arrowcol_sdk_runtime::Contract;
use arrow_array::{{Array, Decimal128Array}};
#[test]
fn conversion_preserves_values_and_rejects_loss() {{
    let mut event = Events {{ id: Some(127), price: Some("12.34".into()), observed_at: Some(1700000000000000), payload: None }};
    let batch = EventsContract::record_batch(&[event.clone()]).unwrap();
    assert_eq!(batch.column(1).as_any().downcast_ref::<Decimal128Array>().unwrap().value(0), 1234);
    assert!(batch.column(3).is_null(0));
    event.id = None;
    assert!(EventsContract::record_batch(&[event.clone()]).is_err());
    event.id = Some(128);
    assert!(EventsContract::record_batch(&[event.clone()]).is_err());
    event.id = Some(1);
    for value in ["12.345", "1000.00", "1e2"] {{
        event.price = Some(value.into());
        assert!(EventsContract::record_batch(&[event.clone()]).is_err());
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
