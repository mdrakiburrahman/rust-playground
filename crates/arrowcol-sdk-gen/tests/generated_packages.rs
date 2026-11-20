mod common;

use std::{fs, process::Stdio};

use arrowcol_sdk_gen::{ArtifactBundle, generate_sdk};

use common::{assert_success, bundle, command, reset, safe, workspace};

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
