use std::{env, error::Error, path::PathBuf};

fn main() -> Result<(), Box<dyn Error>> {
    println!("cargo:rerun-if-changed=proto/arrowcol_codegen.proto");
    println!("cargo:rerun-if-changed=../arrowcol-catalog/proto/google/protobuf/descriptor.proto");
    println!("cargo:rerun-if-env-changed=PROTOC");
    let output = PathBuf::from(env::var("OUT_DIR")?);
    prost_build::Config::new()
        .file_descriptor_set_path(output.join("codegen.pb"))
        .compile_protos(
            &["proto/arrowcol_codegen.proto"],
            &["proto", "../arrowcol-catalog/proto"],
        )?;
    Ok(())
}
