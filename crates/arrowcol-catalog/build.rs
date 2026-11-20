use std::{env, error::Error, path::PathBuf, process::Command};

fn main() -> Result<(), Box<dyn Error>> {
    println!("cargo:rerun-if-changed=proto/options.proto");
    println!("cargo:rerun-if-changed=proto/google/protobuf/descriptor.proto");
    println!("cargo:rerun-if-env-changed=PROTOC");
    let output = PathBuf::from(env::var("OUT_DIR")?).join("options.pb");
    let result = Command::new(env::var_os("PROTOC").unwrap_or_else(|| "protoc".into()))
        .arg("--proto_path=proto")
        .arg("--include_imports")
        .arg(format!("--descriptor_set_out={}", output.display()))
        .arg("proto/options.proto")
        .output()?;
    if !result.status.success() {
        return Err(format!("compiling catalog options: {}", String::from_utf8_lossy(&result.stderr)).into());
    }
    Ok(())
}
