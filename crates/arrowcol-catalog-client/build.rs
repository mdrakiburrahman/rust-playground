fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/catalog.proto");
    tonic_prost_build::configure().compile_protos(&["proto/catalog.proto"], &["proto"])?;
    Ok(())
}
