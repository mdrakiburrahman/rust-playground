fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/catalog.proto");
    // Signed requests must encode map fields identically before and after transport.
    tonic_prost_build::configure()
        .btree_map(".arrowcol.catalog.v1.TableSnapshot.properties")
        .compile_protos(&["proto/catalog.proto"], &["proto"])?;
    Ok(())
}
