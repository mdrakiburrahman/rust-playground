use std::{fs, path::PathBuf, time::SystemTime};

use arrowcol_sdk_gen::{SafeOutputPath, package_generated_sdk};

#[test]
fn package_can_rename_and_read_back_on_the_host_mounted_workspace() -> Result<(), Box<dyn std::error::Error>> {
    let workspace = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .ok_or("crates directory")?
        .parent()
        .ok_or("workspace")?
        .to_path_buf();
    let nonce = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)?.as_nanos();
    let root = PathBuf::from(format!(".temp/host-package-test-{}-{nonce}", std::process::id()));
    let input = SafeOutputPath::new(&workspace, root.join("input"))?;
    let archive = SafeOutputPath::new(&workspace, root.join("contract-sdk.tar.gz"))?;
    fs::create_dir_all(input.absolute())?;
    fs::write(input.absolute().join("contract.txt"), b"frozen contract")?;
    let first = package_generated_sdk(&input, &archive)?;
    let first_bytes = fs::read(archive.absolute())?;
    let second = package_generated_sdk(&input, &archive)?;
    assert_eq!(first, second);
    assert_eq!(first_bytes, fs::read(archive.absolute())?);
    fs::remove_dir_all(workspace.join(root))?;
    Ok(())
}
