#![allow(clippy::expect_used)]
use arrowcol_sdk_ffi::{
    arrowcol_compile_contract, arrowcol_destroy, arrowcol_free_error, arrowcol_open, arrowcol_push,
};
use std::ffi::{CStr, CString};

#[test]
fn compiled_contract_matches_the_fixture_compiler() {
    let fixture =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../arrowcol-sdk-runtime/tests/fixtures/events");
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(fixture.join("manifest.json")).expect("manifest")).expect("json");
    let ddl = manifest["schema"]["canonical_ddl"].as_str().expect("canonical ddl");
    let out = std::env::temp_dir().join(format!("arrowcol-compile-{}", std::process::id()));
    let ddl = CString::new(ddl).expect("ddl");
    let path = CString::new(out.to_string_lossy().as_bytes()).expect("path");
    let error = unsafe { arrowcol_compile_contract(ddl.as_ptr(), path.as_ptr()) };
    assert!(
        error.is_null(),
        "{}",
        unsafe { CStr::from_ptr(error) }.to_string_lossy()
    );
    assert_eq!(
        std::fs::read(out.join("schema.arrow")).expect("compiled"),
        std::fs::read(fixture.join("schema.arrow")).expect("fixture")
    );
    let mut expected = manifest;
    expected["schema"]["metadata"]["location"] = serde_json::Value::Null;
    let compiled: serde_json::Value =
        serde_json::from_slice(&std::fs::read(out.join("manifest.json")).expect("compiled manifest")).expect("json");
    assert_eq!(compiled, expected);

    let descriptors = std::fs::read(out.join("descriptor.pb")).expect("descriptor");
    let mut handle = std::ptr::dangling_mut();
    let error = unsafe {
        arrowcol_open(
            c"http://127.0.0.1:9".as_ptr(),
            path.as_ptr(),
            descriptors.as_ptr(),
            descriptors.len(),
            c"arrowcol.demo.public.Events".as_ptr(),
            c"null".as_ptr(),
            &mut handle,
        )
    };
    assert!(handle.is_null());
    let message = unsafe { CStr::from_ptr(error) }.to_string_lossy().into_owned();
    assert!(
        message.contains("StreamOptions"),
        "model validation should pass before options: {message}"
    );
    unsafe { arrowcol_free_error(error) };
    std::fs::remove_dir_all(&out).expect("cleanup");

    let error = unsafe { arrowcol_compile_contract(c"ALTER TABLE a.b.c ADD COLUMNS (x INT)".as_ptr(), path.as_ptr()) };
    assert!(!error.is_null());
    assert!(!out.exists());
    unsafe { arrowcol_free_error(error) };
}

#[test]
fn shared_open_rejects_compiled_model_before_endpoint_and_clears_output() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../arrowcol-sdk-runtime/tests/fixtures/events");
    let descriptors = std::fs::read(path.join("descriptor.pb")).expect("descriptor");
    let path = CString::new(path.to_string_lossy().as_bytes()).expect("path");
    let endpoint = c"not a valid endpoint";
    let mut handle = std::ptr::dangling_mut();
    let error = unsafe {
        arrowcol_open(
            endpoint.as_ptr(),
            path.as_ptr(),
            descriptors.as_ptr(),
            descriptors.len(),
            c"unrelated.Message".as_ptr(),
            c"{}".as_ptr(),
            &mut handle,
        )
    };
    assert!(handle.is_null());
    assert!(!error.is_null());
    assert!(
        unsafe { CStr::from_ptr(error) }
            .to_str()
            .expect("error")
            .contains("compiled message not found")
    );
    unsafe {
        arrowcol_free_error(error);
    }
}

#[test]
fn shared_exports_report_invalid_arguments_without_unwinding() {
    let mut handle = std::ptr::dangling_mut();
    let error = unsafe {
        arrowcol_open(
            std::ptr::null(),
            std::ptr::null(),
            std::ptr::null(),
            0,
            std::ptr::null(),
            std::ptr::null(),
            &mut handle,
        )
    };
    assert!(handle.is_null());
    assert!(!error.is_null());
    unsafe {
        arrowcol_free_error(error);
        arrowcol_destroy(std::ptr::null_mut());
        arrowcol_free_error(std::ptr::null_mut());
    }
    let error = unsafe { arrowcol_push(std::ptr::null_mut(), std::ptr::null(), 0, 2) };
    assert!(
        unsafe { CStr::from_ptr(error) }
            .to_str()
            .expect("error")
            .contains("batch flag")
    );
    unsafe {
        arrowcol_free_error(error);
    }
}
