#![allow(clippy::expect_used)]
use arrowcol_sdk_ffi::{arrowcol_destroy, arrowcol_free_error, arrowcol_open, arrowcol_push};
use std::ffi::{CStr, CString};

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
