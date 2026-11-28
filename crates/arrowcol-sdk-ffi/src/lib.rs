//! Narrow C ABI implementation shared by generated table adapters.
//! Callers serialize access to a handle. All pointers must remain valid for the duration of a call.
use arrowcol_catalog::{
    CatalogSchemaVersion, DdlCommand, DdlParser, SchemaVersion, deterministic_table_id, generate_descriptor_artifacts,
};
use arrowcol_sdk_runtime::{ArrowcolContract, ArrowcolStream, Contract, Error, GeneratedStream, Result, StreamOptions};
use prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage};
use std::{
    ffi::{CStr, CString, c_char, c_void},
    panic::{AssertUnwindSafe, catch_unwind},
    path::Path,
};

trait Producer: Send {
    fn push(&mut self, data: &[u8], batch: bool) -> Result<()>;
    fn flush(&mut self) -> Result<()>;
    fn close(&mut self) -> Result<()>;
}
struct Typed<C: Contract> {
    stream: GeneratedStream<C>,
    runtime: tokio::runtime::Runtime,
    max_rows: usize,
}
impl<C: Contract> Producer for Typed<C>
where
    C::Event: Message + Default,
{
    fn push(&mut self, mut data: &[u8], batch: bool) -> Result<()> {
        let mut events = Vec::new();
        if batch {
            while !data.is_empty() {
                if events.len() == self.max_rows {
                    return Err(Error::invalid("batch exceeds batch_rows"));
                }
                events.push(C::Event::decode_length_delimited(&mut data).map_err(Error::invalid)?);
            }
        } else {
            events.push(C::Event::decode(data).map_err(Error::invalid)?);
        }
        self.runtime.block_on(self.stream.push_batch(&events))
    }
    fn flush(&mut self) -> Result<()> {
        self.runtime.block_on(self.stream.flush())
    }
    fn close(&mut self) -> Result<()> {
        self.runtime.block_on(self.stream.close())
    }
}
struct Handle(Box<dyn Producer>);

fn boundary(action: impl FnOnce() -> Result<()>) -> *mut c_char {
    let error = match catch_unwind(AssertUnwindSafe(action)) {
        Ok(Ok(())) => return std::ptr::null_mut(),
        Ok(Err(error)) => error.to_string(),
        Err(_) => "native_failure: producer panicked; delivery outcome may be unknown".to_owned(),
    };
    // Replacement prevents interior NUL from truncating or suppressing an error at the C boundary.
    match CString::new(error.replace('\0', "\\0")) {
        Ok(error) => error.into_raw(),
        Err(_) => std::process::abort(),
    }
}

/// Opens a generated contract's producer. Returned errors belong to `free_error`.
///
/// # Safety
/// `endpoint` and `options` must be NUL-terminated UTF-8 strings; `output` must be writable.
pub unsafe fn open<C: Contract>(
    endpoint: *const c_char,
    options: *const c_char,
    output: *mut *mut c_void,
) -> *mut c_char
where
    C::Event: Message + Default,
{
    boundary(|| {
        if endpoint.is_null() || options.is_null() || output.is_null() {
            return Err(Error::invalid("null open argument"));
        }
        unsafe {
            *output = std::ptr::null_mut();
        }
        let endpoint = unsafe { CStr::from_ptr(endpoint) }.to_str().map_err(Error::invalid)?;
        let options: StreamOptions =
            serde_json::from_slice(unsafe { CStr::from_ptr(options) }.to_bytes()).map_err(Error::invalid)?;
        let max_rows = options.batch_rows;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .map_err(Error::invalid)?;
        let stream = runtime.block_on(GeneratedStream::<C>::connect(endpoint, options))?;
        let handle = Box::new(Handle(Box::new(Typed {
            stream,
            runtime,
            max_rows,
        })));
        unsafe {
            *output = Box::into_raw(handle).cast();
        }
        Ok(())
    })
}

/// Copies/decodes protobuf input before returning. Batch input is length-delimited protobuf messages.
///
/// # Safety
/// `handle` must be a live handle from `open`, used exclusively. `data` must point to `len` readable bytes.
pub unsafe fn push(handle: *mut c_void, data: *const u8, len: usize, batch: bool) -> *mut c_char {
    boundary(|| {
        if handle.is_null() || (len > 0 && data.is_null()) || len > 16 * 1024 * 1024 {
            return Err(Error::invalid("invalid handle/data or encoded input exceeds 16 MiB"));
        }
        let data = if len == 0 {
            &[]
        } else {
            unsafe { std::slice::from_raw_parts(data, len) }
        };
        unsafe { &mut *handle.cast::<Handle>() }.0.push(data, batch)
    })
}

/// Waits for preceding records to be admitted, not for sink publication.
///
/// # Safety
/// `handle` must be a live, exclusively accessed handle from `open`.
pub unsafe fn flush(handle: *mut c_void) -> *mut c_char {
    boundary(|| {
        if handle.is_null() {
            return Err(Error::invalid("null stream"));
        }
        unsafe { &mut *handle.cast::<Handle>() }.0.flush()
    })
}

/// Flushes and closes; retains the handle until `destroy`.
///
/// # Safety
/// `handle` must be a live, exclusively accessed handle from `open`.
pub unsafe fn close(handle: *mut c_void) -> *mut c_char {
    boundary(|| {
        if handle.is_null() {
            return Err(Error::invalid("null stream"));
        }
        unsafe { &mut *handle.cast::<Handle>() }.0.close()
    })
}

/// Frees a handle without an implicit flush. Call `close` first and inspect its result.
///
/// # Safety
/// A non-null handle must come from `open` and be destroyed exactly once, with no concurrent calls.
pub unsafe fn destroy(handle: *mut c_void) {
    if !handle.is_null() {
        // A panic must never unwind into the foreign runtime.
        if catch_unwind(AssertUnwindSafe(|| unsafe {
            drop(Box::from_raw(handle.cast::<Handle>()));
        }))
        .is_err()
        {
            std::process::abort();
        }
    }
}

/// Releases an error allocated by this library.
///
/// # Safety
/// `error` must be null or a previously unfreed error pointer returned by this library.
pub unsafe fn free_error(error: *mut c_char) {
    if !error.is_null() {
        unsafe {
            drop(CString::from_raw(error));
        }
    }
}

struct Reflected {
    stream: ArrowcolStream<DynamicMessage>,
    contract: ArrowcolContract,
    runtime: tokio::runtime::Runtime,
    max_rows: usize,
}
impl Producer for Reflected {
    fn push(&mut self, mut data: &[u8], batch: bool) -> Result<()> {
        let mut events = Vec::new();
        if batch {
            while !data.is_empty() {
                if events.len() == self.max_rows {
                    return Err(Error::invalid("batch exceeds batch_rows"));
                }
                let len = prost::decode_length_delimiter(&mut data).map_err(Error::invalid)?;
                let bytes = data
                    .get(..len)
                    .ok_or_else(|| Error::invalid("truncated protobuf batch"))?;
                events.push(self.contract.decode(bytes)?);
                data = &data[len..];
            }
        } else {
            events.push(self.contract.decode(data)?);
        }
        self.runtime.block_on(self.stream.push_batch(&events))
    }
    fn flush(&mut self) -> Result<()> {
        self.runtime.block_on(self.stream.flush())
    }
    fn close(&mut self) -> Result<()> {
        self.runtime.block_on(self.stream.close())
    }
}

/// Opens the schema-independent producer. Descriptor bytes are a compiled model's
/// FileDescriptorSet, including imports. Model binding is checked before any network call.
///
/// # Safety
/// Strings must be NUL-terminated UTF-8. Descriptor bytes must remain readable for `model_len`.
/// `output` must be writable and the resulting handle must be used exclusively.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn arrowcol_open(
    endpoint: *const c_char,
    contract_path: *const c_char,
    model: *const u8,
    model_len: usize,
    model_name: *const c_char,
    options: *const c_char,
    output: *mut *mut c_void,
) -> *mut c_char {
    boundary(|| {
        if output.is_null() {
            return Err(Error::invalid("null output pointer"));
        }
        unsafe {
            *output = std::ptr::null_mut();
        }
        if endpoint.is_null()
            || contract_path.is_null()
            || model.is_null()
            || model_name.is_null()
            || options.is_null()
            || model_len > 16 * 1024 * 1024
        {
            return Err(Error::invalid("null open argument or oversized descriptor"));
        }
        let text = |ptr| unsafe { CStr::from_ptr(ptr) }.to_str().map_err(Error::invalid);
        let contract = ArrowcolContract::load(text(contract_path)?)?;
        let pool =
            DescriptorPool::decode(unsafe { std::slice::from_raw_parts(model, model_len) }).map_err(Error::invalid)?;
        let descriptor = pool
            .get_message_by_name(text(model_name)?)
            .ok_or_else(|| Error::invalid("compiled message not found in descriptor set"))?;
        contract.validate_model(&descriptor)?;
        let options: StreamOptions = serde_json::from_str(text(options)?).map_err(Error::invalid)?;
        let max_rows = options.batch_rows;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .map_err(Error::invalid)?;
        let stream = runtime.block_on(ArrowcolStream::connect_dynamic(
            text(endpoint)?,
            contract.clone(),
            &descriptor,
            options,
        ))?;
        let handle = Box::new(Handle(Box::new(Reflected {
            stream,
            contract,
            runtime,
            max_rows,
        })));
        unsafe {
            *output = Box::into_raw(handle).cast();
        }
        Ok(())
    })
}

/// Compiles one CREATE TABLE statement into the canonical contract files the catalog derives
/// from the same DDL: `manifest.json`, `schema.arrow` and `descriptor.pb`, written to `output`.
///
/// # Safety
/// `ddl` and `output` must be NUL-terminated UTF-8 strings.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn arrowcol_compile_contract(ddl: *const c_char, output: *const c_char) -> *mut c_char {
    boundary(|| {
        if ddl.is_null() || output.is_null() {
            return Err(Error::invalid("null compile argument"));
        }
        let text = |ptr| unsafe { CStr::from_ptr(ptr) }.to_str().map_err(Error::invalid);
        let DdlCommand::CreateTable(create) = DdlParser::new("UTC").parse(text(ddl)?).map_err(Error::invalid)? else {
            return Err(Error::invalid(
                "contract compilation requires one CREATE TABLE statement",
            ));
        };
        let schema = CatalogSchemaVersion::new(
            deterministic_table_id(create.identity()),
            create.identity().clone(),
            SchemaVersion::FIRST,
            create.fields().to_vec(),
            create.canonical_ddl(),
        )
        .and_then(|schema| schema.with_metadata(create.metadata().clone()))
        .map_err(Error::invalid)?;
        let artifacts = generate_descriptor_artifacts(&schema).map_err(Error::invalid)?;
        let output = Path::new(text(output)?);
        std::fs::create_dir_all(output).map_err(Error::invalid)?;
        for (name, bytes) in [
            ("manifest.json", artifacts.manifest()),
            ("schema.arrow", artifacts.arrow_schema()),
            ("descriptor.pb", artifacts.protobuf_descriptor()),
        ] {
            std::fs::write(output.join(name), bytes).map_err(Error::invalid)?;
        }
        ArrowcolContract::load(output).map(|_| ())
    })
}

/// # Safety
/// Same pointer/lifecycle requirements as [`push`]. `batch` must be zero or one.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn arrowcol_push(handle: *mut c_void, data: *const u8, len: usize, batch: u8) -> *mut c_char {
    if batch > 1 {
        return boundary(|| Err(Error::invalid("invalid batch flag")));
    }
    unsafe { push(handle, data, len, batch != 0) }
}
/// # Safety
/// Same requirements as [`flush`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn arrowcol_flush(handle: *mut c_void) -> *mut c_char {
    unsafe { flush(handle) }
}
/// # Safety
/// Same requirements as [`close`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn arrowcol_close(handle: *mut c_void) -> *mut c_char {
    unsafe { close(handle) }
}
/// # Safety
/// Same requirements as [`destroy`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn arrowcol_destroy(handle: *mut c_void) {
    unsafe { destroy(handle) }
}
/// # Safety
/// Same requirements as [`free_error`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn arrowcol_free_error(error: *mut c_char) {
    unsafe { free_error(error) }
}
