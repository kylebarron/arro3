//! A custom implementation of ffi_stream to export chunked arrays, not RecordBatches
//!
//! This is derived from
//! <https://github.com/apache/arrow-rs/blob/9d0abcc6f4e11594c23811c2c2d297f2eb2963af/arrow/src/ffi_stream.rs>

use std::mem::ManuallyDrop;
use std::ptr::addr_of;
use std::{
    convert::TryFrom,
    ffi::CString,
    os::raw::{c_char, c_int, c_void},
};

use arrow_array::ffi::*;
use arrow_array::ffi_stream::FFI_ArrowArrayStream;
use arrow_array::Array;
use arrow_schema::ArrowError;

use arrow_array::ArrayReader;

const ENOMEM: i32 = 12;
const EIO: i32 = 5;
const EINVAL: i32 = 22;
const ENOSYS: i32 = 78;

/// Create a new [`FFI_ArrowArrayStream`] from an [`ArrayReader`].
pub fn new_stream(array_reader: Box<dyn ArrayReader + Send>) -> FFI_ArrowArrayStream {
    let private_data = Box::new(ArrayStreamPrivateData {
        array_reader: ManuallyDrop::new(array_reader),
        last_error: None,
    });

    // SAFETY: the callbacks implement the C stream interface over `ArrayStreamPrivateData`, and
    // `release_stream` frees that private data exactly once.
    unsafe {
        FFI_ArrowArrayStream::new_unchecked(
            Some(get_schema),
            Some(get_next),
            Some(get_last_error),
            Some(release_stream),
            Box::into_raw(private_data) as *mut c_void,
        )
    }
}

// callback used to drop [FFI_ArrowArrayStream] when it is exported.
unsafe extern "C" fn release_stream(stream: *mut FFI_ArrowArrayStream) {
    if stream.is_null() {
        return;
    }
    let private_data = (*stream).private_data() as *mut ArrayStreamPrivateData;

    // Mark the stream as released by clearing all of its callbacks. `ptr::write` doesn't drop the
    // old value, so this doesn't call `release_stream` again.
    std::ptr::write(stream, FFI_ArrowArrayStream::empty());

    drop(Box::from_raw(private_data));
}

struct ArrayStreamPrivateData {
    array_reader: ManuallyDrop<Box<dyn ArrayReader + Send>>,
    last_error: Option<CString>,
}

impl Drop for ArrayStreamPrivateData {
    fn drop(&mut self) {
        // SAFETY: `array_reader` isn't used again after this.
        let array_reader = unsafe { ManuallyDrop::take(&mut self.array_reader) };

        // The reader can own Python objects, such as the iterator behind
        // `RecordBatchReader.from_batches`, so drop it with the GIL held. The consumer may release
        // the stream without the GIL: pyarrow releases it before destroying a reader.
        // `Python::attach` isn't enough here, because pyo3 tracks attachment per thread: if a pyo3
        // frame further up the stack attached before the consumer released the GIL, pyo3 still
        // considers this thread attached and would decref without the GIL.
        unsafe {
            if pyo3::ffi::Py_IsInitialized() != 0 {
                let gstate = pyo3::ffi::PyGILState_Ensure();
                drop(array_reader);
                pyo3::ffi::PyGILState_Release(gstate);
            } else {
                drop(array_reader);
            }
        }
    }
}

// The callback used to get array schema
unsafe extern "C" fn get_schema(
    stream: *mut FFI_ArrowArrayStream,
    schema: *mut FFI_ArrowSchema,
) -> c_int {
    ExportedArrayStream { stream }.get_schema(schema)
}

// The callback used to get next array
unsafe extern "C" fn get_next(
    stream: *mut FFI_ArrowArrayStream,
    array: *mut FFI_ArrowArray,
) -> c_int {
    ExportedArrayStream { stream }.get_next(array)
}

// The callback used to get the error from last operation on the `FFI_ArrowArrayStream`
unsafe extern "C" fn get_last_error(stream: *mut FFI_ArrowArrayStream) -> *const c_char {
    let mut ffi_stream = ExportedArrayStream { stream };
    // The consumer should not take ownership of this string, we should return
    // a const pointer to it.
    match ffi_stream.get_last_error() {
        Some(err_string) => err_string.as_ptr(),
        None => std::ptr::null(),
    }
}

struct ExportedArrayStream {
    stream: *mut FFI_ArrowArrayStream,
}

impl ExportedArrayStream {
    fn get_private_data(&mut self) -> &mut ArrayStreamPrivateData {
        unsafe { &mut *((*self.stream).private_data() as *mut ArrayStreamPrivateData) }
    }

    pub fn get_schema(&mut self, out: *mut FFI_ArrowSchema) -> i32 {
        let private_data = self.get_private_data();
        let reader = &private_data.array_reader;

        let schema = FFI_ArrowSchema::try_from(reader.field().as_ref());

        match schema {
            Ok(schema) => {
                unsafe { std::ptr::copy(addr_of!(schema), out, 1) };
                std::mem::forget(schema);
                0
            }
            Err(ref err) => {
                private_data.last_error = Some(
                    CString::new(err.to_string()).expect("Error string has a null byte in it."),
                );
                get_error_code(err)
            }
        }
    }

    pub fn get_next(&mut self, out: *mut FFI_ArrowArray) -> i32 {
        let private_data = self.get_private_data();
        let reader = &mut private_data.array_reader;

        match reader.next() {
            None => {
                // Marks ArrowArray released to indicate reaching the end of stream.
                unsafe { std::ptr::write(out, FFI_ArrowArray::empty()) }
                0
            }
            Some(next_array) => {
                if let Ok(array) = next_array {
                    let array = FFI_ArrowArray::new(&array.to_data());

                    unsafe { std::ptr::write_unaligned(out, array) };
                    0
                } else {
                    let err = &next_array.unwrap_err();
                    private_data.last_error = Some(
                        CString::new(err.to_string()).expect("Error string has a null byte in it."),
                    );
                    get_error_code(err)
                }
            }
        }
    }

    pub fn get_last_error(&mut self) -> Option<&CString> {
        self.get_private_data().last_error.as_ref()
    }
}

fn get_error_code(err: &ArrowError) -> i32 {
    match err {
        ArrowError::NotYetImplemented(_) => ENOSYS,
        ArrowError::MemoryError(_) => ENOMEM,
        ArrowError::IoError(_, _) => EIO,
        _ => EINVAL,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::ArrayRef;
    use arrow_schema::{ArrowError, DataType, Field};
    use pyo3::prelude::*;

    use super::new_stream;
    use crate::ffi::ArrayIterator;

    /// pyarrow releases the GIL before it destroys a reader, and so before it releases a stream it
    /// imported. If a pyo3 frame further up the stack is attached at that point, pyo3 still
    /// considers the thread attached. Releasing the stream must still drop the Python objects its
    /// reader owns with the GIL held.
    #[test]
    fn release_drops_python_objects_with_gil_held() {
        Python::initialize();
        Python::attach(|py| {
            let generator = py
                .eval(c"(i for i in range(3))", None, None)
                .unwrap()
                .unbind();
            let arrays: Vec<Result<ArrayRef, ArrowError>> = vec![];
            let owns_generator = arrays.into_iter().inspect(move |_| {
                let _ = &generator;
            });
            let field = Arc::new(Field::new("a", DataType::Int32, false));
            let stream = new_stream(Box::new(ArrayIterator::new(owns_generator, field)));

            // Release the stream the way pyarrow does: with the GIL released.
            unsafe {
                let tstate = pyo3::ffi::PyEval_SaveThread();
                drop(stream);
                pyo3::ffi::PyEval_RestoreThread(tstate);
            }
        });
    }
}
