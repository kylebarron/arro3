use std::ffi::CString;
use std::sync::Arc;

use arrow_array::Array;
use arrow_schema::FieldRef;
use pyo3::prelude::*;
use pyo3::types::PyCapsule;

use crate::ffi::to_python::chunked::ArrayIterator;
use crate::ffi::to_python::ffi_stream::new_stream;

// Note: we include the field so that we can round-trip extension types, which would otherwise lose
// their metadata.
#[pyclass(module = "arro3.core._rust", name = "ChunkedArray", subclass)]
pub struct PyChunkedArray {
    chunks: Vec<Arc<dyn Array>>,
    field: FieldRef,
}

impl PyChunkedArray {
    pub fn new(chunks: Vec<Arc<dyn Array>>, field: FieldRef) -> Self {
        Self { chunks, field }
    }
}

#[pymethods]
impl PyChunkedArray {
    /// An implementation of the [Arrow PyCapsule
    /// Interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html).
    /// This dunder method should not be called directly, but enables zero-copy
    /// data transfer to other Python libraries that understand Arrow memory.
    ///
    /// For example (as of the upcoming pyarrow v16), you can call
    /// [`pyarrow.chunked_array()`][pyarrow.chunked_array] to convert this array into a
    /// pyarrow array, without copying memory.
    #[allow(unused_variables)]
    fn __arrow_c_stream__(&self, requested_schema: Option<PyObject>) -> PyResult<PyObject> {
        let field = self.field.clone();
        let chunks = self.chunks.clone();

        let array_reader = Box::new(ArrayIterator::new(chunks.into_iter().map(Ok), field));
        let ffi_stream = new_stream(array_reader);
        let stream_capsule_name = CString::new("arrow_array_stream").unwrap();

        Python::with_gil(|py| {
            let stream_capsule = PyCapsule::new(py, ffi_stream, Some(stream_capsule_name))?;
            Ok(stream_capsule.to_object(py))
        })
    }
}
