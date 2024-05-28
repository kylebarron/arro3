use std::ffi::CString;

use arrow::ffi_stream::FFI_ArrowArrayStream;
use arrow_array::RecordBatchReader;
use pyo3::exceptions::PyIOError;
use pyo3::prelude::*;
use pyo3::types::PyCapsule;

use crate::error::PyArrowResult;

/// A wrapper around an [arrow_array::RecordBatchReader]
#[pyclass(module = "arro3.core._rust", name = "RecordBatchReader", subclass)]
pub struct PyRecordBatchReader(pub(crate) Option<Box<dyn RecordBatchReader + Send>>);

impl PyRecordBatchReader {
    pub fn into_reader(mut self) -> PyArrowResult<Box<dyn RecordBatchReader + Send>> {
        let stream = self
            .0
            .take()
            .ok_or(PyIOError::new_err("Cannot write from closed stream."))?;
        Ok(stream)
    }
}

#[pymethods]
impl PyRecordBatchReader {
    /// An implementation of the [Arrow PyCapsule
    /// Interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html).
    /// This dunder method should not be called directly, but enables zero-copy
    /// data transfer to other Python libraries that understand Arrow memory.
    ///
    /// For example, you can call [`pyarrow.table()`][pyarrow.table] to convert this array
    /// into a pyarrow table, without copying memory.
    fn __arrow_c_stream__(
        &mut self,
        _requested_schema: Option<PyObject>,
    ) -> PyArrowResult<PyObject> {
        let reader = self
            .0
            .take()
            .ok_or(PyIOError::new_err("Cannot read from closed stream"))?;

        let ffi_stream = FFI_ArrowArrayStream::new(reader);
        let stream_capsule_name = CString::new("arrow_array_stream").unwrap();

        Python::with_gil(|py| {
            let stream_capsule = PyCapsule::new(py, ffi_stream, Some(stream_capsule_name))?;
            Ok(stream_capsule.to_object(py))
        })
    }
}
