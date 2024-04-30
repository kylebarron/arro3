use std::ffi::CString;

use arrow::ffi_stream::FFI_ArrowArrayStream;
use arrow_array::{RecordBatch, RecordBatchIterator};
use arrow_schema::SchemaRef;
use pyo3::prelude::*;
use pyo3::types::PyCapsule;

use crate::error::PyArrowResult;

#[pyclass(module = "arro3.core._rust", name = "Table", subclass)]
#[derive(Debug)]
pub struct PyTable {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
}

impl PyTable {
    pub fn new(schema: SchemaRef, batches: Vec<RecordBatch>) -> Self {
        Self { schema, batches }
    }
}

#[pymethods]
impl PyTable {
    /// An implementation of the [Arrow PyCapsule
    /// Interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html).
    /// This dunder method should not be called directly, but enables zero-copy
    /// data transfer to other Python libraries that understand Arrow memory.
    ///
    /// For example, you can call [`pyarrow.table()`][pyarrow.table] to convert this array
    /// into a pyarrow table, without copying memory.
    #[allow(unused_variables)]
    fn __arrow_c_stream__(&self, requested_schema: Option<PyObject>) -> PyArrowResult<PyObject> {
        let batches = self.batches.clone();

        let record_batch_reader = Box::new(RecordBatchIterator::new(
            batches.into_iter().map(Ok),
            self.schema.clone(),
        ));
        let ffi_stream = FFI_ArrowArrayStream::new(record_batch_reader);

        let stream_capsule_name = CString::new("arrow_array_stream").unwrap();

        Python::with_gil(|py| {
            let stream_capsule = PyCapsule::new(py, ffi_stream, Some(stream_capsule_name))?;
            Ok(stream_capsule.to_object(py))
        })
    }
}
