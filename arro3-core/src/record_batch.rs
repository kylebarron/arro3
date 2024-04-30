use std::ffi::CString;

use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow_array::{Array, RecordBatch, StructArray};
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyTuple};

use crate::error::PyArrowResult;

#[pyclass(module = "arro3.core._rust", name = "RecordBatch", subclass)]
#[derive(Debug)]
pub struct PyRecordBatch(RecordBatch);

impl PyRecordBatch {
    pub fn new(batch: RecordBatch) -> Self {
        Self(batch)
    }
}

impl From<RecordBatch> for PyRecordBatch {
    fn from(value: RecordBatch) -> Self {
        Self(value)
    }
}

impl From<PyRecordBatch> for RecordBatch {
    fn from(value: PyRecordBatch) -> Self {
        value.0
    }
}

#[pymethods]
impl PyRecordBatch {
    /// An implementation of the [Arrow PyCapsule
    /// Interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html).
    /// This dunder method should not be called directly, but enables zero-copy
    /// data transfer to other Python libraries that understand Arrow memory.
    ///
    /// For example, you can call [`pyarrow.array()`][pyarrow.array] to convert this array
    /// into a pyarrow array, without copying memory.
    #[allow(unused_variables)]
    pub fn __arrow_c_array__(&self, requested_schema: Option<PyObject>) -> PyArrowResult<PyObject> {
        let schema = self.0.schema();
        let array = StructArray::from(self.0.clone());

        let ffi_schema = FFI_ArrowSchema::try_from(schema.as_ref())?;
        let ffi_array = FFI_ArrowArray::new(&array.to_data());

        let schema_capsule_name = CString::new("arrow_schema").unwrap();
        let array_capsule_name = CString::new("arrow_array").unwrap();

        Python::with_gil(|py| {
            let schema_capsule = PyCapsule::new(py, ffi_schema, Some(schema_capsule_name))?;
            let array_capsule = PyCapsule::new(py, ffi_array, Some(array_capsule_name))?;
            let tuple = PyTuple::new(py, vec![schema_capsule, array_capsule]);
            Ok(tuple.to_object(py))
        })
    }

    pub fn __eq__(&self, other: &PyRecordBatch) -> bool {
        self.0 == other.0
    }
}
