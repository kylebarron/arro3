use std::ffi::CString;

use arrow::ffi::FFI_ArrowSchema;
use arrow_schema::FieldRef;
use pyo3::prelude::*;
use pyo3::types::PyCapsule;

use crate::error::PyArrowResult;

#[pyclass(module = "arro3.core._rust", name = "Field", subclass)]
pub struct PyField(FieldRef);

impl PyField {
    pub fn new(field: FieldRef) -> Self {
        Self(field)
    }
}

impl From<PyField> for FieldRef {
    fn from(value: PyField) -> Self {
        value.0
    }
}

impl From<FieldRef> for PyField {
    fn from(value: FieldRef) -> Self {
        Self(value)
    }
}

#[pymethods]
impl PyField {
    /// An implementation of the [Arrow PyCapsule
    /// Interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html).
    /// This dunder method should not be called directly, but enables zero-copy
    /// data transfer to other Python libraries that understand Arrow memory.
    ///
    /// For example, you can call [`pyarrow.field()`][pyarrow.field] to convert this array
    /// into a pyarrow field, without copying memory.
    fn __arrow_c_schema__(&self) -> PyArrowResult<PyObject> {
        let ffi_schema = FFI_ArrowSchema::try_from(self.0.as_ref())?;
        let schema_capsule_name = CString::new("arrow_schema").unwrap();

        Python::with_gil(|py| {
            let schema_capsule = PyCapsule::new(py, ffi_schema, Some(schema_capsule_name))?;
            Ok(schema_capsule.to_object(py))
        })
    }
}
