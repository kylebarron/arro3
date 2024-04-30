use std::ffi::CString;

use arrow::ffi::FFI_ArrowSchema;
use arrow_schema::SchemaRef;
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyType};

use crate::error::PyArrowResult;

#[pyclass(module = "arro3.core._rust", name = "Schema", subclass)]
pub struct PySchema(SchemaRef);

impl PySchema {
    pub fn new(schema: SchemaRef) -> Self {
        Self(schema)
    }
}

impl From<PySchema> for SchemaRef {
    fn from(value: PySchema) -> Self {
        value.0
    }
}

impl From<SchemaRef> for PySchema {
    fn from(value: SchemaRef) -> Self {
        Self(value)
    }
}

#[pymethods]
impl PySchema {
    /// An implementation of the [Arrow PyCapsule
    /// Interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html).
    /// This dunder method should not be called directly, but enables zero-copy
    /// data transfer to other Python libraries that understand Arrow memory.
    ///
    /// For example, you can call [`pyarrow.schema()`][pyarrow.schema] to convert this array
    /// into a pyarrow schema, without copying memory.
    fn __arrow_c_schema__(&self) -> PyArrowResult<PyObject> {
        let ffi_schema = FFI_ArrowSchema::try_from(self.0.as_ref())?;
        let schema_capsule_name = CString::new("arrow_schema").unwrap();

        Python::with_gil(|py| {
            let schema_capsule = PyCapsule::new(py, ffi_schema, Some(schema_capsule_name))?;
            Ok(schema_capsule.to_object(py))
        })
    }

    /// Construct this object from existing Arrow data
    ///
    /// Args:
    ///     input: Arrow array to use for constructing this object
    ///
    /// Returns:
    ///     Self
    #[classmethod]
    pub fn from_arrow(_cls: &PyType, input: &PyAny) -> PyResult<Self> {
        input.extract()
    }

    pub fn __eq__(&self, other: &PySchema) -> bool {
        self.0 == other.0
    }
}
