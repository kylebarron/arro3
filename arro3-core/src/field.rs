use std::ffi::CString;
use std::sync::Arc;

use arrow::ffi::FFI_ArrowSchema;
use arrow_schema::{Field, FieldRef};
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyType};

use crate::error::PyArrowResult;
use crate::ffi::from_python::utils::import_schema_pycapsule;

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

impl AsRef<Field> for PyField {
    fn as_ref(&self) -> &Field {
        &self.0
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

    pub fn __eq__(&self, other: &PyField) -> bool {
        self.0 == other.0
    }

    #[classmethod]
    pub fn from_arrow(_cls: &PyType, input: &PyAny) -> PyResult<Self> {
        input.extract()
    }

    /// Construct this object from a bare Arrow PyCapsule
    #[classmethod]
    pub fn from_arrow_pycapsule(_cls: &PyType, capsule: &PyCapsule) -> PyResult<Self> {
        let schema_ptr = import_schema_pycapsule(capsule)?;
        let field =
            Field::try_from(schema_ptr).map_err(|err| PyTypeError::new_err(err.to_string()))?;
        Ok(Self::new(Arc::new(field)))
    }
}
