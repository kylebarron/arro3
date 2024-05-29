use std::ffi::CString;

use arrow::ffi::FFI_ArrowSchema;
use pyo3::types::PyCapsule;
use pyo3::Python;

use crate::error::PyArrowResult;
use crate::field::PyField;
use crate::schema::PySchema;

pub trait ToSchemaPyCapsule {
    fn to_py_capsule<'py>(&'py self, py: Python<'py>) -> PyArrowResult<&'py PyCapsule>;
}

impl ToSchemaPyCapsule for PySchema {
    fn to_py_capsule<'py>(&'py self, py: Python<'py>) -> PyArrowResult<&'py PyCapsule> {
        let ffi_schema = FFI_ArrowSchema::try_from(self.as_ref())?;
        let schema_capsule_name = CString::new("arrow_schema").unwrap();
        let schema_capsule = PyCapsule::new(py, ffi_schema, Some(schema_capsule_name))?;
        Ok(schema_capsule)
    }
}

impl ToSchemaPyCapsule for PyField {
    fn to_py_capsule<'py>(&'py self, py: Python<'py>) -> PyArrowResult<&'py PyCapsule> {
        let ffi_schema = FFI_ArrowSchema::try_from(self.as_ref())?;
        let schema_capsule_name = CString::new("arrow_schema").unwrap();
        let schema_capsule = PyCapsule::new(py, ffi_schema, Some(schema_capsule_name))?;
        Ok(schema_capsule)
    }
}
