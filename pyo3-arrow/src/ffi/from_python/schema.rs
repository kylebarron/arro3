use crate::ffi::from_python::utils::call_arrow_c_schema;
use crate::schema::PySchema;
use pyo3::prelude::*;
use pyo3::PyAny;

impl<'a> FromPyObject<'_, 'a> for PySchema {
    type Error = PyErr;

    fn extract(obj: Borrowed<'_, 'a, PyAny>) -> Result<Self, Self::Error> {
        let schema_ptr = call_arrow_c_schema(&obj)?;
        Self::from_arrow_pycapsule(&schema_ptr)
    }
}
