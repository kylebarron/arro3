use crate::ffi::from_python::utils::call_arrow_c_schema;
use crate::PyDataType;
use pyo3::prelude::*;
use pyo3::PyAny;

impl<'py> FromPyObject<'_, 'py> for PyDataType {
    type Error = PyErr;

    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
        let capsule = call_arrow_c_schema(&obj)?;
        Self::from_arrow_pycapsule(&capsule)
    }
}
