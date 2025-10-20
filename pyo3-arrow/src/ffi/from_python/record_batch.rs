use crate::ffi::from_python::utils::call_arrow_c_array;
use crate::record_batch::PyRecordBatch;
use pyo3::prelude::*;
use pyo3::PyAny;

impl<'py> FromPyObject<'_, 'py> for PyRecordBatch {
    type Error = PyErr;

    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
        let (schema_capsule, array_capsule) = call_arrow_c_array(&obj)?;
        Self::from_arrow_pycapsule(&schema_capsule, &array_capsule)
    }
}
