use crate::ffi::from_python::utils::call_arrow_c_stream;
use crate::table::PyTable;
use pyo3::prelude::*;
use pyo3::PyAny;

impl<'a> FromPyObject<'_, 'a> for PyTable {
    type Error = PyErr;

    fn extract(obj: Borrowed<'_, 'a, PyAny>) -> Result<Self, Self::Error> {
        let capsule = call_arrow_c_stream(&obj)?;
        Self::from_arrow_pycapsule(&capsule)
    }
}
