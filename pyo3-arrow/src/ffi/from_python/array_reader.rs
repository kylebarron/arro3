use crate::array_reader::PyArrayReader;
use crate::ffi::from_python::utils::call_arrow_c_stream;
use pyo3::prelude::*;
use pyo3::PyAny;

impl<'py> FromPyObject<'_, 'py> for PyArrayReader {
    type Error = PyErr;

    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
        let capsule = call_arrow_c_stream(&obj)?;
        Self::from_arrow_pycapsule(&capsule)
    }
}
