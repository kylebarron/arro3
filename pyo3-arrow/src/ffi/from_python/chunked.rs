use crate::chunked::PyChunkedArray;
use crate::ffi::from_python::utils::call_arrow_c_stream;
use pyo3::prelude::*;
use pyo3::{PyAny, PyResult};

impl<'a> FromPyObject<'a> for PyChunkedArray {
    fn extract_bound(ob: &Bound<'a, PyAny>) -> PyResult<Self> {
        let capsule = call_arrow_c_stream(ob)?;
        Self::from_arrow_pycapsule(&capsule)
    }
}
