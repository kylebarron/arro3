use crate::ffi::from_python::utils::call_arrow_c_schema;
use crate::PyDataType;
use pyo3::prelude::*;
use pyo3::{PyAny, PyResult};

impl<'a> FromPyObject<'a> for PyDataType {
    fn extract_bound(ob: &Bound<'a, PyAny>) -> PyResult<Self> {
        let capsule = call_arrow_c_schema(ob)?;
        Self::from_arrow_pycapsule(&capsule)
    }
}
