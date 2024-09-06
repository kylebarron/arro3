use crate::array::*;
use crate::ffi::from_python::utils::call_arrow_c_array;
use pyo3::prelude::*;
use pyo3::{PyAny, PyResult};

impl<'a> FromPyObject<'a> for PyArray {
    fn extract_bound(ob: &Bound<'a, PyAny>) -> PyResult<Self> {
        let (schema_capsule, array_capsule) = call_arrow_c_array(ob)?;
        Self::from_arrow_pycapsule(&schema_capsule, &array_capsule)
    }
}
