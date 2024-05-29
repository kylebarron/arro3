use crate::array::*;
use crate::ffi::from_python::utils::call_arrow_c_array;
use pyo3::prelude::*;
use pyo3::{PyAny, PyResult};

impl<'a> FromPyObject<'a> for PyArray {
    fn extract(ob: &'a PyAny) -> PyResult<Self> {
        let (schema_capsule, array_capsule) = call_arrow_c_array(ob)?;
        Python::with_gil(|py| {
            Self::from_arrow_pycapsule(py.get_type::<PyArray>(), schema_capsule, array_capsule)
        })
    }
}
