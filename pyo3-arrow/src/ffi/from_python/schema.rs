use crate::ffi::from_python::utils::call_arrow_c_schema;
use crate::schema::PySchema;
use pyo3::prelude::*;
use pyo3::{PyAny, PyResult};

impl<'a> FromPyObject<'a> for PySchema {
    fn extract_bound(ob: &Bound<'a, PyAny>) -> PyResult<Self> {
        let schema_ptr = call_arrow_c_schema(ob)?;
        Self::from_arrow_pycapsule(&schema_ptr)
    }
}
