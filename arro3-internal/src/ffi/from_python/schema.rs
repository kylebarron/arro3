use crate::ffi::from_python::utils::call_arrow_c_schema;
use crate::schema::PySchema;
use pyo3::prelude::*;
use pyo3::{PyAny, PyResult};

impl<'a> FromPyObject<'a> for PySchema {
    fn extract_bound(ob: &Bound<'a, PyAny>) -> PyResult<Self> {
        let schema_ptr = call_arrow_c_schema(ob)?;
        Python::with_gil(|py| {
            Self::from_arrow_pycapsule(&py.get_type_bound::<PySchema>(), &schema_ptr)
        })
    }
}
