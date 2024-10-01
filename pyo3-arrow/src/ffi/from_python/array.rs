use crate::array::*;
use crate::ffi::from_python::utils::call_arrow_c_array;
use crate::input::AnyBufferProtocol;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::{PyAny, PyResult};

impl<'a> FromPyObject<'a> for PyArray {
    fn extract_bound(ob: &Bound<'a, PyAny>) -> PyResult<Self> {
        if ob.hasattr("__arrow_c_array__")? {
            let (schema_capsule, array_capsule) = call_arrow_c_array(ob)?;
            Self::from_arrow_pycapsule(&schema_capsule, &array_capsule)
        } else if let Ok(buf) = ob.extract::<AnyBufferProtocol>() {
            Ok(buf.try_into()?)
        } else {
            Err(PyValueError::new_err(
                "Expected object with __arrow_c_array__ method or implementing buffer protocol.",
            ))
        }
    }
}
