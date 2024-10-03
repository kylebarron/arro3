use crate::array::*;
#[cfg(feature = "buffer_protocol")]
use crate::buffer::AnyBufferProtocol;
use crate::ffi::from_python::utils::call_arrow_c_array;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::{PyAny, PyResult};

impl<'a> FromPyObject<'a> for PyArray {
    fn extract_bound(ob: &Bound<'a, PyAny>) -> PyResult<Self> {
        if ob.hasattr("__arrow_c_array__")? {
            let (schema_capsule, array_capsule) = call_arrow_c_array(ob)?;
            Self::from_arrow_pycapsule(&schema_capsule, &array_capsule)
        } else {
            #[cfg(feature = "buffer_protocol")]
            if let Ok(buf) = ob.extract::<AnyBufferProtocol>() {
                return Ok(buf.try_into()?);
            }

            Err(PyValueError::new_err(
                "Expected object with __arrow_c_array__ method or implementing buffer protocol.",
            ))
        }
    }
}
