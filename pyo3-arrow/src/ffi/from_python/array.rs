use crate::array::*;
#[cfg(feature = "buffer_protocol")]
use crate::buffer::AnyBufferProtocol;
use crate::ffi::from_python::utils::call_arrow_c_array;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::{intern, PyAny};

impl<'py> FromPyObject<'_, 'py> for PyArray {
    type Error = PyErr;

    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
        if obj.hasattr(intern!(obj.py(), "__arrow_c_array__"))? {
            let (schema_capsule, array_capsule) = call_arrow_c_array(&obj)?;
            Self::from_arrow_pycapsule(&schema_capsule, &array_capsule)
        } else {
            #[cfg(feature = "buffer_protocol")]
            if let Ok(buf) = obj.extract::<AnyBufferProtocol>() {
                return Ok(buf.try_into()?);
            }

            Err(PyValueError::new_err(
                "Expected object with __arrow_c_array__ method or implementing buffer protocol.",
            ))
        }
    }
}
