use std::sync::Arc;

use crate::array::*;
use crate::ffi::from_python::utils::import_arrow_c_array;
use pyo3::prelude::*;
use pyo3::{PyAny, PyResult};

impl<'a> FromPyObject<'a> for PyArray {
    fn extract(ob: &'a PyAny) -> PyResult<Self> {
        let (array, field) = import_arrow_c_array(ob)?;
        Ok(PyArray::new(array, Arc::new(field)))
    }
}
