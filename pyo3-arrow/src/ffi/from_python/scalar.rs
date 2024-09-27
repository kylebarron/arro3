use crate::array::*;
use crate::PyScalar;
use pyo3::prelude::*;
use pyo3::{PyAny, PyResult};

impl<'a> FromPyObject<'a> for PyScalar {
    fn extract_bound(ob: &Bound<'a, PyAny>) -> PyResult<Self> {
        let array = ob.extract::<PyArray>()?;
        let (array, field) = array.into_inner();
        Self::try_new(array, field).map_err(|err| err.into())
    }
}
