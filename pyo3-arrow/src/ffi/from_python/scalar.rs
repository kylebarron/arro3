use crate::array::*;
use crate::PyScalar;
use pyo3::prelude::*;
use pyo3::PyAny;

impl<'a> FromPyObject<'_, 'a> for PyScalar {
    type Error = PyErr;

    fn extract(obj: Borrowed<'_, 'a, PyAny>) -> Result<Self, Self::Error> {
        let array = obj.extract::<PyArray>()?;
        let (array, field) = array.into_inner();
        Ok(Self::try_new(array, field)?)
    }
}
