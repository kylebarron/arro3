use arro3_internal::array::PyArray;
use arro3_internal::error::PyArrowResult;
use pyo3::prelude::*;

#[pyfunction]
pub fn take(values: PyArray, indices: PyArray) -> PyArrowResult<PyObject> {
    let (values_arr, values_field) = values.into_inner();
    let (indices, _) = indices.into_inner();
    let result = arrow_select::take::take(&values_arr, &indices, None)?;
    Python::with_gil(|py| PyArray::new(result, values_field).to_python(py))
}
