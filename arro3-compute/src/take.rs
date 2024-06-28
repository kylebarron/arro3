use pyo3::prelude::*;
use pyo3_arrow::error::PyArrowResult;
use pyo3_arrow::PyArray;

#[pyfunction]
pub fn take(py: Python, values: PyArray, indices: PyArray) -> PyArrowResult<PyObject> {
    let (values_array, values_field) = values.into_inner();
    let (indices, _) = indices.into_inner();
    let output_array =
        py.allow_threads(|| arrow_select::take::take(&values_array, &indices, None))?;
    PyArray::new(output_array, values_field).to_arro3(py)
}
