use pyo3::prelude::*;
use pyo3_arrow::error::PyArrowResult;
use pyo3_arrow::PyArray;

/// Take elements by index from an Array, creating a new Array from those
/// indexes.
#[pyfunction]
pub fn take(py: Python, values: PyArray, indices: PyArray) -> PyArrowResult<PyObject> {
    let output_array =
        py.allow_threads(|| arrow_select::take::take(values.as_ref(), indices.as_ref(), None))?;
    Ok(PyArray::new(output_array, values.field().clone()).to_arro3(py)?)
}
