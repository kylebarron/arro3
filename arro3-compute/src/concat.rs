use pyo3::prelude::*;
use pyo3_arrow::error::PyArrowResult;
use pyo3_arrow::{PyArray, PyChunkedArray};

#[pyfunction]
pub fn concat(input: PyChunkedArray) -> PyArrowResult<PyObject> {
    let (chunks, field) = input.into_inner();
    let array_refs = chunks.iter().map(|arr| arr.as_ref()).collect::<Vec<_>>();
    let concatted = arrow_select::concat::concat(array_refs.as_slice())?;
    Python::with_gil(|py| PyArray::new(concatted, field).to_arro3(py))
}
