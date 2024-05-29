use arro3_internal::array::PyArray;
use arro3_internal::chunked::PyChunkedArray;
use arro3_internal::error::PyArrowResult;
use pyo3::prelude::*;

#[pyfunction]
pub fn concat(input: PyChunkedArray) -> PyArrowResult<PyArray> {
    let (chunks, field) = input.into_inner();
    let array_refs = chunks.iter().map(|arr| arr.as_ref()).collect::<Vec<_>>();
    let concatted = arrow_select::concat::concat(array_refs.as_slice())?;
    Ok(PyArray::new(concatted, field))
}
