use arro3_internal::array::PyArray;
use arro3_internal::chunked::PyChunkedArray;
use arro3_internal::error::PyArrowResult;
use pyo3::intern;
use pyo3::prelude::*;

#[pyfunction]
pub fn concat(input: PyChunkedArray) -> PyArrowResult<PyObject> {
    let (chunks, field) = input.into_inner();
    let array_refs = chunks.iter().map(|arr| arr.as_ref()).collect::<Vec<_>>();
    let concatted = arrow_select::concat::concat(array_refs.as_slice())?;
    let py_array = PyArray::new(concatted, field);

    Python::with_gil(|py| {
        let arro3_mod = py.import(intern!(py, "arro3.core"))?;
        let core_obj = arro3_mod.getattr(intern!(py, "Array"))?.call_method1(
            intern!(py, "from_arrow_pycapsule"),
            py_array.__arrow_c_array__(py, None)?,
        )?;
        Ok(core_obj.to_object(py))
    })
}
