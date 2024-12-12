use pyo3::exceptions::PyRuntimeWarning;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::PyTuple;

mod accessors;
mod constructors;

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[pyfunction]
fn ___version() -> &'static str {
    VERSION
}

/// Raise RuntimeWarning for debug builds
#[pyfunction]
fn check_debug_build(py: Python) -> PyResult<()> {
    #[cfg(debug_assertions)]
    {
        let warnings_mod = py.import(intern!(py, "warnings"))?;
        let warning = PyRuntimeWarning::new_err(
            "arro3.core has not been compiled in release mode. Performance will be degraded.",
        );
        let args = PyTuple::new(py, vec![warning])?;
        warnings_mod.call_method1(intern!(py, "warn"), args)?;
    }

    Ok(())
}

/// A Python module implemented in Rust.
#[pymodule(gil_used = false)]
fn _core(py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    check_debug_build(py)?;

    m.add_wrapped(wrap_pyfunction!(___version))?;

    m.add_class::<pyo3_arrow::PyArray>()?;
    m.add_class::<pyo3_arrow::PyArrayReader>()?;
    m.add_class::<pyo3_arrow::buffer::PyArrowBuffer>()?;
    m.add_class::<pyo3_arrow::PyChunkedArray>()?;
    m.add_class::<pyo3_arrow::PyDataType>()?;
    m.add_class::<pyo3_arrow::PyField>()?;
    m.add_class::<pyo3_arrow::PyRecordBatch>()?;
    m.add_class::<pyo3_arrow::PyRecordBatchReader>()?;
    m.add_class::<pyo3_arrow::PyScalar>()?;
    m.add_class::<pyo3_arrow::PySchema>()?;
    m.add_class::<pyo3_arrow::PyTable>()?;

    m.add_wrapped(wrap_pyfunction!(
        accessors::dictionary::dictionary_dictionary
    ))?;
    m.add_wrapped(wrap_pyfunction!(accessors::dictionary::dictionary_indices))?;
    m.add_wrapped(wrap_pyfunction!(accessors::list_flatten::list_flatten))?;
    m.add_wrapped(wrap_pyfunction!(accessors::list_offsets::list_offsets))?;
    m.add_wrapped(wrap_pyfunction!(accessors::struct_field::struct_field))?;

    m.add_wrapped(wrap_pyfunction!(constructors::fixed_size_list_array))?;
    m.add_wrapped(wrap_pyfunction!(constructors::list_array))?;
    m.add_wrapped(wrap_pyfunction!(constructors::struct_array))?;

    Ok(())
}
