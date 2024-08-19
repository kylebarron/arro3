use pyo3::prelude::*;

mod accessors;
mod constructors;

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[pyfunction]
fn ___version() -> &'static str {
    VERSION
}

/// A Python module implemented in Rust.
#[pymodule]
fn _core(_py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    m.add_wrapped(wrap_pyfunction!(___version))?;

    m.add_class::<pyo3_arrow::PyArray>()?;
    m.add_class::<pyo3_arrow::PyArrayReader>()?;
    m.add_class::<pyo3_arrow::PyChunkedArray>()?;
    m.add_class::<pyo3_arrow::PyDataType>()?;
    m.add_class::<pyo3_arrow::PyField>()?;
    m.add_class::<pyo3_arrow::PyRecordBatch>()?;
    m.add_class::<pyo3_arrow::PyRecordBatchReader>()?;
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
