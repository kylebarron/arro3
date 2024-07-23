use pyo3::prelude::*;

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[pyfunction]
fn ___version() -> &'static str {
    VERSION
}

/// A Python module implemented in Rust.
#[pymodule]
fn _rust(_py: Python, m: &Bound<PyModule>) -> PyResult<()> {
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

    Ok(())
}
