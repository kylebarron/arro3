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

    m.add_class::<pyo3_arrow::array::PyArray>()?;
    m.add_class::<pyo3_arrow::chunked::PyChunkedArray>()?;
    m.add_class::<pyo3_arrow::field::PyField>()?;
    m.add_class::<pyo3_arrow::record_batch::PyRecordBatch>()?;
    m.add_class::<pyo3_arrow::record_batch_reader::PyRecordBatchReader>()?;
    m.add_class::<pyo3_arrow::schema::PySchema>()?;
    m.add_class::<pyo3_arrow::table::PyTable>()?;

    Ok(())
}
