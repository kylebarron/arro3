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

    m.add_class::<arro3_internal::array::PyArray>()?;
    m.add_class::<arro3_internal::chunked::PyChunkedArray>()?;
    m.add_class::<arro3_internal::field::PyField>()?;
    m.add_class::<arro3_internal::record_batch::PyRecordBatch>()?;
    m.add_class::<arro3_internal::record_batch_reader::PyRecordBatchReader>()?;
    m.add_class::<arro3_internal::schema::PySchema>()?;
    m.add_class::<arro3_internal::table::PyTable>()?;

    Ok(())
}
