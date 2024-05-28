use pyo3::prelude::*;

pub mod array;
pub mod chunked;
pub mod error;
pub mod ffi;
pub mod field;
pub mod interop;
pub mod record_batch;
pub mod record_batch_reader;
pub mod schema;
pub mod table;

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[pyfunction]
fn ___version() -> &'static str {
    VERSION
}

/// A Python module implemented in Rust.
#[pymodule]
fn _rust(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_wrapped(wrap_pyfunction!(___version))?;

    m.add_class::<array::PyArray>()?;
    m.add_class::<chunked::PyChunkedArray>()?;
    m.add_class::<field::PyField>()?;
    m.add_class::<record_batch::PyRecordBatch>()?;
    m.add_class::<record_batch_reader::PyRecordBatchReader>()?;
    m.add_class::<schema::PySchema>()?;
    m.add_class::<table::PyTable>()?;

    Ok(())
}
