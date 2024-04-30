use pyo3::prelude::*;

pub mod array;
pub mod chunked;
pub mod error;
pub mod ffi;
pub mod field;
pub mod record_batch;
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
    m.add_class::<schema::PySchema>()?;
    m.add_class::<table::PyTable>()?;

    // Top-level array/chunked array functions
    // m.add_function(wrap_pyfunction!(
    //     crate::algorithm::geo::affine_ops::affine_transform,
    //     m
    // )?)?;

    Ok(())
}
