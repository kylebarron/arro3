use pyo3::prelude::*;

mod api;
mod delete;
mod get;
mod list;
mod runtime;
mod signer;

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[pyfunction]
fn ___version() -> &'static str {
    VERSION
}

/// A Python module implemented in Rust.
#[pymodule]
fn _object_store_rs(py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    m.add_wrapped(wrap_pyfunction!(___version))?;

    pyo3_object_store::register_store_module(py, m, "object_store_rs")?;

    m.add_wrapped(wrap_pyfunction!(delete::delete_async))?;
    m.add_wrapped(wrap_pyfunction!(delete::delete))?;
    m.add_wrapped(wrap_pyfunction!(list::list_async))?;
    m.add_wrapped(wrap_pyfunction!(list::list))?;
    m.add_wrapped(wrap_pyfunction!(signer::sign_url_async))?;
    m.add_wrapped(wrap_pyfunction!(signer::sign_url))?;
    Ok(())
}
