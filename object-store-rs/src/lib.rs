use pyo3::prelude::*;

mod copy;
mod delete;
mod get;
mod head;
mod list;
mod put;
mod rename;
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

    m.add_wrapped(wrap_pyfunction!(copy::copy_async))?;
    m.add_wrapped(wrap_pyfunction!(copy::copy_if_not_exists_async))?;
    m.add_wrapped(wrap_pyfunction!(copy::copy_if_not_exists))?;
    m.add_wrapped(wrap_pyfunction!(copy::copy))?;
    m.add_wrapped(wrap_pyfunction!(delete::delete_async))?;
    m.add_wrapped(wrap_pyfunction!(delete::delete))?;
    m.add_wrapped(wrap_pyfunction!(get::get_async))?;
    m.add_wrapped(wrap_pyfunction!(get::get_range_async))?;
    m.add_wrapped(wrap_pyfunction!(get::get_range))?;
    m.add_wrapped(wrap_pyfunction!(get::get_ranges_async))?;
    m.add_wrapped(wrap_pyfunction!(get::get_ranges))?;
    m.add_wrapped(wrap_pyfunction!(get::get))?;
    m.add_wrapped(wrap_pyfunction!(head::head_async))?;
    m.add_wrapped(wrap_pyfunction!(head::head))?;
    m.add_wrapped(wrap_pyfunction!(list::list_async))?;
    m.add_wrapped(wrap_pyfunction!(list::list_with_delimiter_async))?;
    m.add_wrapped(wrap_pyfunction!(list::list_with_delimiter))?;
    m.add_wrapped(wrap_pyfunction!(list::list))?;
    m.add_wrapped(wrap_pyfunction!(put::put_file_async))?;
    m.add_wrapped(wrap_pyfunction!(put::put_file))?;
    m.add_wrapped(wrap_pyfunction!(rename::rename_async))?;
    m.add_wrapped(wrap_pyfunction!(rename::rename_if_not_exists_async))?;
    m.add_wrapped(wrap_pyfunction!(rename::rename_if_not_exists))?;
    m.add_wrapped(wrap_pyfunction!(rename::rename))?;
    m.add_wrapped(wrap_pyfunction!(signer::sign_url_async))?;
    m.add_wrapped(wrap_pyfunction!(signer::sign_url))?;

    Ok(())
}
