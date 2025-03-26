use pyo3::exceptions::PyRuntimeWarning;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::PyTuple;

mod csv;
mod error;
mod ipc;
mod json;
mod parquet;
mod utils;

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
            "arro3.io has not been compiled in release mode. Performance will be degraded.",
        );
        let args = PyTuple::new(py, vec![warning])?;
        warnings_mod.call_method1(intern!(py, "warn"), args)?;
    }

    Ok(())
}

#[pymodule]
fn _io(py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    check_debug_build(py)?;

    m.add_wrapped(wrap_pyfunction!(___version))?;

    pyo3_object_store::register_store_module(py, m, "arro3.io", "store")?;
    pyo3_object_store::register_exceptions_module(py, m, "arro3.io", "exceptions")?;

    m.add_wrapped(wrap_pyfunction!(csv::infer_csv_schema))?;
    m.add_wrapped(wrap_pyfunction!(csv::read_csv))?;
    m.add_wrapped(wrap_pyfunction!(csv::write_csv))?;

    m.add_wrapped(wrap_pyfunction!(json::infer_json_schema))?;
    m.add_wrapped(wrap_pyfunction!(json::read_json))?;
    m.add_wrapped(wrap_pyfunction!(json::write_json))?;
    m.add_wrapped(wrap_pyfunction!(json::write_ndjson))?;

    m.add_wrapped(wrap_pyfunction!(ipc::read_ipc))?;
    m.add_wrapped(wrap_pyfunction!(ipc::read_ipc_stream))?;
    m.add_wrapped(wrap_pyfunction!(ipc::write_ipc))?;
    m.add_wrapped(wrap_pyfunction!(ipc::write_ipc_stream))?;

    m.add_wrapped(wrap_pyfunction!(parquet::read_parquet))?;
    m.add_wrapped(wrap_pyfunction!(parquet::read_parquet_async))?;
    m.add_wrapped(wrap_pyfunction!(parquet::write_parquet))?;

    Ok(())
}
