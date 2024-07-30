use pyo3::prelude::*;

mod csv;
mod ipc;
mod json;
mod parquet;
mod utils;

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[pyfunction]
fn ___version() -> &'static str {
    VERSION
}

#[pymodule]
fn _io(_py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    m.add_wrapped(wrap_pyfunction!(___version))?;

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
    m.add_wrapped(wrap_pyfunction!(parquet::write_parquet))?;

    Ok(())
}
