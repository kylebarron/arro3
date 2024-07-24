use pyo3::prelude::*;

mod csv;
mod utils;

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[pyfunction]
fn ___version() -> &'static str {
    VERSION
}

#[pymodule]
fn _rust(_py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    m.add_wrapped(wrap_pyfunction!(___version))?;

    m.add_wrapped(wrap_pyfunction!(csv::infer_csv_schema))?;
    m.add_wrapped(wrap_pyfunction!(csv::read_csv))?;
    m.add_wrapped(wrap_pyfunction!(csv::write_csv))?;

    Ok(())
}
