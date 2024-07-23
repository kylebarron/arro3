use pyo3::prelude::*;

mod cast;
mod concat;
mod take;

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[pyfunction]
fn ___version() -> &'static str {
    VERSION
}

#[pymodule]
fn _rust(_py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    m.add_wrapped(wrap_pyfunction!(___version))?;

    m.add_wrapped(wrap_pyfunction!(cast::cast))?;
    m.add_wrapped(wrap_pyfunction!(concat::concat))?;
    m.add_wrapped(wrap_pyfunction!(take::take))?;

    Ok(())
}
