use pyo3::prelude::*;

mod boolean;
mod cast;
mod concat;
mod filter;
mod list_flatten;
mod list_offsets;
mod struct_field;
mod take;

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[pyfunction]
fn ___version() -> &'static str {
    VERSION
}

#[pymodule]
fn _compute(_py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    m.add_wrapped(wrap_pyfunction!(___version))?;

    m.add_wrapped(wrap_pyfunction!(boolean::is_not_null))?;
    m.add_wrapped(wrap_pyfunction!(boolean::is_null))?;
    m.add_wrapped(wrap_pyfunction!(cast::can_cast_types))?;
    m.add_wrapped(wrap_pyfunction!(cast::cast))?;
    m.add_wrapped(wrap_pyfunction!(concat::concat))?;
    m.add_wrapped(wrap_pyfunction!(filter::filter))?;
    m.add_wrapped(wrap_pyfunction!(list_flatten::list_flatten))?;
    m.add_wrapped(wrap_pyfunction!(list_offsets::list_offsets))?;
    m.add_wrapped(wrap_pyfunction!(struct_field::struct_field))?;
    m.add_wrapped(wrap_pyfunction!(take::take))?;

    Ok(())
}
