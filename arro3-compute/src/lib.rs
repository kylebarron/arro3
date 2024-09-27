use pyo3::prelude::*;

mod aggregate;
mod arith;
mod boolean;
mod cast;
mod concat;
mod dictionary;
mod filter;
mod take;

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[pyfunction]
fn ___version() -> &'static str {
    VERSION
}

#[pymodule]
fn _compute(_py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    m.add_wrapped(wrap_pyfunction!(___version))?;

    m.add_wrapped(wrap_pyfunction!(aggregate::max))?;
    m.add_wrapped(wrap_pyfunction!(aggregate::min))?;
    m.add_wrapped(wrap_pyfunction!(aggregate::sum))?;
    m.add_wrapped(wrap_pyfunction!(arith::add_wrapping))?;
    m.add_wrapped(wrap_pyfunction!(arith::add))?;
    m.add_wrapped(wrap_pyfunction!(arith::div))?;
    m.add_wrapped(wrap_pyfunction!(arith::mul_wrapping))?;
    m.add_wrapped(wrap_pyfunction!(arith::mul))?;
    m.add_wrapped(wrap_pyfunction!(arith::neg_wrapping))?;
    m.add_wrapped(wrap_pyfunction!(arith::neg))?;
    m.add_wrapped(wrap_pyfunction!(arith::rem))?;
    m.add_wrapped(wrap_pyfunction!(arith::sub_wrapping))?;
    m.add_wrapped(wrap_pyfunction!(arith::sub))?;
    m.add_wrapped(wrap_pyfunction!(boolean::is_not_null))?;
    m.add_wrapped(wrap_pyfunction!(boolean::is_null))?;
    m.add_wrapped(wrap_pyfunction!(cast::can_cast_types))?;
    m.add_wrapped(wrap_pyfunction!(cast::cast))?;
    m.add_wrapped(wrap_pyfunction!(cast::cast))?;
    m.add_wrapped(wrap_pyfunction!(concat::concat))?;
    m.add_wrapped(wrap_pyfunction!(concat::concat))?;
    m.add_wrapped(wrap_pyfunction!(dictionary::dictionary_dictionary))?;
    m.add_wrapped(wrap_pyfunction!(dictionary::dictionary_encode))?;
    m.add_wrapped(wrap_pyfunction!(dictionary::dictionary_indices))?;
    m.add_wrapped(wrap_pyfunction!(filter::filter))?;
    m.add_wrapped(wrap_pyfunction!(take::take))?;

    Ok(())
}
