use pyo3::exceptions::PyRuntimeWarning;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::PyTuple;

mod aggregate;
mod arith;
mod boolean;
mod cast;
mod concat;
mod dictionary;
mod filter;
mod take;
mod temporal;

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
            "arro3.compute has not been compiled in release mode. Performance will be degraded.",
        );
        let args = PyTuple::new(py, vec![warning])?;
        warnings_mod.call_method1(intern!(py, "warn"), args)?;
    }

    Ok(())
}

#[pymodule(gil_used = false)]
fn _compute(py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    check_debug_build(py)?;

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
    m.add_wrapped(wrap_pyfunction!(dictionary::dictionary_encode))?;
    m.add_wrapped(wrap_pyfunction!(filter::filter))?;
    m.add_wrapped(wrap_pyfunction!(take::take))?;
    m.add_wrapped(wrap_pyfunction!(temporal::date_part))?;

    Ok(())
}
