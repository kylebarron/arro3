use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyTuple};

pub fn to_nanoarrow_schema<'py>(
    py: Python<'py>,
    capsule: &Bound<'py, PyCapsule>,
) -> PyResult<Bound<'py, PyAny>> {
    let na_mod = py.import(intern!(py, "nanoarrow"))?;
    na_mod
        .getattr(intern!(py, "Schema"))?
        .call1(PyTuple::new(py, vec![capsule])?)
}

pub fn to_nanoarrow_array<'py>(
    py: Python<'py>,
    capsules: &Bound<'py, PyTuple>,
) -> PyResult<Bound<'py, PyAny>> {
    let na_mod = py.import(intern!(py, "nanoarrow"))?;
    na_mod.getattr(intern!(py, "Array"))?.call1(capsules)
}

pub fn to_nanoarrow_array_stream<'py>(
    py: Python<'py>,
    capsule: &Bound<'py, PyCapsule>,
) -> PyResult<Bound<'py, PyAny>> {
    let na_mod = py.import(intern!(py, "nanoarrow"))?;
    na_mod
        .getattr(intern!(py, "ArrayStream"))?
        .call1(PyTuple::new(py, vec![capsule])?)
}
