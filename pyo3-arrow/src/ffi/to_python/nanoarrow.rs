use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyTuple};

pub fn to_nanoarrow_schema(py: Python, capsule: &Bound<'_, PyCapsule>) -> PyResult<PyObject> {
    let na_mod = py.import_bound(intern!(py, "nanoarrow"))?;
    let pyarrow_obj = na_mod
        .getattr(intern!(py, "Schema"))?
        .call1(PyTuple::new_bound(py, vec![capsule]))?;
    Ok(pyarrow_obj.to_object(py))
}

pub fn to_nanoarrow_array(py: Python, capsules: &Bound<'_, PyTuple>) -> PyResult<PyObject> {
    let na_mod = py.import_bound(intern!(py, "nanoarrow"))?;
    let pyarrow_obj = na_mod.getattr(intern!(py, "Array"))?.call1(capsules)?;
    Ok(pyarrow_obj.to_object(py))
}

pub fn to_nanoarrow_array_stream(py: Python, capsule: &Bound<'_, PyCapsule>) -> PyResult<PyObject> {
    let na_mod = py.import_bound(intern!(py, "nanoarrow"))?;
    let pyarrow_obj = na_mod
        .getattr(intern!(py, "ArrayStream"))?
        .call1(PyTuple::new_bound(py, vec![capsule]))?;
    Ok(pyarrow_obj.to_object(py))
}
