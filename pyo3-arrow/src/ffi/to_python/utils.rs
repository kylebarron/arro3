use std::ffi::CString;

use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow_array::Array;
use arrow_schema::{ArrowError, FieldRef};
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyTuple};

use crate::error::PyArrowResult;
use crate::ffi::to_python::ffi_stream::new_stream;
use crate::ffi::ArrayReader;

/// Export a [`arrow_schema::Schema`], [`arrow_schema::Field`], or [`arrow_schema::DataType`] to a
/// PyCapsule holding an Arrow C Schema pointer.
pub fn to_schema_pycapsule(
    py: Python,
    field: impl TryInto<FFI_ArrowSchema, Error = ArrowError>,
) -> PyArrowResult<Bound<PyCapsule>> {
    let ffi_schema: FFI_ArrowSchema = field.try_into()?;
    let schema_capsule_name = CString::new("arrow_schema").unwrap();
    let schema_capsule = PyCapsule::new_bound(py, ffi_schema, Some(schema_capsule_name))?;
    Ok(schema_capsule)
}

/// Export an [`Array`] and [`FieldRef`] to a tuple of PyCapsules holding an Arrow C Schema and
/// Arrow C Array pointers.
pub fn to_array_pycapsules<'py>(
    py: Python<'py>,
    field: FieldRef,
    array: &dyn Array,
    _requested_schema: Option<Bound<PyCapsule>>,
) -> PyArrowResult<Bound<'py, PyTuple>> {
    let ffi_schema = FFI_ArrowSchema::try_from(&field)?;
    let ffi_array = FFI_ArrowArray::new(&array.to_data());

    let schema_capsule_name = CString::new("arrow_schema").unwrap();
    let array_capsule_name = CString::new("arrow_array").unwrap();

    let schema_capsule = PyCapsule::new_bound(py, ffi_schema, Some(schema_capsule_name))?;
    let array_capsule = PyCapsule::new_bound(py, ffi_array, Some(array_capsule_name))?;
    let tuple = PyTuple::new_bound(py, vec![schema_capsule, array_capsule]);

    Ok(tuple)
}

/// Export an [`ArrayIterator`] to a PyCapsule holding an Arrow C Stream pointer.
pub fn to_stream_pycapsule<'py>(
    py: Python<'py>,
    array_reader: Box<dyn ArrayReader + Send>,
    _requested_schema: Option<Bound<PyCapsule>>,
) -> PyResult<Bound<'py, PyCapsule>> {
    let ffi_stream = new_stream(array_reader);
    let stream_capsule_name = CString::new("arrow_array_stream").unwrap();
    PyCapsule::new_bound(py, ffi_stream, Some(stream_capsule_name))
}
