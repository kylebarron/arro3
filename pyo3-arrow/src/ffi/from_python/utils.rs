use std::ffi::CStr;

use arrow_array::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow_array::ffi_stream::FFI_ArrowArrayStream;
use arrow_array::{make_array, ArrayRef};
use arrow_schema::Field;
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyTuple};
use pyo3::{intern, PyAny, PyResult};

const ARROW_SCHEMA_CAPSULE_NAME: &CStr = c"arrow_schema";
const ARROW_ARRAY_CAPSULE_NAME: &CStr = c"arrow_array";
const ARROW_ARRAY_STREAM_CAPSULE_NAME: &CStr = c"arrow_array_stream";

/// Import `__arrow_c_schema__` across Python boundary
pub(crate) fn call_arrow_c_schema<'py>(ob: &'py Bound<PyAny>) -> PyResult<Bound<'py, PyCapsule>> {
    let py_str = intern!(ob.py(), "__arrow_c_schema__");
    if !ob.hasattr(py_str)? {
        return Err(PyValueError::new_err(
            "Expected an object with dunder __arrow_c_schema__",
        ));
    }

    Ok(ob.getattr(py_str)?.call0()?.cast_into()?)
}

pub(crate) fn import_schema_pycapsule<'py>(
    capsule: &'py Bound<PyCapsule>,
) -> PyResult<&'py FFI_ArrowSchema> {
    let schema_ptr = capsule
        .pointer_checked(Some(ARROW_SCHEMA_CAPSULE_NAME))?
        .cast();
    Ok(unsafe { schema_ptr.as_ref() })
}

fn import_array_pycapsule(capsule: &'_ Bound<PyCapsule>) -> PyResult<FFI_ArrowArray> {
    let array_ptr = capsule
        .pointer_checked(Some(ARROW_ARRAY_CAPSULE_NAME))?
        .cast::<FFI_ArrowArray>();
    Ok(unsafe { FFI_ArrowArray::from_raw(array_ptr.as_ptr()) })
}

pub(crate) fn import_stream_pycapsule(
    capsule: &Bound<PyCapsule>,
) -> PyResult<FFI_ArrowArrayStream> {
    let stream_ptr = capsule
        .pointer_checked(Some(ARROW_ARRAY_STREAM_CAPSULE_NAME))?
        .cast::<FFI_ArrowArrayStream>();
    Ok(unsafe { FFI_ArrowArrayStream::from_raw(stream_ptr.as_ptr()) })
}

/// Import `__arrow_c_array__` across Python boundary
pub(crate) fn call_arrow_c_array<'py>(
    ob: &'py Bound<PyAny>,
) -> PyResult<(Bound<'py, PyCapsule>, Bound<'py, PyCapsule>)> {
    let py_str = intern!(ob.py(), "__arrow_c_array__");
    if !ob.hasattr(py_str)? {
        return Err(PyValueError::new_err(
            "Expected an object with dunder __arrow_c_array__",
        ));
    }

    let tuple = ob.getattr(py_str)?.call0()?;
    if !tuple.is_instance_of::<PyTuple>() {
        return Err(PyTypeError::new_err(
            "Expected __arrow_c_array__ to return a tuple.",
        ));
    }

    let schema_capsule = tuple.get_item(0)?.cast_into()?;
    let array_capsule = tuple.get_item(1)?.cast_into()?;
    Ok((schema_capsule, array_capsule))
}

pub(crate) fn import_array_pycapsules(
    schema_capsule: &Bound<PyCapsule>,
    array_capsule: &Bound<PyCapsule>,
) -> PyResult<(ArrayRef, Field)> {
    let schema_ptr = import_schema_pycapsule(schema_capsule)?;
    let array_ptr = import_array_pycapsule(array_capsule)?;

    let array_data = unsafe { arrow_array::ffi::from_ffi(array_ptr, schema_ptr) }
        .map_err(|err| PyTypeError::new_err(err.to_string()))?;
    let field = Field::try_from(schema_ptr).map_err(|err| PyTypeError::new_err(err.to_string()))?;
    let array = make_array(array_data);
    Ok((array, field))
}

/// Import `__arrow_c_stream__` across Python boundary.
pub(crate) fn call_arrow_c_stream<'py>(ob: &'py Bound<PyAny>) -> PyResult<Bound<'py, PyCapsule>> {
    let py_str = intern!(ob.py(), "__arrow_c_stream__");
    if !ob.hasattr(py_str)? {
        return Err(PyValueError::new_err(
            "Expected an object with dunder __arrow_c_stream__",
        ));
    }

    let capsule = ob.getattr(py_str)?.call0()?.cast_into()?;
    Ok(capsule)
}
