use std::sync::Arc;

use arrow::array::ArrayData;
use arrow::datatypes::Field;
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow::ffi_stream::FFI_ArrowArrayStream;
use arrow_array::{make_array, ArrayRef, StructArray};
use arrow_schema::DataType;
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyTuple};
use pyo3::{PyAny, PyResult};

/// Validate PyCapsule has provided name
pub fn validate_pycapsule_name(capsule: &Bound<PyCapsule>, expected_name: &str) -> PyResult<()> {
    let capsule_name = capsule.name()?;
    if let Some(capsule_name) = capsule_name {
        let capsule_name = capsule_name.to_str()?;
        if capsule_name != expected_name {
            return Err(PyValueError::new_err(format!(
                "Expected name '{}' in PyCapsule, instead got '{}'",
                expected_name, capsule_name
            )));
        }
    } else {
        return Err(PyValueError::new_err(
            "Expected schema PyCapsule to have name set.",
        ));
    }

    Ok(())
}

/// Import `__arrow_c_schema__` across Python boundary
pub(crate) fn call_arrow_c_schema<'py>(ob: &'py Bound<PyAny>) -> PyResult<Bound<'py, PyCapsule>> {
    if !ob.hasattr("__arrow_c_schema__")? {
        return Err(PyValueError::new_err(
            "Expected an object with dunder __arrow_c_schema__",
        ));
    }

    Ok(ob.getattr("__arrow_c_schema__")?.call0()?.downcast_into()?)
}

pub(crate) fn import_schema_pycapsule<'py>(
    capsule: &'py Bound<PyCapsule>,
) -> PyResult<&'py FFI_ArrowSchema> {
    validate_pycapsule_name(capsule, "arrow_schema")?;

    let schema_ptr = unsafe { capsule.reference::<FFI_ArrowSchema>() };
    Ok(schema_ptr)
}

/// Import `__arrow_c_array__` across Python boundary
pub(crate) fn call_arrow_c_array<'py>(
    ob: &'py Bound<PyAny>,
) -> PyResult<(Bound<'py, PyCapsule>, Bound<'py, PyCapsule>)> {
    if !ob.hasattr("__arrow_c_array__")? {
        return Err(PyValueError::new_err(
            "Expected an object with dunder __arrow_c_array__",
        ));
    }

    let tuple = ob.getattr("__arrow_c_array__")?.call0()?;
    if !tuple.is_instance_of::<PyTuple>() {
        return Err(PyTypeError::new_err(
            "Expected __arrow_c_array__ to return a tuple.",
        ));
    }

    let schema_capsule = tuple.get_item(0)?.downcast_into()?;
    let array_capsule = tuple.get_item(1)?.downcast_into()?;
    Ok((schema_capsule, array_capsule))
}

/// A custom implementation of ArrayData -> StructArray to work around the upstream bug:
/// https://github.com/apache/arrow-rs/issues/6151
fn make_struct(data: ArrayData) -> StructArray {
    let arrays: Vec<ArrayRef> = data
        .child_data()
        .iter()
        .map(|cd| our_make_array(cd.clone()))
        .collect();
    let data_type = data.data_type().clone();
    let nulls = data.nulls().cloned();

    let struct_fields = match data_type {
        DataType::Struct(struct_fields) => struct_fields,
        _ => panic!(),
    };

    let mut struct_array = StructArray::new(struct_fields, arrays, nulls);

    // We _always_ need to slice the incoming struct array, not just when the offset is positive,
    // because the array length is otherwise also lost.
    // https://github.com/apache/arrow-rs/issues/6151#issuecomment-2256749153
    struct_array = struct_array.slice(data.offset(), data.len());

    struct_array
}

fn our_make_array(data: ArrayData) -> ArrayRef {
    match data.data_type() {
        DataType::Struct(_) => Arc::new(make_struct(data)),
        _ => make_array(data),
    }
}

pub(crate) fn import_array_pycapsules(
    schema_capsule: &Bound<PyCapsule>,
    array_capsule: &Bound<PyCapsule>,
) -> PyResult<(ArrayRef, Field)> {
    validate_pycapsule_name(schema_capsule, "arrow_schema")?;
    validate_pycapsule_name(array_capsule, "arrow_array")?;

    let schema_ptr = unsafe { schema_capsule.reference::<FFI_ArrowSchema>() };
    let array = unsafe { FFI_ArrowArray::from_raw(array_capsule.pointer() as _) };

    let array_data = unsafe { arrow::ffi::from_ffi(array, schema_ptr) }
        .map_err(|err| PyTypeError::new_err(err.to_string()))?;
    let field = Field::try_from(schema_ptr).map_err(|err| PyTypeError::new_err(err.to_string()))?;
    Ok((our_make_array(array_data), field))
}

/// Import `__arrow_c_stream__` across Python boundary.
pub(crate) fn call_arrow_c_stream<'py>(ob: &'py Bound<PyAny>) -> PyResult<Bound<'py, PyCapsule>> {
    if !ob.hasattr("__arrow_c_stream__")? {
        return Err(PyValueError::new_err(
            "Expected an object with dunder __arrow_c_stream__",
        ));
    }

    let capsule = ob.getattr("__arrow_c_stream__")?.call0()?.downcast_into()?;
    Ok(capsule)
}

pub(crate) fn import_stream_pycapsule(
    capsule: &Bound<PyCapsule>,
) -> PyResult<FFI_ArrowArrayStream> {
    validate_pycapsule_name(capsule, "arrow_array_stream")?;

    let stream = unsafe { FFI_ArrowArrayStream::from_raw(capsule.pointer() as _) };
    Ok(stream)
}
