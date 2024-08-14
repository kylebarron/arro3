use std::ffi::CString;
use std::sync::Arc;

use arrow::compute::kernels::cast;
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow_array::Array;
use arrow_schema::{ArrowError, DataType, Field, FieldRef};
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyTuple};

use crate::error::PyArrowResult;
use crate::ffi::from_python::utils::import_schema_pycapsule;
use crate::ffi::to_python::ffi_stream::new_stream;
use crate::ffi::{ArrayIterator, ArrayReader};

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
    requested_schema: Option<Bound<'py, PyCapsule>>,
) -> PyArrowResult<Bound<'py, PyTuple>> {
    // Cast array if requested
    let (array_data, field) = if let Some(capsule) = requested_schema {
        let schema_ptr = import_schema_pycapsule(&capsule)?;

        // Note: we don't import a Field directly because the name might not be set.
        // https://github.com/apache/arrow-rs/issues/6251
        let data_type = DataType::try_from(schema_ptr)?;
        let field = Arc::new(Field::new("", data_type, true));

        let casted_array = cast(array, field.data_type())?;
        (casted_array.to_data(), field)
    } else {
        (array.to_data(), field)
    };

    let ffi_schema = FFI_ArrowSchema::try_from(&field)?;
    let ffi_array = FFI_ArrowArray::new(&array_data);

    let schema_capsule_name = CString::new("arrow_schema").unwrap();
    let array_capsule_name = CString::new("arrow_array").unwrap();

    let schema_capsule = PyCapsule::new_bound(py, ffi_schema, Some(schema_capsule_name))?;
    let array_capsule = PyCapsule::new_bound(py, ffi_array, Some(array_capsule_name))?;
    let tuple = PyTuple::new_bound(py, vec![schema_capsule, array_capsule]);

    Ok(tuple)
}

/// Export an [`ArrayIterator`][crate::ffi::ArrayIterator] to a PyCapsule holding an Arrow C Stream
/// pointer.
pub fn to_stream_pycapsule<'py>(
    py: Python<'py>,
    mut array_reader: Box<dyn ArrayReader + Send>,
    requested_schema: Option<Bound<'py, PyCapsule>>,
) -> PyArrowResult<Bound<'py, PyCapsule>> {
    // Cast array if requested
    if let Some(capsule) = requested_schema {
        let schema_ptr = import_schema_pycapsule(&capsule)?;

        // Note: we don't import a Field directly because the name might not be set.
        // https://github.com/apache/arrow-rs/issues/6251
        let data_type = DataType::try_from(schema_ptr)?;
        let field = Arc::new(Field::new("", data_type, true));

        let output_field = field.clone();
        let array_iter = array_reader.map(move |array| {
            let out = cast(array?.as_ref(), field.data_type())?;
            Ok(out)
        });
        array_reader = Box::new(ArrayIterator::new(array_iter, output_field));
    }

    let ffi_stream = new_stream(array_reader);
    let stream_capsule_name = CString::new("arrow_array_stream").unwrap();
    Ok(PyCapsule::new_bound(
        py,
        ffi_stream,
        Some(stream_capsule_name),
    )?)
}
