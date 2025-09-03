use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::IntoPyObjectExt;
use pyo3_arrow::error::PyArrowResult;
use pyo3_arrow::export::{Arro3Array, Arro3ArrayReader};
use pyo3_arrow::ffi::ArrayIterator;
use pyo3_arrow::input::AnyArray;
use pyo3_arrow::{PyArrayReader, PyDataType, PyField};

/// Cast `input` to the provided data type and return a new Arrow object with type `to_type`, if
/// possible.
///
/// Args:
///     input: an Arrow Array, RecordBatch, ChunkedArray, Table, ArrayReader, or RecordBatchReader
///     to_type: an Arrow DataType, Field, or Schema describing the output type of the cast.
#[pyfunction]
pub fn cast<'py>(
    py: Python<'py>,
    input: AnyArray,
    to_type: PyField,
) -> PyArrowResult<Bound<'py, PyAny>> {
    match input {
        AnyArray::Array(arr) => {
            let new_field = to_type.into_inner();
            let out = arrow_cast::cast(arr.as_ref(), new_field.data_type())?;
            Ok(Arro3Array::from(out).into_bound_py_any(py)?)
        }
        AnyArray::Stream(stream) => {
            let reader = stream.into_reader()?;
            let field = reader.field();
            let from_type = field.data_type();

            let new_field = to_type.into_inner();
            let to_type = new_field.data_type().clone();
            if !arrow_cast::can_cast_types(from_type, &to_type) {
                return Err(PyTypeError::new_err(format!(
                    "Unable to cast from type {from_type} to {to_type}"
                ))
                .into());
            }

            let iter = reader
                .into_iter()
                .map(move |array| arrow_cast::cast(&array?, &to_type));
            Ok(
                Arro3ArrayReader::from(PyArrayReader::new(Box::new(ArrayIterator::new(
                    iter, new_field,
                ))))
                .into_bound_py_any(py)?,
            )
        }
    }
}

#[pyfunction]
pub fn can_cast_types(from_type: PyDataType, to_type: PyDataType) -> bool {
    arrow_cast::can_cast_types(from_type.as_ref(), to_type.as_ref())
}
