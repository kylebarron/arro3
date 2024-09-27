use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3_arrow::error::PyArrowResult;
use pyo3_arrow::ffi::ArrayIterator;
use pyo3_arrow::input::AnyArray;
use pyo3_arrow::{PyArray, PyArrayReader, PyField};

/// Cast `input` to the provided data type and return a new Arrow object with type `to_type`, if
/// possible.
///
/// Args:
///     input: an Arrow Array, RecordBatch, ChunkedArray, Table, ArrayReader, or RecordBatchReader
///     to_type: an Arrow DataType, Field, or Schema describing the output type of the cast.
#[pyfunction]
pub fn cast(py: Python, input: AnyArray, to_type: PyField) -> PyArrowResult<PyObject> {
    match input {
        AnyArray::Array(arr) => {
            let new_field = to_type.into_inner();
            let out = arrow_cast::cast(arr.as_ref(), new_field.data_type())?;
            Ok(PyArray::new(out, new_field).to_arro3(py)?)
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
            Ok(PyArrayReader::new(Box::new(ArrayIterator::new(iter, new_field))).to_arro3(py)?)
        }
    }
}

#[pyfunction]
pub fn can_cast_types(from_type: PyDataType, to_type: PyDataType) -> bool {
    arrow_cast::can_cast_types(from_type.as_ref(), to_type.as_ref())
}
