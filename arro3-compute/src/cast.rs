use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3_arrow::error::PyArrowResult;
use pyo3_arrow::ffi::ArrayIterator;
use pyo3_arrow::input::AnyArray;
use pyo3_arrow::{PyArray, PyArrayReader, PyDataType};

/// Cast `input` to the provided data type and return a new Arrow object with type `to_type`, if
/// possible.
///
/// Args:
///     input: an Arrow Array, RecordBatch, ChunkedArray, Table, ArrayReader, or RecordBatchReader
///     to_type: an Arrow DataType, Field, or Schema describing the output type of the cast.
#[pyfunction]
pub fn cast(py: Python, input: AnyArray, to_type: PyDataType) -> PyArrowResult<PyObject> {
    match input {
        AnyArray::Array(arr) => {
            let (arr, field) = arr.into_inner();
            let out = arrow_cast::cast(&arr, to_type.as_ref())?;
            let new_field = field.as_ref().clone().with_data_type(to_type.into_inner());
            Ok(PyArray::new(out, new_field.into()).to_arro3(py)?)
        }
        AnyArray::Stream(stream) => {
            let reader = stream.into_reader()?;
            let field = reader.field();
            let from_type = field.data_type();
            let to_type = to_type.into_inner();

            if !arrow_cast::can_cast_types(from_type, &to_type) {
                return Err(PyTypeError::new_err(format!(
                    "Unable to cast from type {from_type} to {to_type}"
                ))
                .into());
            }

            let out_field = field.as_ref().clone().with_data_type(to_type.clone());
            let iter = reader.into_iter().map(move |array| {
                let casted = arrow_cast::cast(&array?, &to_type)?;
                Ok(casted)
            });
            Ok(
                PyArrayReader::new(Box::new(ArrayIterator::new(iter, out_field.into())))
                    .to_arro3(py)?,
            )
        }
    }
}
