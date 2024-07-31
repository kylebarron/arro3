use std::sync::Arc;

use arrow::array::AsArray;
use arrow_array::{ArrayRef, Int32Array, Int64Array};
use arrow_schema::{ArrowError, DataType, Field};
use pyo3::prelude::*;
use pyo3_arrow::error::PyArrowResult;
use pyo3_arrow::ffi::ArrayIterator;
use pyo3_arrow::input::AnyArray;
use pyo3_arrow::{PyArray, PyArrayReader};

#[pyfunction]
#[pyo3(signature = (input, *, physical=true))]
pub fn list_offsets(py: Python, input: AnyArray, physical: bool) -> PyArrowResult<PyObject> {
    match input {
        AnyArray::Array(array) => {
            let (array, _field) = array.into_inner();
            let offsets = _list_offsets(array, physical)?;
            Ok(PyArray::from_array_ref(offsets).to_arro3(py)?)
        }
        AnyArray::Stream(stream) => {
            let reader = stream.into_reader()?;
            let out_field = match reader.field().data_type() {
                DataType::List(_) => Field::new("", DataType::Int32, false),
                DataType::LargeList(_) => Field::new("", DataType::Int64, false),
                _ => {
                    return Err(
                        ArrowError::SchemaError("Expected list-typed Array".to_string()).into(),
                    );
                }
            };

            let iter = reader.into_iter().map(move |array| {
                let out = _list_offsets(array?, physical)?;
                Ok(out)
            });
            Ok(
                PyArrayReader::new(Box::new(ArrayIterator::new(iter, out_field.into())))
                    .to_arro3(py)?,
            )
        }
    }
}

fn _list_offsets(array: ArrayRef, physical: bool) -> Result<ArrayRef, ArrowError> {
    if !physical {
        return Err(ArrowError::ComputeError(
            "Logical list offset slicing not yet implemented".to_string(),
        ));
    }

    match array.data_type() {
        DataType::List(_) => {
            let arr = array.as_list::<i32>();
            let offsets = arr.offsets();
            Ok(Arc::new(Int32Array::from(offsets.to_vec())))
        }
        DataType::LargeList(_) => {
            let arr = array.as_list::<i64>();
            let offsets = arr.offsets();
            Ok(Arc::new(Int64Array::from(offsets.to_vec())))
        }
        _ => Err(ArrowError::SchemaError(
            "Expected list-typed Array".to_string(),
        )),
    }
}
