use arrow::array::AsArray;
use arrow_array::ArrayRef;
use arrow_schema::{ArrowError, DataType, FieldRef};
use pyo3::prelude::*;
use pyo3_arrow::error::PyArrowResult;
use pyo3_arrow::ffi::ArrayIterator;
use pyo3_arrow::input::AnyArray;
use pyo3_arrow::{PyArray, PyArrayReader};

#[pyfunction]
pub fn list_flatten(py: Python, input: AnyArray) -> PyArrowResult<PyObject> {
    match input {
        AnyArray::Array(array) => {
            let (array, field) = array.into_inner();
            let flat_array = flatten_array(array)?;
            let flat_field = flatten_field(field)?;
            Ok(PyArray::new(flat_array, flat_field).to_arro3(py)?)
        }
        AnyArray::Stream(stream) => {
            let reader = stream.into_reader()?;
            let flatten_field = flatten_field(reader.field())?;

            let iter = reader.into_iter().map(move |array| {
                let out = flatten_array(array?)?;
                Ok(out)
            });
            Ok(
                PyArrayReader::new(Box::new(ArrayIterator::new(iter, flatten_field)))
                    .to_arro3(py)?,
            )
        }
    }
}

fn flatten_array(array: ArrayRef) -> Result<ArrayRef, ArrowError> {
    match array.data_type() {
        DataType::List(_) => {
            let arr = array.as_list::<i32>();
            Ok(arr.values().clone())
        }
        DataType::LargeList(_) => {
            let arr = array.as_list::<i64>();
            Ok(arr.values().clone())
        }
        DataType::FixedSizeList(_, _) => {
            let arr = array.as_fixed_size_list();
            Ok(arr.values().clone())
        }
        _ => Err(ArrowError::SchemaError(
            "Expected list-typed Array".to_string(),
        )),
    }
}

fn flatten_field(field: FieldRef) -> Result<FieldRef, ArrowError> {
    match field.data_type() {
        DataType::List(inner_field)
        | DataType::LargeList(inner_field)
        | DataType::FixedSizeList(inner_field, _) => Ok(inner_field.clone()),
        _ => Err(ArrowError::SchemaError(
            "Expected list-typed Array".to_string(),
        )),
    }
}
