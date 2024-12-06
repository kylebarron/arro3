use arrow_array::cast::AsArray;
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
            Ok(PyArray::new(flat_array, flat_field).to_arro3(py)?.unbind())
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
                    .to_arro3(py)?
                    .unbind(),
            )
        }
    }
}

fn flatten_array(array: ArrayRef) -> Result<ArrayRef, ArrowError> {
    let offset = array.offset();
    let length = array.len();
    match array.data_type() {
        DataType::List(_) => {
            let arr = array.as_list::<i32>();
            let start = arr.offsets().get(offset).unwrap();
            let end = arr.offsets().get(offset + length).unwrap();
            Ok(arr
                .values()
                .slice(*start as usize, (*end - *start) as usize)
                .clone())
        }
        DataType::LargeList(_) => {
            let arr = array.as_list::<i64>();
            let start = arr.offsets().get(offset).unwrap();
            let end = arr.offsets().get(offset + length).unwrap();
            Ok(arr
                .values()
                .slice(*start as usize, (*end - *start) as usize)
                .clone())
        }
        DataType::FixedSizeList(_, list_size) => {
            let arr = array.as_fixed_size_list();
            Ok(arr.values().clone().slice(
                offset * (*list_size as usize),
                (offset + length) * (*list_size as usize),
            ))
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
