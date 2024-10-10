use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{ArrayRef, Int32Array, Int64Array, OffsetSizeTrait};
use arrow_buffer::{OffsetBuffer, ScalarBuffer};
use arrow_schema::{ArrowError, DataType, Field};
use pyo3::prelude::*;
use pyo3_arrow::error::PyArrowResult;
use pyo3_arrow::ffi::ArrayIterator;
use pyo3_arrow::input::AnyArray;
use pyo3_arrow::{PyArray, PyArrayReader};

#[pyfunction]
#[pyo3(signature = (input, *, logical=true))]
pub fn list_offsets(py: Python, input: AnyArray, logical: bool) -> PyArrowResult<PyObject> {
    match input {
        AnyArray::Array(array) => {
            let (array, _field) = array.into_inner();
            let offsets = _list_offsets(array, logical)?;
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

            let iter = reader
                .into_iter()
                .map(move |array| _list_offsets(array?, logical));
            Ok(
                PyArrayReader::new(Box::new(ArrayIterator::new(iter, out_field.into())))
                    .to_arro3(py)?,
            )
        }
    }
}

fn _list_offsets(array: ArrayRef, logical: bool) -> Result<ArrayRef, ArrowError> {
    let offset = array.offset();
    let length = array.len();

    match array.data_type() {
        DataType::List(_) => {
            let arr = array.as_list::<i32>();
            let offsets = arr.offsets();
            let offsets = if logical {
                slice_offsets(offsets, offset, length)
            } else {
                offsets.clone().into_inner()
            };
            Ok(Arc::new(Int32Array::new(offsets, None)))
        }
        DataType::LargeList(_) => {
            let arr = array.as_list::<i64>();
            let offsets = arr.offsets();
            let offsets = if logical {
                slice_offsets(offsets, offset, length)
            } else {
                offsets.clone().into_inner()
            };
            Ok(Arc::new(Int64Array::new(offsets, None)))
        }
        _ => Err(ArrowError::SchemaError(
            "Expected list-typed Array".to_string(),
        )),
    }
}

fn slice_offsets<O: OffsetSizeTrait>(
    offsets: &OffsetBuffer<O>,
    offset: usize,
    length: usize,
) -> ScalarBuffer<O> {
    let sliced = offsets.slice(offset, length);
    let first_offset = sliced.first().copied().unwrap_or(O::zero());
    if first_offset.to_usize().unwrap() == 0 {
        sliced.into_inner()
    } else {
        let mut new_offsets = Vec::with_capacity(sliced.len());
        for value in sliced.iter() {
            new_offsets.push(*value - first_offset);
        }
        ScalarBuffer::from(new_offsets)
    }
}
