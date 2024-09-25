use std::sync::Arc;

use arrow::array::{
    AsArray, BinaryViewBuilder, BooleanBuilder, GenericBinaryBuilder, GenericStringBuilder,
    PrimitiveBuilder, StringViewBuilder,
};
use arrow::{compute, downcast_primitive_array};
use arrow_array::{
    ArrayRef, ArrowPrimitiveType, BinaryViewArray, BooleanArray, GenericBinaryArray,
    GenericStringArray, OffsetSizeTrait, PrimitiveArray, StringViewArray,
};
use arrow_schema::{ArrowError, DataType};
use arrow_select::concat;
use pyo3::prelude::*;
use pyo3_arrow::error::PyArrowResult;
use pyo3_arrow::input::AnyArray;
use pyo3_arrow::PyScalar;

#[pyfunction]
pub fn min(py: Python, input: AnyArray) -> PyArrowResult<PyObject> {
    match input {
        AnyArray::Array(array) => {
            let (array, field) = array.into_inner();
            let result = min_array(array)?;
            Ok(PyScalar::try_new(result, field)?.to_arro3(py)?)
        }
        AnyArray::Stream(stream) => {
            let reader = stream.into_reader()?;
            let field = reader.field();

            // Call min_array on each array in stream
            let mut intermediate_outputs = vec![];
            for array in reader {
                intermediate_outputs.push(min_array(array?)?);
            }

            // Concatenate intermediate outputs into a single array
            let refs = intermediate_outputs
                .iter()
                .map(|x| x.as_ref())
                .collect::<Vec<_>>();
            let concatted = concat::concat(refs.as_slice())?;

            // Call min_array on intermediate outputs
            let result = min_array(concatted)?;
            Ok(PyScalar::try_new(result, field)?.to_arro3(py)?)
        }
    }
}

fn min_array(array: ArrayRef) -> Result<ArrayRef, ArrowError> {
    let array_ref = array.as_ref();

    let array = downcast_primitive_array!(
        array_ref => {
            min_primitive(array_ref)
        }
        DataType::Utf8 => min_string(array.as_string::<i32>()),
        DataType::LargeUtf8 => min_string(array.as_string::<i64>()),
        DataType::Utf8View => min_string_view(array.as_string_view()),
        DataType::Binary => min_binary(array.as_binary::<i32>()),
        DataType::LargeBinary => min_binary(array.as_binary::<i64>()),
        DataType::BinaryView => min_binary_view(array.as_binary_view()),
        DataType::Boolean => min_boolean(array.as_boolean()),
        d => return Err(ArrowError::ComputeError(format!("{d:?} not supported in rank")))
    );
    Ok(array)
}

#[inline(never)]
fn min_primitive<T: ArrowPrimitiveType>(array: &PrimitiveArray<T>) -> ArrayRef {
    let mut builder = PrimitiveBuilder::<T>::with_capacity(1);
    builder.append_option(compute::min(array));
    Arc::new(builder.finish())
}

#[inline(never)]
fn min_string<O: OffsetSizeTrait>(array: &GenericStringArray<O>) -> ArrayRef {
    let mut builder = GenericStringBuilder::<O>::new();
    builder.append_option(compute::min_string(array));
    Arc::new(builder.finish())
}

#[inline(never)]
fn min_string_view(array: &StringViewArray) -> ArrayRef {
    let mut builder = StringViewBuilder::new();
    builder.append_option(compute::min_string_view(array));
    Arc::new(builder.finish())
}

#[inline(never)]
fn min_binary<O: OffsetSizeTrait>(array: &GenericBinaryArray<O>) -> ArrayRef {
    let mut builder = GenericBinaryBuilder::<O>::new();
    builder.append_option(compute::min_binary(array));
    Arc::new(builder.finish())
}

#[inline(never)]
fn min_binary_view(array: &BinaryViewArray) -> ArrayRef {
    let mut builder = BinaryViewBuilder::new();
    builder.append_option(compute::min_binary_view(array));
    Arc::new(builder.finish())
}

#[inline(never)]
fn min_boolean(array: &BooleanArray) -> ArrayRef {
    let mut builder = BooleanBuilder::new();
    builder.append_option(compute::min_boolean(array));
    Arc::new(builder.finish())
}