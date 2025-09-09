use std::sync::Arc;

use arrow_arith::aggregate as compute;
use arrow_array::builder::{
    BinaryViewBuilder, BooleanBuilder, GenericBinaryBuilder, GenericStringBuilder,
    PrimitiveBuilder, StringViewBuilder,
};
use arrow_array::cast::AsArray;
use arrow_array::{
    downcast_primitive_array, Array, ArrayRef, ArrowPrimitiveType, BinaryViewArray, BooleanArray,
    GenericBinaryArray, GenericStringArray, OffsetSizeTrait, PrimitiveArray, StringViewArray,
};
use arrow_schema::{ArrowError, DataType};
use arrow_select::concat;
use pyo3::prelude::*;
use pyo3_arrow::error::PyArrowResult;
use pyo3_arrow::export::Arro3Scalar;
use pyo3_arrow::input::AnyArray;
use pyo3_arrow::PyScalar;

#[pyfunction]
pub fn max(input: AnyArray) -> PyArrowResult<Arro3Scalar> {
    match input {
        AnyArray::Array(array) => {
            let (array, field) = array.into_inner();
            let result = max_array(array)?;
            Ok(PyScalar::try_new(result, field)?.into())
        }
        AnyArray::Stream(stream) => {
            let reader = stream.into_reader()?;
            let field = reader.field();

            // Call max_array on each array in stream
            let mut intermediate_outputs = vec![];
            for array in reader {
                intermediate_outputs.push(max_array(array?)?);
            }

            // Concatenate intermediate outputs into a single array
            let refs = intermediate_outputs
                .iter()
                .map(|x| x.as_ref())
                .collect::<Vec<_>>();
            let concatted = concat::concat(refs.as_slice())?;

            // Call max_array on intermediate outputs
            let result = max_array(concatted)?;
            Ok(PyScalar::try_new(result, field)?.into())
        }
    }
}

fn max_array(array: ArrayRef) -> Result<ArrayRef, ArrowError> {
    let array_ref = array.as_ref();

    let array = downcast_primitive_array!(
        array_ref => {
            max_primitive(array_ref)
        }
        DataType::Utf8 => max_string(array.as_string::<i32>()),
        DataType::LargeUtf8 => max_string(array.as_string::<i64>()),
        DataType::Utf8View => max_string_view(array.as_string_view()),
        DataType::Binary => max_binary(array.as_binary::<i32>()),
        DataType::LargeBinary => max_binary(array.as_binary::<i64>()),
        DataType::BinaryView => max_binary_view(array.as_binary_view()),
        DataType::Boolean => max_boolean(array.as_boolean()),
        d => return Err(ArrowError::ComputeError(format!("{d:?} not supported in rank")))
    );
    Ok(array)
}

#[inline(never)]
fn max_primitive<T: ArrowPrimitiveType>(array: &PrimitiveArray<T>) -> ArrayRef {
    let mut builder = PrimitiveBuilder::<T>::with_capacity(1);
    builder.append_option(compute::max(array));
    // Need to append the original data type, because PrimitiveBuilder::<T> will sometimes lose the
    // exact data type. E.g. It will lose the time zone for datetime types.
    Arc::new(builder.finish().with_data_type(array.data_type().clone()))
}

#[inline(never)]
fn max_string<O: OffsetSizeTrait>(array: &GenericStringArray<O>) -> ArrayRef {
    let mut builder = GenericStringBuilder::<O>::new();
    builder.append_option(compute::max_string(array));
    Arc::new(builder.finish())
}

#[inline(never)]
fn max_string_view(array: &StringViewArray) -> ArrayRef {
    let mut builder = StringViewBuilder::new();
    builder.append_option(compute::max_string_view(array));
    Arc::new(builder.finish())
}

#[inline(never)]
fn max_binary<O: OffsetSizeTrait>(array: &GenericBinaryArray<O>) -> ArrayRef {
    let mut builder = GenericBinaryBuilder::<O>::new();
    builder.append_option(compute::max_binary(array));
    Arc::new(builder.finish())
}

#[inline(never)]
fn max_binary_view(array: &BinaryViewArray) -> ArrayRef {
    let mut builder = BinaryViewBuilder::new();
    builder.append_option(compute::max_binary_view(array));
    Arc::new(builder.finish())
}

#[inline(never)]
fn max_boolean(array: &BooleanArray) -> ArrayRef {
    let mut builder = BooleanBuilder::new();
    builder.append_option(compute::max_boolean(array));
    Arc::new(builder.finish())
}

#[pyfunction]
pub fn min(input: AnyArray) -> PyArrowResult<Arro3Scalar> {
    match input {
        AnyArray::Array(array) => {
            let (array, field) = array.into_inner();
            let result = min_array(array)?;
            Ok(PyScalar::try_new(result, field)?.into())
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
            Ok(PyScalar::try_new(result, field)?.into())
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
    // Need to append the original data type, because PrimitiveBuilder::<T> will sometimes lose the
    // exact data type. E.g. It will lose the time zone for datetime types.
    Arc::new(builder.finish().with_data_type(array.data_type().clone()))
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

#[pyfunction]
pub fn sum(input: AnyArray) -> PyArrowResult<Arro3Scalar> {
    match input {
        AnyArray::Array(array) => {
            let (array, field) = array.into_inner();
            let result = sum_array(array)?;
            Ok(PyScalar::try_new(result, field)?.into())
        }
        AnyArray::Stream(stream) => {
            let reader = stream.into_reader()?;
            let field = reader.field();

            // Call sum_array on each array in stream
            let mut intermediate_outputs = vec![];
            for array in reader {
                intermediate_outputs.push(sum_array(array?)?);
            }

            // Concatenate intermediate outputs into a single array
            let refs = intermediate_outputs
                .iter()
                .map(|x| x.as_ref())
                .collect::<Vec<_>>();
            let concatted = concat::concat(refs.as_slice())?;

            // Call sum_array on intermediate outputs
            let result = sum_array(concatted)?;
            Ok(PyScalar::try_new(result, field)?.into())
        }
    }
}

fn sum_array(array: ArrayRef) -> Result<ArrayRef, ArrowError> {
    let array_ref = array.as_ref();

    let array = downcast_primitive_array!(
        array_ref => {
            sum_primitive(array_ref)
        }
        d => return Err(ArrowError::ComputeError(format!("{d:?} not supported in rank")))
    );
    Ok(array)
}

#[inline(never)]
fn sum_primitive<T: ArrowPrimitiveType>(array: &PrimitiveArray<T>) -> ArrayRef {
    let mut builder = PrimitiveBuilder::<T>::with_capacity(1);
    builder.append_option(compute::sum(array));
    Arc::new(builder.finish())
}
