use std::sync::Arc;

use arrow_array::builder::{
    GenericByteDictionaryBuilder, Int32Builder, PrimitiveDictionaryBuilder,
};
use arrow_array::cast::AsArray;
use arrow_array::downcast_primitive_array;
use arrow_array::types::{
    BinaryType, ByteArrayType, Int32Type, LargeBinaryType, LargeUtf8Type, Utf8Type,
};
use arrow_array::{
    ArrayRef, ArrowPrimitiveType, BooleanArray, DictionaryArray, GenericByteArray, PrimitiveArray,
};
use arrow_schema::{ArrowError, DataType, Field};
use pyo3::prelude::*;
use pyo3::IntoPyObjectExt;
use pyo3_arrow::error::PyArrowResult;
use pyo3_arrow::export::{Arro3Array, Arro3ArrayReader};
use pyo3_arrow::ffi::ArrayIterator;
use pyo3_arrow::input::AnyArray;
use pyo3_arrow::PyArrayReader;

// Note: for chunked array input, each output chunk will not necessarily have the same dictionary
#[pyfunction]
pub(crate) fn dictionary_encode<'py>(
    py: Python<'py>,
    array: AnyArray,
) -> PyArrowResult<Bound<'py, PyAny>> {
    match array {
        AnyArray::Array(array) => {
            let (array, _field) = array.into_inner();
            let output_array = dictionary_encode_array(array)?;
            Ok(Arro3Array::from(output_array).into_bound_py_any(py)?)
        }
        AnyArray::Stream(stream) => {
            let reader = stream.into_reader()?;

            let existing_field = reader.field();
            let output_data_type = DataType::Dictionary(
                Box::new(DataType::Int32),
                Box::new(existing_field.data_type().clone()),
            );
            let output_field = Field::new("", output_data_type, true);

            let iter = reader
                .into_iter()
                .map(move |array| dictionary_encode_array(array?));
            Ok(
                Arro3ArrayReader::from(PyArrayReader::new(Box::new(ArrayIterator::new(
                    iter,
                    output_field.into(),
                ))))
                .into_bound_py_any(py)?,
            )
        }
    }
}

fn dictionary_encode_array(array: ArrayRef) -> Result<ArrayRef, ArrowError> {
    let array_ref = array.as_ref();
    let array = downcast_primitive_array!(
        array_ref => {
            primitive_dictionary_encode(array_ref)
        }
        DataType::Boolean => boolean_dictionary_encode(array.as_boolean())?,
        DataType::Utf8 => bytes_dictionary_encode(array.as_bytes::<Utf8Type>()),
        DataType::LargeUtf8 => bytes_dictionary_encode(array.as_bytes::<LargeUtf8Type>()),
        DataType::Binary => bytes_dictionary_encode(array.as_bytes::<BinaryType>()),
        DataType::LargeBinary => bytes_dictionary_encode(array.as_bytes::<LargeBinaryType>()),
        DataType::Dictionary(_, _) => array,
        d => return Err(ArrowError::ComputeError(format!("{d:?} not supported in dictionary_encode")))
    );
    Ok(array)
}

#[inline(never)]
fn primitive_dictionary_encode<T: ArrowPrimitiveType>(array: &PrimitiveArray<T>) -> ArrayRef {
    let mut builder = PrimitiveDictionaryBuilder::<Int32Type, T>::new();
    for value in array {
        builder.append_option(value);
    }
    Arc::new(builder.finish())
}

#[inline(never)]
fn boolean_dictionary_encode(array: &BooleanArray) -> Result<ArrayRef, ArrowError> {
    // There is no dictionary builder for booleans in arrow-rs, so build the keys by
    // hand. Dictionary values are stored in order of first appearance, matching
    // the behavior of pyarrow's dictionary_encode.
    let mut values: Vec<bool> = Vec::with_capacity(2);
    let mut keys = Int32Builder::with_capacity(array.len());
    for value in array {
        match value {
            Some(value) => {
                let key = match values.iter().position(|v| *v == value) {
                    Some(key) => key,
                    None => {
                        values.push(value);
                        values.len() - 1
                    }
                };
                keys.append_value(key as i32);
            }
            None => keys.append_null(),
        }
    }
    let dictionary =
        DictionaryArray::<Int32Type>::try_new(keys.finish(), Arc::new(BooleanArray::from(values)))?;
    Ok(Arc::new(dictionary))
}

#[inline(never)]
fn bytes_dictionary_encode<T: ByteArrayType>(array: &GenericByteArray<T>) -> ArrayRef {
    let mut builder = GenericByteDictionaryBuilder::<Int32Type, T>::new();
    for value in array {
        builder.append_option(value);
    }
    Arc::new(builder.finish())
}
