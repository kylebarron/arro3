use std::sync::Arc;

use arrow::array::{AsArray, GenericByteDictionaryBuilder, PrimitiveDictionaryBuilder};
use arrow::datatypes::{
    BinaryType, ByteArrayType, Int32Type, LargeBinaryType, LargeUtf8Type, Utf8Type,
};
use arrow::downcast_primitive_array;
use arrow_array::{ArrayRef, ArrowPrimitiveType, GenericByteArray, PrimitiveArray};
use arrow_schema::{ArrowError, DataType, Field};
use pyo3::prelude::*;
use pyo3_arrow::error::PyArrowResult;
use pyo3_arrow::ffi::ArrayIterator;
use pyo3_arrow::input::AnyArray;
use pyo3_arrow::{PyArray, PyArrayReader};

// Note: for chunked array input, each output chunk will not necessarily have the same dictionary
#[pyfunction]
pub(crate) fn dictionary_encode(py: Python, array: AnyArray) -> PyArrowResult<PyObject> {
    match array {
        AnyArray::Array(array) => {
            let (array, _field) = array.into_inner();
            let output_array = dictionary_encode_array(array)?;
            Ok(PyArray::from_array_ref(output_array).to_arro3(py)?)
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
                PyArrayReader::new(Box::new(ArrayIterator::new(iter, output_field.into())))
                    .to_arro3(py)?,
            )
        }
    }
}

#[pyfunction]
pub(crate) fn dictionary_indices(py: Python, array: AnyArray) -> PyArrowResult<PyObject> {
    match array {
        AnyArray::Array(array) => {
            let (array, _field) = array.into_inner();
            let output_array = _dictionary_indices(array)?;
            Ok(PyArray::from_array_ref(output_array).to_arro3(py)?)
        }
        AnyArray::Stream(stream) => {
            let reader = stream.into_reader()?;
            let existing_field = reader.field();
            let out_field = match existing_field.data_type() {
                DataType::Dictionary(key_type, _value_type) => {
                    Field::new("", *key_type.clone(), true)
                }
                _ => {
                    return Err(ArrowError::ComputeError(
                        "Expected dictionary-typed Array".to_string(),
                    )
                    .into())
                }
            };
            let iter = reader
                .into_iter()
                .map(move |array| _dictionary_indices(array?));
            Ok(
                PyArrayReader::new(Box::new(ArrayIterator::new(iter, out_field.into())))
                    .to_arro3(py)?,
            )
        }
    }
}

/// Access the dictionary of the dictionary array
///
/// This is equivalent to the `.dictionary` attribute on a PyArrow DictionaryArray.
#[pyfunction]
pub(crate) fn dictionary_dictionary(py: Python, array: AnyArray) -> PyArrowResult<PyObject> {
    match array {
        AnyArray::Array(array) => {
            let (array, _field) = array.into_inner();
            let output_array = _dictionary_dictionary(array)?;
            Ok(PyArray::from_array_ref(output_array).to_arro3(py)?)
        }
        AnyArray::Stream(stream) => {
            let reader = stream.into_reader()?;
            let existing_field = reader.field();
            let out_field = match existing_field.data_type() {
                DataType::Dictionary(_key_type, value_type) => {
                    Field::new("", *value_type.clone(), true)
                }
                _ => {
                    return Err(ArrowError::ComputeError(
                        "Expected dictionary-typed Array".to_string(),
                    )
                    .into())
                }
            };
            let iter = reader
                .into_iter()
                .map(move |array| _dictionary_dictionary(array?));
            Ok(
                PyArrayReader::new(Box::new(ArrayIterator::new(iter, out_field.into())))
                    .to_arro3(py)?,
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
        DataType::Utf8 => bytes_dictionary_encode(array.as_bytes::<Utf8Type>()),
        DataType::LargeUtf8 => bytes_dictionary_encode(array.as_bytes::<LargeUtf8Type>()),
        DataType::Binary => bytes_dictionary_encode(array.as_bytes::<BinaryType>()),
        DataType::LargeBinary => bytes_dictionary_encode(array.as_bytes::<LargeBinaryType>()),
        DataType::Dictionary(_, _) => array,
        d => return Err(ArrowError::ComputeError(format!("{d:?} not supported in rank")))
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
fn bytes_dictionary_encode<T: ByteArrayType>(array: &GenericByteArray<T>) -> ArrayRef {
    let mut builder = GenericByteDictionaryBuilder::<Int32Type, T>::new();
    for value in array {
        builder.append_option(value);
    }
    Arc::new(builder.finish())
}

fn _dictionary_indices(array: ArrayRef) -> Result<ArrayRef, ArrowError> {
    match array.data_type() {
        DataType::Dictionary(_, _) => {
            let dict_arr = array.as_any_dictionary();
            let keys_arr = dict_arr.keys();
            let keys_arr_ref = keys_arr.slice(0, keys_arr.len());
            Ok(keys_arr_ref)
        }
        _ => Err(ArrowError::ComputeError(
            "Expected dictionary-typed Array".to_string(),
        )),
    }
}

fn _dictionary_dictionary(array: ArrayRef) -> Result<ArrayRef, ArrowError> {
    match array.data_type() {
        DataType::Dictionary(_, _) => {
            let dict_arr = array.as_any_dictionary();
            let values_arr = dict_arr.values().clone();
            Ok(values_arr)
        }
        _ => Err(ArrowError::ComputeError(
            "Expected dictionary-typed Array".to_string(),
        )),
    }
}
