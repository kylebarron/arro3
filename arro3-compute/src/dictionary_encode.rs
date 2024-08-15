use std::sync::Arc;

use arrow::array::{AsArray, GenericByteDictionaryBuilder, PrimitiveDictionaryBuilder};
use arrow::datatypes::{
    BinaryType, ByteArrayType, Int32Type, LargeBinaryType, LargeUtf8Type, Utf8Type,
};
use arrow::downcast_primitive_array;
use arrow_array::{ArrayRef, ArrowPrimitiveType, GenericByteArray, PrimitiveArray};
use arrow_schema::{ArrowError, DataType};
use pyo3::prelude::*;
use pyo3_arrow::error::PyArrowResult;
use pyo3_arrow::PyArray;

#[pyfunction]
pub(crate) fn dictionary_encode(py: Python, array: PyArray) -> PyArrowResult<PyObject> {
    let (array, _field) = array.into_inner();
    let array_ref = array.as_ref();

    let output_array: ArrayRef = downcast_primitive_array!(
        array_ref => {
            primitive_dictionary_encode(array_ref)
        }
        DataType::Utf8 => bytes_dictionary_encode(array.as_bytes::<Utf8Type>()),
        DataType::LargeUtf8 => bytes_dictionary_encode(array.as_bytes::<LargeUtf8Type>()),
        DataType::Binary => bytes_dictionary_encode(array.as_bytes::<BinaryType>()),
        DataType::LargeBinary => bytes_dictionary_encode(array.as_bytes::<LargeBinaryType>()),
        DataType::Dictionary(_, _) => array,
        d => return Err(ArrowError::ComputeError(format!("{d:?} not supported in rank")).into())
    );

    Ok(PyArray::from_array_ref(output_array).to_arro3(py)?)
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
