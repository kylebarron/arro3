use arrow_array::cast::AsArray;
use arrow_array::ArrayRef;
use arrow_schema::{ArrowError, DataType, Field};
use pyo3::prelude::*;
use pyo3::IntoPyObjectExt;
use pyo3_arrow::error::PyArrowResult;
use pyo3_arrow::export::{Arro3Array, Arro3ArrayReader};
use pyo3_arrow::ffi::ArrayIterator;
use pyo3_arrow::input::AnyArray;
use pyo3_arrow::PyArrayReader;

#[pyfunction]
pub(crate) fn dictionary_indices<'py>(
    py: Python<'py>,
    array: AnyArray,
) -> PyArrowResult<Bound<'py, PyAny>> {
    match array {
        AnyArray::Array(array) => {
            let (array, _field) = array.into_inner();
            let output_array = _dictionary_indices(array)?;
            Ok(Arro3Array::from(output_array).into_bound_py_any(py)?)
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
                Arro3ArrayReader::from(PyArrayReader::new(Box::new(ArrayIterator::new(
                    iter,
                    out_field.into(),
                ))))
                .into_bound_py_any(py)?,
            )
        }
    }
}

/// Access the dictionary of the dictionary array
///
/// This is equivalent to the `.dictionary` attribute on a PyArrow DictionaryArray.
#[pyfunction]
pub(crate) fn dictionary_dictionary<'py>(
    py: Python<'py>,
    array: AnyArray,
) -> PyArrowResult<Bound<'py, PyAny>> {
    match array {
        AnyArray::Array(array) => {
            let (array, _field) = array.into_inner();
            let output_array = _dictionary_dictionary(array)?;
            Ok(Arro3Array::from(output_array).into_bound_py_any(py)?)
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
                Arro3ArrayReader::from(PyArrayReader::new(Box::new(ArrayIterator::new(
                    iter,
                    out_field.into(),
                ))))
                .into_bound_py_any(py)?,
            )
        }
    }
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
