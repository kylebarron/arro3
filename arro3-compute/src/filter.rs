use arrow_array::cast::AsArray;
use arrow_schema::{ArrowError, DataType};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::IntoPyObjectExt;
use pyo3_arrow::error::PyArrowResult;
use pyo3_arrow::export::{Arro3Array, Arro3ArrayReader};
use pyo3_arrow::ffi::ArrayIterator;
use pyo3_arrow::input::AnyArray;
use pyo3_arrow::{PyArray, PyArrayReader};

#[pyfunction]
pub fn filter<'py>(
    py: Python<'py>,
    values: AnyArray,
    predicate: AnyArray,
) -> PyArrowResult<Bound<'py, PyAny>> {
    match (values, predicate) {
        (AnyArray::Array(values), AnyArray::Array(predicate)) => {
            let (values, values_field) = values.into_inner();
            let predicate = predicate
                .as_ref()
                .as_boolean_opt()
                .ok_or(ArrowError::ComputeError(
                    "Expected boolean array for predicate".to_string(),
                ))?;

            let filtered = arrow_select::filter::filter(values.as_ref(), predicate)?;
            let pyarray = PyArray::new(filtered, values_field);
            Ok(Arro3Array::from(pyarray).into_bound_py_any(py)?)
        }
        (AnyArray::Stream(values), AnyArray::Stream(predicate)) => {
            let values = values.into_reader()?;
            let predicate = predicate.into_reader()?;

            if !predicate
                .field()
                .data_type()
                .equals_datatype(&DataType::Boolean)
            {
                return Err(PyValueError::new_err("Expected boolean array for predicate").into());
            }

            let values_field = values.field();

            let iter = values
                .into_iter()
                .zip(predicate)
                .map(move |(values, predicate)| {
                    let predicate_arr = predicate?;
                    let filtered =
                        arrow_select::filter::filter(values?.as_ref(), predicate_arr.as_boolean())?;
                    Ok(filtered)
                });
            Ok(
                Arro3ArrayReader::from(PyArrayReader::new(Box::new(ArrayIterator::new(
                    iter,
                    values_field,
                ))))
                .into_bound_py_any(py)?,
            )
        }
        _ => Err(PyValueError::new_err("Unsupported combination of array and stream").into()),
    }
}
