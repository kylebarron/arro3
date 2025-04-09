use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_schema::{DataType, Field};
use pyo3::prelude::*;
use pyo3_arrow::error::PyArrowResult;
use pyo3_arrow::ffi::ArrayIterator;
use pyo3_arrow::input::AnyArray;
use pyo3_arrow::{PyArray, PyArrayReader};

#[pyfunction]
pub fn is_null(py: Python, input: AnyArray) -> PyArrowResult<PyObject> {
    match input {
        AnyArray::Array(input) => {
            let out = arrow_arith::boolean::is_null(input.as_ref())?;
            Ok(PyArray::from_array_ref(Arc::new(out))
                .to_arro3(py)?
                .unbind())
        }
        AnyArray::Stream(input) => {
            let input = input.into_reader()?;
            let out_field = Field::new("", DataType::Boolean, true);

            let iter = input.into_iter().map(move |input| {
                let out = arrow_arith::boolean::is_null(&input?)?;
                Ok(Arc::new(out) as ArrayRef)
            });
            Ok(
                PyArrayReader::new(Box::new(ArrayIterator::new(iter, out_field.into())))
                    .to_arro3(py)?
                    .unbind(),
            )
        }
    }
}

#[pyfunction]
pub fn is_not_null(py: Python, input: AnyArray) -> PyArrowResult<PyObject> {
    match input {
        AnyArray::Array(input) => {
            let out = arrow_arith::boolean::is_not_null(input.as_ref())?;
            Ok(PyArray::from_array_ref(Arc::new(out))
                .to_arro3(py)?
                .unbind())
        }
        AnyArray::Stream(input) => {
            let input = input.into_reader()?;
            let out_field = Field::new("", DataType::Boolean, true);

            let iter = input.into_iter().map(move |input| {
                let out = arrow_arith::boolean::is_not_null(&input?)?;
                Ok(Arc::new(out) as ArrayRef)
            });
            Ok(
                PyArrayReader::new(Box::new(ArrayIterator::new(iter, out_field.into())))
                    .to_arro3(py)?
                    .unbind(),
            )
        }
    }
}
