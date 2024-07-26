use pyo3::exceptions::{PyException, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::PyDowncastError;

pub enum PyArrowError {
    ArrowError(arrow::error::ArrowError),
    PyErr(PyErr),
}

impl From<PyArrowError> for PyErr {
    fn from(error: PyArrowError) -> Self {
        match error {
            PyArrowError::ArrowError(err) => PyException::new_err(err.to_string()),
            PyArrowError::PyErr(err) => err,
        }
    }
}

impl From<arrow::error::ArrowError> for PyArrowError {
    fn from(other: arrow::error::ArrowError) -> Self {
        Self::ArrowError(other)
    }
}

impl From<PyTypeError> for PyArrowError {
    fn from(other: PyTypeError) -> Self {
        Self::PyErr((&other).into())
    }
}

impl<'a> From<PyDowncastError<'a>> for PyArrowError {
    fn from(other: PyDowncastError<'a>) -> Self {
        Self::PyErr(PyValueError::new_err(format!(
            "Could not downcast: {}",
            other
        )))
    }
}

impl From<PyValueError> for PyArrowError {
    fn from(other: PyValueError) -> Self {
        Self::PyErr((&other).into())
    }
}

impl From<PyErr> for PyArrowError {
    fn from(other: PyErr) -> Self {
        Self::PyErr(other)
    }
}

pub type PyArrowResult<T> = Result<T, PyArrowError>;
