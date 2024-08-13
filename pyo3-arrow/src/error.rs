//! Contains the [`PyArrowError`], the Error returned by most fallible functions in this crate.

use pyo3::exceptions::{PyException, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::PyDowncastError;

/// The Error variants returned by this crate.
pub enum PyArrowError {
    /// A wrapped [arrow::error::ArrowError]
    ArrowError(arrow::error::ArrowError),
    /// A wrapped [PyErr]
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

/// A type wrapper around `Result<T, PyArrowError>`.
pub type PyArrowResult<T> = Result<T, PyArrowError>;
