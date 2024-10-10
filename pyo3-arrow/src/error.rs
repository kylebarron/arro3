//! Contains the [`PyArrowError`], the Error returned by most fallible functions in this crate.

use pyo3::exceptions::{PyException, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::PyDowncastError;
use thiserror::Error;

/// The Error variants returned by this crate.
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum PyArrowError {
    /// A wrapped [arrow::error::ArrowError]
    #[error(transparent)]
    ArrowError(#[from] arrow::error::ArrowError),

    /// A wrapped [PyErr]
    #[error(transparent)]
    PyErr(#[from] PyErr),
}

impl From<PyArrowError> for PyErr {
    fn from(error: PyArrowError) -> Self {
        match error {
            PyArrowError::PyErr(err) => err,
            PyArrowError::ArrowError(err) => PyException::new_err(err.to_string()),
        }
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

/// A type wrapper around `Result<T, PyArrowError>`.
pub type PyArrowResult<T> = Result<T, PyArrowError>;
