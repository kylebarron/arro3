//! Contains the [`PyArrowError`], the Error returned by most fallible functions in this crate.

use numpy::BorrowError;
use pyo3::exceptions::{PyException, PyValueError};
use pyo3::prelude::*;
use pyo3::CastError;
use thiserror::Error;

/// The Error variants returned by this crate.
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum PyArrowError {
    /// A wrapped [arrow_schema::ArrowError]
    #[error(transparent)]
    ArrowError(#[from] arrow_schema::ArrowError),

    /// A wrapped [PyErr]
    #[error(transparent)]
    PyErr(#[from] PyErr),

    /// Indicates why borrowing an array failed.
    #[error(transparent)]
    NumpyBorrowError(#[from] BorrowError),
}

impl From<PyArrowError> for PyErr {
    fn from(error: PyArrowError) -> Self {
        match error {
            PyArrowError::PyErr(err) => err,
            PyArrowError::ArrowError(err) => PyException::new_err(err.to_string()),
            PyArrowError::NumpyBorrowError(err) => PyException::new_err(err.to_string()),
        }
    }
}

impl<'a, 'py> From<CastError<'a, 'py>> for PyArrowError {
    fn from(other: CastError<'a, 'py>) -> Self {
        Self::PyErr(PyValueError::new_err(format!(
            "Could not downcast: {}",
            other
        )))
    }
}

/// A type wrapper around `Result<T, PyArrowError>`.
pub type PyArrowResult<T> = Result<T, PyArrowError>;
