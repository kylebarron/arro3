//! Contains the [`Arro3IoError`], the Error returned by most fallible functions in this crate.

use pyo3::exceptions::{PyException, PyValueError};
use pyo3::prelude::*;
use pyo3::DowncastError;
use thiserror::Error;

/// The Error variants returned by this crate.
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum Arro3IoError {
    /// A wrapped [arrow::error::ArrowError]
    #[error(transparent)]
    ArrowError(#[from] arrow::error::ArrowError),

    /// A wrapped [object_store::Error]
    #[error(transparent)]
    ObjectStoreError(#[from] object_store::Error),

    /// A wrapped [parquet::errors::ParquetError]
    #[error(transparent)]
    ParquetError(#[from] parquet::errors::ParquetError),

    /// A wrapped [PyErr]
    #[error(transparent)]
    PyErr(#[from] PyErr),
}

impl From<Arro3IoError> for PyErr {
    fn from(error: Arro3IoError) -> Self {
        match error {
            Arro3IoError::PyErr(err) => err,
            Arro3IoError::ArrowError(err) => PyException::new_err(err.to_string()),
            Arro3IoError::ObjectStoreError(err) => PyException::new_err(err.to_string()),
            Arro3IoError::ParquetError(err) => PyException::new_err(err.to_string()),
        }
    }
}

impl<'a, 'py> From<DowncastError<'a, 'py>> for Arro3IoError {
    fn from(other: DowncastError<'a, 'py>) -> Self {
        Self::PyErr(PyValueError::new_err(format!(
            "Could not downcast: {}",
            other
        )))
    }
}

/// A type wrapper around `Result<T, Arro3IoError>`.
pub type Arro3IoResult<T> = Result<T, Arro3IoError>;
