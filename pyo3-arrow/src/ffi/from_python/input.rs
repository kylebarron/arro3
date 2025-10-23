use crate::array_reader::PyArrayReader;
use crate::input::{AnyArray, AnyDatum, AnyRecordBatch};
use crate::{PyArray, PyScalar};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::{intern, PyAny};

impl<'py> FromPyObject<'_, 'py> for AnyRecordBatch {
    type Error = PyErr;

    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
        if obj.hasattr(intern!(obj.py(), "__arrow_c_array__"))? {
            Ok(Self::RecordBatch(obj.extract()?))
        } else if obj.hasattr(intern!(obj.py(), "__arrow_c_stream__"))? {
            Ok(Self::Stream(obj.extract()?))
        } else {
            Err(PyValueError::new_err(
                "Expected object with __arrow_c_array__ or __arrow_c_stream__ method",
            ))
        }
    }
}

impl<'py> FromPyObject<'_, 'py> for AnyArray {
    type Error = PyErr;

    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
        // First extract infallibly if __arrow_c_array__ method is present, so that any exception
        // in that gets propagated. Also check if PyArray extract works so that Buffer Protocol
        // conversion still works.
        // Do the same for __arrow_c_stream__ and PyArrayReader below.
        if obj.hasattr(intern!(obj.py(), "__arrow_c_array__"))? {
            Ok(Self::Array(obj.extract()?))
        } else if let Ok(arr) = obj.extract::<PyArray>() {
            Ok(Self::Array(arr))
        } else if obj.hasattr(intern!(obj.py(), "__arrow_c_stream__"))? {
            Ok(Self::Stream(obj.extract()?))
        } else if let Ok(stream) = obj.extract::<PyArrayReader>() {
            Ok(Self::Stream(stream))
        } else {
            Err(PyValueError::new_err(
                "Expected object with __arrow_c_array__ or __arrow_c_stream__ method or implementing buffer protocol.",
            ))
        }
    }
}

impl<'py> FromPyObject<'_, 'py> for AnyDatum {
    type Error = PyErr;

    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
        let array = obj.extract::<PyArray>()?;
        if array.as_ref().len() == 1 {
            let (array, field) = array.into_inner();
            Ok(Self::Scalar(PyScalar::try_new(array, field)?))
        } else {
            Ok(Self::Array(array))
        }
    }
}
