use crate::array_reader::PyArrayReader;
use crate::input::{AnyArray, AnyDatum, AnyRecordBatch};
use crate::{PyArray, PyRecordBatch, PyRecordBatchReader, PyScalar};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::{PyAny, PyResult};

impl<'a> FromPyObject<'a> for AnyRecordBatch {
    fn extract_bound(ob: &Bound<'a, PyAny>) -> PyResult<Self> {
        if ob.hasattr("__arrow_c_array__")? {
            Ok(Self::RecordBatch(PyRecordBatch::extract_bound(ob)?))
        } else if ob.hasattr("__arrow_c_stream__")? {
            Ok(Self::Stream(PyRecordBatchReader::extract_bound(ob)?))
        } else {
            Err(PyValueError::new_err(
                "Expected object with __arrow_c_array__ or __arrow_c_stream__ method",
            ))
        }
    }
}

impl<'a> FromPyObject<'a> for AnyArray {
    fn extract_bound(ob: &Bound<'a, PyAny>) -> PyResult<Self> {
        if let Ok(arr) = ob.extract::<PyArray>() {
            Ok(Self::Array(arr))
        } else if let Ok(stream) = ob.extract::<PyArrayReader>() {
            Ok(Self::Stream(stream))
        } else {
            Err(PyValueError::new_err(
                "Expected object with __arrow_c_array__ or __arrow_c_stream__ method or implementing buffer protocol.",
            ))
        }
    }
}

impl<'a> FromPyObject<'a> for AnyDatum {
    fn extract_bound(ob: &Bound<'a, PyAny>) -> PyResult<Self> {
        let array = PyArray::extract_bound(ob)?;
        if array.as_ref().len() == 1 {
            let (array, field) = array.into_inner();
            Ok(Self::Scalar(PyScalar::try_new(array, field)?))
        } else {
            Ok(Self::Array(array))
        }
    }
}
