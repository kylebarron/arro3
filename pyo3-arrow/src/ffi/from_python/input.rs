use crate::array_reader::PyArrayReader;
use crate::input::{AnyArray, AnyRecordBatch};
use crate::{PyArray, PyRecordBatch, PyRecordBatchReader};
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
        if ob.hasattr("__arrow_c_array__")? {
            Ok(Self::Array(PyArray::extract_bound(ob)?))
        } else if ob.hasattr("__arrow_c_stream__")? {
            Ok(Self::Stream(PyArrayReader::extract_bound(ob)?))
        } else {
            Err(PyValueError::new_err(
                "Expected object with __arrow_c_array__ or __arrow_c_stream__ method",
            ))
        }
    }
}
