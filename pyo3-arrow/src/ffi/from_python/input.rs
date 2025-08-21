use crate::array_reader::PyArrayReader;
use crate::input::{AnyArray, AnyDatum, AnyRecordBatch};
use crate::{PyArray, PyScalar};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::{intern, PyAny, PyResult};

impl<'a> FromPyObject<'a> for AnyRecordBatch {
    fn extract_bound(ob: &Bound<'a, PyAny>) -> PyResult<Self> {
        if ob.hasattr(intern!(ob.py(), "__arrow_c_array__"))? {
            Ok(Self::RecordBatch(ob.extract()?))
        } else if ob.hasattr(intern!(ob.py(), "__arrow_c_stream__"))? {
            Ok(Self::Stream(ob.extract()?))
        } else {
            Err(PyValueError::new_err(
                "Expected object with __arrow_c_array__ or __arrow_c_stream__ method",
            ))
        }
    }
}

impl<'a> FromPyObject<'a> for AnyArray {
    fn extract_bound(ob: &Bound<'a, PyAny>) -> PyResult<Self> {
        // First extract infallibly if __arrow_c_array__ method is present, so that any exception
        // in that gets propagated. Also check if PyArray extract works so that Buffer Protocol
        // conversion still works.
        // Do the same for __arrow_c_stream__ and PyArrayReader below.
        if ob.hasattr(intern!(ob.py(), "__arrow_c_array__"))? {
            Ok(Self::Array(ob.extract()?))
        } else if let Ok(arr) = ob.extract::<PyArray>() {
            Ok(Self::Array(arr))
        } else if ob.hasattr(intern!(ob.py(), "__arrow_c_stream__"))? {
            Ok(Self::Stream(ob.extract()?))
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
        let array = ob.extract::<PyArray>()?;
        if array.as_ref().len() == 1 {
            let (array, field) = array.into_inner();
            Ok(Self::Scalar(PyScalar::try_new(array, field)?))
        } else {
            Ok(Self::Array(array))
        }
    }
}
