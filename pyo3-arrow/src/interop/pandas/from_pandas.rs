use std::sync::Arc;

use arrow::datatypes::{
    Float16Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type,
    UInt32Type, UInt64Type, UInt8Type,
};
use arrow_array::{ArrayRef, BooleanArray, PrimitiveArray, RecordBatch};
use arrow_schema::SchemaRef;
use indexmap::IndexMap;
use numpy::{dtype_bound, PyArray1, PyArrayDescr, PyUntypedArray};
use pyo3::exceptions::PyValueError;
use pyo3::intern;
use pyo3::prelude::*;

use crate::error::PyArrowResult;

enum PandasDtype<'a> {
    Numpy(&'a PyArrayDescr),
}

impl<'py> FromPyObject<'py> for PandasDtype<'py> {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        Ok(Self::Numpy(ob.extract()?))
    }
}

pub fn from_pandas_dataframe(
    py: Python,
    df: &PyObject,
    schema: Option<SchemaRef>,
) -> PyArrowResult<(Vec<RecordBatch>, SchemaRef)> {
    let dtypes = access_dtypes(py, df)?;
    let mapping = todo!();
    // let fields = vec![];
}

fn access_dtypes<'py>(
    py: Python<'py>,
    df: &PyObject,
) -> PyResult<IndexMap<String, PandasDtype<'py>>> {
    let dtypes_dict = df
        .getattr(py, intern!(py, "dtypes"))?
        .call_method0(py, intern!(py, "to_dict"))?;
    dtypes_dict.extract(py)
}
