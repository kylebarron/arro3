use arrow_array::RecordBatch;
use arrow_schema::{Schema, SchemaRef};
use indexmap::IndexMap;
use numpy::PyArrayDescr;
use pyo3::intern;
use pyo3::prelude::*;

use crate::error::PyArrowResult;
use crate::PyTable;

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
    df: &Bound<PyAny>,
    schema: Option<SchemaRef>,
) -> PyArrowResult<(Vec<RecordBatch>, SchemaRef)> {
    // If pandas 2.2+ and the Arrow C Stream export works, prefer that.
    if df.hasattr(intern!(py, "__arrow_c_stream__"))? {
        if let Ok(table) = df.extract::<PyTable>() {
            return Ok(table.into_inner());
        }
    }

    let schema = if let Some(schema) = schema {
        schema
    } else {
        infer_arrow_schema(py, df)?
    };
    let batch = import_batch(py, df, &schema)?;
    Ok((vec![batch], schema))
}

fn infer_arrow_schema<'py>(py: Python<'py>, df: &'py Bound<PyAny>) -> PyResult<SchemaRef> {
    let dtypes = access_dtypes(py, df)?;
    todo!()
}

fn import_batch<'py>(
    py: Python<'py>,
    df: &'py Bound<PyAny>,
    schema: &Schema,
) -> PyResult<RecordBatch> {
    todo!()
}

fn access_dtypes<'py>(
    py: Python<'py>,
    df: &'py Bound<PyAny>,
) -> PyResult<IndexMap<String, PandasDtype<'py>>> {
    let dtypes_dict = df
        .getattr(intern!(py, "dtypes"))?
        .call_method0(intern!(py, "to_dict"))?;
    dtypes_dict.extract()
}
