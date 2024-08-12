use std::fmt::Display;
use std::sync::Arc;

use arrow::ffi_stream::ArrowArrayStreamReader as ArrowRecordBatchStreamReader;
use arrow_array::RecordBatch;
use arrow_array::{ArrayRef, RecordBatchReader, StructArray};
use arrow_schema::{Field, SchemaRef};
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyTuple};

use crate::ffi::from_python::utils::import_stream_pycapsule;
use crate::ffi::to_python::chunked::ArrayIterator;
use crate::ffi::to_python::nanoarrow::to_nanoarrow_array_stream;
use crate::ffi::to_python::to_stream_pycapsule;
use crate::schema::display_schema;
use crate::utils::schema_equals;

// TODO: remove;
#[pyclass]
#[derive(Debug)]
pub struct PyTable {
    batches: Vec<RecordBatch>,
    schema: SchemaRef,
}

impl PyTable {
    pub fn new(batches: Vec<RecordBatch>, schema: SchemaRef) -> Self {
        assert!(
            batches
                .iter()
                .all(|rb| schema_equals(rb.schema_ref(), &schema)),
            "All batches must have same schema"
        );
        Self { schema, batches }
    }

    pub fn batches(&self) -> &[RecordBatch] {
        &self.batches
    }

    pub fn into_inner(self) -> (Vec<RecordBatch>, SchemaRef) {
        (self.batches, self.schema)
    }

    /// Construct an Arrow C Stream PyCapsule
    #[allow(unused_variables)]
    pub fn __arrow_c_stream__<'py>(
        &'py self,
        py: Python<'py>,
        requested_schema: Option<Bound<PyCapsule>>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let field = self.schema.fields().clone();
        let array_reader = self.batches.clone().into_iter().map(|batch| {
            let arr: ArrayRef = Arc::new(StructArray::from(batch));
            Ok(arr)
        });
        let array_reader = Box::new(ArrayIterator::new(
            array_reader,
            Field::new_struct("", field, false).into(),
        ));
        to_stream_pycapsule(py, array_reader, requested_schema)
    }

    /// Construct this from a bare Arrow C Stream PyCapsule
    pub fn from_arrow_pycapsule(capsule: &Bound<PyCapsule>) -> PyResult<Self> {
        let stream = import_stream_pycapsule(capsule)?;
        let stream_reader = ArrowRecordBatchStreamReader::try_new(stream)
            .map_err(|err| PyValueError::new_err(err.to_string()))?;
        let schema = stream_reader.schema();

        let mut batches = vec![];
        for batch in stream_reader {
            let batch = batch.map_err(|err| PyTypeError::new_err(err.to_string()))?;
            batches.push(batch);
        }

        Ok(Self::new(batches, schema))
    }

    /// Export this to a Python `arro3.core.Table`.
    pub fn to_arro3(&self, py: Python) -> PyResult<PyObject> {
        let arro3_mod = py.import_bound(intern!(py, "arro3.core"))?;
        let core_obj = arro3_mod.getattr(intern!(py, "Table"))?.call_method1(
            intern!(py, "from_arrow_pycapsule"),
            PyTuple::new_bound(py, vec![self.__arrow_c_stream__(py, None)?]),
        )?;
        Ok(core_obj.to_object(py))
    }

    /// Export this to a Python `nanoarrow.ArrayStream`.
    pub fn to_nanoarrow(&self, py: Python) -> PyResult<PyObject> {
        to_nanoarrow_array_stream(py, &self.__arrow_c_stream__(py, None)?)
    }

    /// Export to a pyarrow.Table
    ///
    /// Requires pyarrow >=14
    pub fn to_pyarrow(self, py: Python) -> PyResult<PyObject> {
        let pyarrow_mod = py.import_bound(intern!(py, "pyarrow"))?;
        let pyarrow_obj = pyarrow_mod
            .getattr(intern!(py, "table"))?
            .call1(PyTuple::new_bound(py, vec![self.into_py(py)]))?;
        Ok(pyarrow_obj.to_object(py))
    }
}

impl Display for PyTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "arro3.core.Table")?;
        writeln!(f, "-----------")?;
        display_schema(&self.schema, f)
    }
}
