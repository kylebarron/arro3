use std::ffi::CString;

use arrow::ffi_stream::ArrowArrayStreamReader as ArrowRecordBatchStreamReader;
use arrow::ffi_stream::FFI_ArrowArrayStream;
use arrow_array::RecordBatchReader;
use arrow_array::{RecordBatch, RecordBatchIterator};
use arrow_schema::SchemaRef;
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyTuple, PyType};

use crate::error::PyArrowResult;
use crate::ffi::from_python::utils::import_stream_pycapsule;

#[pyclass(module = "arro3.core._rust", name = "Table", subclass)]
#[derive(Debug)]
pub struct PyTable {
    batches: Vec<RecordBatch>,
    schema: SchemaRef,
}

impl PyTable {
    pub fn new(schema: SchemaRef, batches: Vec<RecordBatch>) -> Self {
        Self { schema, batches }
    }

    pub fn batches(&self) -> &[RecordBatch] {
        &self.batches
    }

    pub fn into_inner(self) -> (Vec<RecordBatch>, SchemaRef) {
        (self.batches, self.schema)
    }

    pub fn to_python(&self, py: Python) -> PyArrowResult<PyObject> {
        let arro3_mod = py.import_bound(intern!(py, "arro3.core"))?;
        let core_obj = arro3_mod.getattr(intern!(py, "Table"))?.call_method1(
            intern!(py, "from_arrow_pycapsule"),
            PyTuple::new_bound(py, vec![self.__arrow_c_stream__(py, None)?]),
        )?;
        Ok(core_obj.to_object(py))
    }
}

#[pymethods]
impl PyTable {
    /// An implementation of the [Arrow PyCapsule
    /// Interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html).
    /// This dunder method should not be called directly, but enables zero-copy
    /// data transfer to other Python libraries that understand Arrow memory.
    ///
    /// For example, you can call [`pyarrow.table()`][pyarrow.table] to convert this array
    /// into a pyarrow table, without copying memory.
    #[allow(unused_variables)]
    fn __arrow_c_stream__<'py>(
        &'py self,
        py: Python<'py>,
        requested_schema: Option<PyObject>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let batches = self.batches.clone();

        let record_batch_reader = Box::new(RecordBatchIterator::new(
            batches.into_iter().map(Ok),
            self.schema.clone(),
        ));
        let ffi_stream = FFI_ArrowArrayStream::new(record_batch_reader);

        let stream_capsule_name = CString::new("arrow_array_stream").unwrap();
        PyCapsule::new_bound(py, ffi_stream, Some(stream_capsule_name))
    }

    pub fn __eq__(&self, other: &PyTable) -> bool {
        self.batches == other.batches && self.schema == other.schema
    }

    pub fn __len__(&self) -> usize {
        self.batches.iter().fold(0, |acc, x| acc + x.num_rows())
    }

    /// Construct this object from existing Arrow data
    ///
    /// Args:
    ///     input: Arrow array to use for constructing this object
    ///
    /// Returns:
    ///     Self
    #[classmethod]
    pub fn from_arrow(_cls: &Bound<PyType>, input: &Bound<PyAny>) -> PyResult<Self> {
        input.extract()
    }

    /// Construct this object from a bare Arrow PyCapsule
    #[classmethod]
    pub fn from_arrow_pycapsule(
        _cls: &Bound<PyType>,
        capsule: &Bound<PyCapsule>,
    ) -> PyResult<Self> {
        let stream = import_stream_pycapsule(capsule)?;
        let stream_reader = ArrowRecordBatchStreamReader::try_new(stream)
            .map_err(|err| PyValueError::new_err(err.to_string()))?;
        let schema = stream_reader.schema();

        let mut batches = vec![];
        for batch in stream_reader {
            let batch = batch.map_err(|err| PyTypeError::new_err(err.to_string()))?;
            batches.push(batch);
        }

        Ok(Self::new(schema, batches))
    }
}
