use std::ffi::CString;
use std::fmt::Display;

use arrow::ffi_stream::ArrowArrayStreamReader as ArrowRecordBatchStreamReader;
use arrow::ffi_stream::FFI_ArrowArrayStream;
use arrow_array::RecordBatchReader;
use arrow_array::{RecordBatch, RecordBatchIterator};
use arrow_schema::SchemaRef;
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyTuple, PyType};

use crate::ffi::from_python::utils::import_stream_pycapsule;
use crate::ffi::to_python::nanoarrow::to_nanoarrow_array_stream;
use crate::schema::display_schema;
use crate::PySchema;

/// A Python-facing Arrow table.
///
/// This is a wrapper around a [SchemaRef] and a `Vec` of [RecordBatch].
#[pyclass(module = "arro3.core._rust", name = "Table", subclass)]
#[derive(Debug)]
pub struct PyTable {
    batches: Vec<RecordBatch>,
    schema: SchemaRef,
}

impl PyTable {
    pub fn new(batches: Vec<RecordBatch>, schema: SchemaRef) -> Self {
        Self { schema, batches }
    }

    pub fn batches(&self) -> &[RecordBatch] {
        &self.batches
    }

    pub fn into_inner(self) -> (Vec<RecordBatch>, SchemaRef) {
        (self.batches, self.schema)
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

    pub fn __repr__(&self) -> String {
        self.to_string()
    }

    /// Construct this object from an existing Arrow object.
    ///
    /// It can be called on anything that exports the Arrow stream interface
    /// (`__arrow_c_stream__`) and yields a StructArray for each item. This Table will materialize
    /// all items from the iterator in memory at once. Use RecordBatchReader if you don't wish to
    /// materialize all batches in memory at once.
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

        Ok(Self::new(batches, schema))
    }

    /// Number of columns in this table.
    #[getter]
    fn num_columns(&self) -> usize {
        self.schema.fields().len()
    }

    /// Access the schema of this table
    #[getter]
    fn schema(&self, py: Python) -> PyResult<PyObject> {
        PySchema::new(self.schema.clone()).to_arro3(py)
    }
}
