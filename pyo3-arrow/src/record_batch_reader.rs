use std::fmt::Display;
use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatchIterator, RecordBatchReader, StructArray};
use arrow_schema::{Field, SchemaRef};
use pyo3::exceptions::{PyIOError, PyStopIteration, PyValueError};
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyTuple, PyType};

use crate::error::PyArrowResult;
use crate::ffi::from_python::utils::import_stream_pycapsule;
use crate::ffi::to_python::chunked::ArrayIterator;
use crate::ffi::to_python::nanoarrow::to_nanoarrow_array_stream;
use crate::ffi::to_python::to_stream_pycapsule;
use crate::input::AnyRecordBatch;
use crate::schema::display_schema;
use crate::{PyRecordBatch, PySchema, PyTable};

/// A Python-facing Arrow record batch reader.
///
/// This is a wrapper around a [RecordBatchReader].
#[pyclass(module = "arro3.core._core", name = "RecordBatchReader", subclass)]
pub struct PyRecordBatchReader(pub(crate) Option<Box<dyn RecordBatchReader + Send>>);

impl PyRecordBatchReader {
    pub fn new(reader: Box<dyn RecordBatchReader + Send>) -> Self {
        Self(Some(reader))
    }

    /// Consume this reader and convert into a [RecordBatchReader].
    ///
    /// The reader can only be consumed once. Calling `into_reader`
    pub fn into_reader(mut self) -> PyResult<Box<dyn RecordBatchReader + Send>> {
        let stream = self
            .0
            .take()
            .ok_or(PyIOError::new_err("Cannot read from closed stream."))?;
        Ok(stream)
    }

    /// Consume this reader and create a [PyTable] object
    pub fn into_table(mut self) -> PyArrowResult<PyTable> {
        let stream = self
            .0
            .take()
            .ok_or(PyIOError::new_err("Cannot read from closed stream."))?;
        let schema = stream.schema();
        let mut batches = vec![];
        for batch in stream {
            batches.push(batch?);
        }
        Ok(PyTable::new(batches, schema))
    }

    /// Access the [SchemaRef] of this RecordBatchReader.
    ///
    /// If the stream has already been consumed, this method will error.
    pub fn schema_ref(&self) -> PyResult<SchemaRef> {
        let stream = self
            .0
            .as_ref()
            .ok_or(PyIOError::new_err("Stream already closed."))?;
        Ok(stream.schema())
    }

    /// Export this to a Python `arro3.core.RecordBatchReader`.
    pub fn to_arro3(&mut self, py: Python) -> PyResult<PyObject> {
        let arro3_mod = py.import_bound(intern!(py, "arro3.core"))?;
        let core_obj = arro3_mod
            .getattr(intern!(py, "RecordBatchReader"))?
            .call_method1(
                intern!(py, "from_arrow_pycapsule"),
                PyTuple::new_bound(py, vec![self.__arrow_c_stream__(py, None)?]),
            )?;
        Ok(core_obj.to_object(py))
    }

    /// Export this to a Python `nanoarrow.ArrayStream`.
    pub fn to_nanoarrow(&mut self, py: Python) -> PyResult<PyObject> {
        to_nanoarrow_array_stream(py, &self.__arrow_c_stream__(py, None)?)
    }

    /// Export to a pyarrow.RecordBatchReader
    ///
    /// Requires pyarrow >=15
    pub fn to_pyarrow(self, py: Python) -> PyResult<PyObject> {
        let pyarrow_mod = py.import_bound(intern!(py, "pyarrow"))?;
        let record_batch_reader_class = pyarrow_mod.getattr(intern!(py, "RecordBatchReader"))?;
        let pyarrow_obj = record_batch_reader_class.call_method1(
            intern!(py, "from_stream"),
            PyTuple::new_bound(py, vec![self.into_py(py)]),
        )?;
        Ok(pyarrow_obj.to_object(py))
    }
}

impl From<Box<dyn RecordBatchReader + Send>> for PyRecordBatchReader {
    fn from(value: Box<dyn RecordBatchReader + Send>) -> Self {
        Self::new(value)
    }
}

impl Display for PyRecordBatchReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "arro3.core.RecordBatchReader")?;
        writeln!(f, "-----------------------")?;
        if let Ok(schema) = self.schema_ref() {
            display_schema(&schema, f)
        } else {
            writeln!(f, "Closed stream")
        }
    }
}

#[pymethods]
impl PyRecordBatchReader {
    /// An implementation of the [Arrow PyCapsule
    /// Interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html).
    /// This dunder method should not be called directly, but enables zero-copy
    /// data transfer to other Python libraries that understand Arrow memory.
    ///
    /// For example, you can call [`pyarrow.table()`][pyarrow.table] to convert this array
    /// into a pyarrow table, without copying memory.
    #[allow(unused_variables)]
    pub fn __arrow_c_stream__<'py>(
        &'py mut self,
        py: Python<'py>,
        requested_schema: Option<Bound<PyCapsule>>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let reader = self
            .0
            .take()
            .ok_or(PyIOError::new_err("Cannot read from closed stream"))?;

        let schema = reader.schema().clone();
        let array_reader = reader.into_iter().map(|maybe_batch| {
            let arr: ArrayRef = Arc::new(StructArray::from(maybe_batch?));
            Ok(arr)
        });
        let array_reader = Box::new(ArrayIterator::new(
            array_reader,
            Field::new_struct("", schema.fields().clone(), false).into(),
        ));
        to_stream_pycapsule(py, array_reader, requested_schema)
    }

    // Return self
    // https://stackoverflow.com/a/52056290
    fn __iter__(&mut self, py: Python) -> PyResult<PyObject> {
        self.to_arro3(py)
    }

    fn __next__(&mut self, py: Python) -> PyArrowResult<PyObject> {
        self.read_next_batch(py)
    }

    pub fn __repr__(&self) -> String {
        self.to_string()
    }

    /// Construct this from an existing Arrow object.
    ///
    /// It can be called on anything that exports the Arrow stream interface
    /// (`__arrow_c_stream__`), such as a `Table` or `RecordBatchReader`.
    #[classmethod]
    pub fn from_arrow(_cls: &Bound<PyType>, input: AnyRecordBatch) -> PyArrowResult<Self> {
        let reader = input.into_reader()?;
        Ok(Self::new(reader))
    }

    /// Construct this object from a bare Arrow PyCapsule.
    #[classmethod]
    pub fn from_arrow_pycapsule(
        _cls: &Bound<PyType>,
        capsule: &Bound<PyCapsule>,
    ) -> PyResult<Self> {
        let stream = import_stream_pycapsule(capsule)?;
        let stream_reader = arrow::ffi_stream::ArrowArrayStreamReader::try_new(stream)
            .map_err(|err| PyValueError::new_err(err.to_string()))?;

        Ok(Self(Some(Box::new(stream_reader))))
    }

    #[classmethod]
    pub fn from_batches(
        _cls: &Bound<PyType>,
        schema: PySchema,
        batches: Vec<PyRecordBatch>,
    ) -> Self {
        let batches = batches
            .into_iter()
            .map(|batch| batch.into_inner())
            .collect::<Vec<_>>();
        Self::new(Box::new(RecordBatchIterator::new(
            batches.into_iter().map(Ok),
            schema.into_inner(),
        )))
    }

    #[classmethod]
    pub fn from_stream(_cls: &Bound<PyType>, data: &Bound<PyAny>) -> PyResult<Self> {
        data.extract()
    }

    /// Returns `true` if this reader has already been consumed.
    #[getter]
    pub fn closed(&self) -> bool {
        self.0.is_none()
    }

    pub fn read_all(&mut self, py: Python) -> PyArrowResult<PyObject> {
        let stream = self
            .0
            .take()
            .ok_or(PyIOError::new_err("Cannot read from closed stream."))?;
        let schema = stream.schema();
        let mut batches = vec![];
        for batch in stream {
            batches.push(batch?);
        }
        Ok(PyTable::new(batches, schema).to_arro3(py)?)
    }

    pub fn read_next_batch(&mut self, py: Python) -> PyArrowResult<PyObject> {
        let stream = self
            .0
            .as_mut()
            .ok_or(PyIOError::new_err("Cannot read from closed stream."))?;

        if let Some(next_batch) = stream.next() {
            Ok(PyRecordBatch::new(next_batch?).to_arro3(py)?)
        } else {
            Err(PyStopIteration::new_err("").into())
        }
    }

    /// Access the schema of this table
    #[getter]
    pub fn schema(&self, py: Python) -> PyResult<PyObject> {
        PySchema::new(self.schema_ref()?.clone()).to_arro3(py)
    }
}
