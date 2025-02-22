use std::fmt::Display;
use std::sync::{Arc, Mutex};

use arrow_array::{ArrayRef, RecordBatchIterator, RecordBatchReader, StructArray};
use arrow_schema::{Field, SchemaRef};
use pyo3::exceptions::{PyIOError, PyStopIteration, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyTuple, PyType};
use pyo3::{intern, IntoPyObjectExt};

use crate::error::PyArrowResult;
use crate::export::{Arro3RecordBatch, Arro3Schema, Arro3Table};
use crate::ffi::from_python::utils::import_stream_pycapsule;
use crate::ffi::to_python::chunked::ArrayIterator;
use crate::ffi::to_python::nanoarrow::to_nanoarrow_array_stream;
use crate::ffi::to_python::to_stream_pycapsule;
use crate::ffi::to_schema_pycapsule;
use crate::input::AnyRecordBatch;
use crate::schema::display_schema;
use crate::{PyRecordBatch, PySchema, PyTable};

/// A Python-facing Arrow record batch reader.
///
/// This is a wrapper around a [RecordBatchReader].
#[pyclass(
    module = "arro3.core._core",
    name = "RecordBatchReader",
    subclass,
    frozen
)]
pub struct PyRecordBatchReader(pub(crate) Mutex<Option<Box<dyn RecordBatchReader + Send>>>);

impl PyRecordBatchReader {
    /// Construct a new PyRecordBatchReader from an existing [RecordBatchReader].
    pub fn new(reader: Box<dyn RecordBatchReader + Send>) -> Self {
        Self(Mutex::new(Some(reader)))
    }

    /// Construct from a raw Arrow C Stream capsule
    pub fn from_arrow_pycapsule(capsule: &Bound<PyCapsule>) -> PyResult<Self> {
        let stream = import_stream_pycapsule(capsule)?;
        let stream_reader = arrow::ffi_stream::ArrowArrayStreamReader::try_new(stream)
            .map_err(|err| PyValueError::new_err(err.to_string()))?;

        Ok(Self::new(Box::new(stream_reader)))
    }

    /// Consume this reader and convert into a [RecordBatchReader].
    ///
    /// The reader can only be consumed once. Calling `into_reader`
    pub fn into_reader(self) -> PyResult<Box<dyn RecordBatchReader + Send>> {
        let stream = self
            .0
            .lock()
            .unwrap()
            .take()
            .ok_or(PyIOError::new_err("Cannot read from closed stream."))?;
        Ok(stream)
    }

    /// Consume this reader and create a [PyTable] object
    pub fn into_table(self) -> PyArrowResult<PyTable> {
        let stream = self
            .0
            .lock()
            .unwrap()
            .take()
            .ok_or(PyIOError::new_err("Cannot read from closed stream."))?;
        let schema = stream.schema();
        let mut batches = vec![];
        for batch in stream {
            batches.push(batch?);
        }
        Ok(PyTable::try_new(batches, schema)?)
    }

    /// Access the [SchemaRef] of this RecordBatchReader.
    ///
    /// If the stream has already been consumed, this method will error.
    pub fn schema_ref(&self) -> PyResult<SchemaRef> {
        let inner = self.0.lock().unwrap();
        let stream = inner
            .as_ref()
            .ok_or(PyIOError::new_err("Stream already closed."))?;
        Ok(stream.schema())
    }

    /// Export this to a Python `arro3.core.RecordBatchReader`.
    pub fn to_arro3<'py>(&'py self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let arro3_mod = py.import(intern!(py, "arro3.core"))?;
        arro3_mod
            .getattr(intern!(py, "RecordBatchReader"))?
            .call_method1(
                intern!(py, "from_arrow_pycapsule"),
                PyTuple::new(py, vec![self.__arrow_c_stream__(py, None)?])?,
            )
    }

    /// Export this to a Python `arro3.core.RecordBatchReader`.
    pub fn into_arro3(self, py: Python) -> PyResult<Bound<PyAny>> {
        let arro3_mod = py.import(intern!(py, "arro3.core"))?;
        let reader = self
            .0
            .lock()
            .unwrap()
            .take()
            .ok_or(PyIOError::new_err("Cannot read from closed stream"))?;
        let capsule = Self::to_stream_pycapsule(py, reader, None)?;
        arro3_mod
            .getattr(intern!(py, "RecordBatchReader"))?
            .call_method1(
                intern!(py, "from_arrow_pycapsule"),
                PyTuple::new(py, vec![capsule])?,
            )
    }

    /// Export this to a Python `nanoarrow.ArrayStream`.
    pub fn to_nanoarrow<'py>(&'py self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        to_nanoarrow_array_stream(py, &self.__arrow_c_stream__(py, None)?)
    }

    /// Export to a pyarrow.RecordBatchReader
    ///
    /// Requires pyarrow >=15
    pub fn to_pyarrow(self, py: Python) -> PyResult<PyObject> {
        let pyarrow_mod = py.import(intern!(py, "pyarrow"))?;
        let record_batch_reader_class = pyarrow_mod.getattr(intern!(py, "RecordBatchReader"))?;
        let pyarrow_obj = record_batch_reader_class.call_method1(
            intern!(py, "from_stream"),
            PyTuple::new(py, vec![self.into_pyobject(py)?])?,
        )?;
        pyarrow_obj.into_py_any(py)
    }

    pub(crate) fn to_stream_pycapsule<'py>(
        py: Python<'py>,
        reader: Box<dyn RecordBatchReader + Send>,
        requested_schema: Option<Bound<'py, PyCapsule>>,
    ) -> PyArrowResult<Bound<'py, PyCapsule>> {
        let schema = reader.schema().clone();
        let array_reader = reader.into_iter().map(|maybe_batch| {
            let arr: ArrayRef = Arc::new(StructArray::from(maybe_batch?));
            Ok(arr)
        });
        let array_reader = Box::new(ArrayIterator::new(
            array_reader,
            Field::new_struct("", schema.fields().clone(), false)
                .with_metadata(schema.metadata.clone())
                .into(),
        ));
        to_stream_pycapsule(py, array_reader, requested_schema)
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
    fn __arrow_c_schema__<'py>(&'py self, py: Python<'py>) -> PyArrowResult<Bound<'py, PyCapsule>> {
        to_schema_pycapsule(py, self.schema_ref()?.as_ref())
    }

    #[pyo3(signature = (requested_schema=None))]
    fn __arrow_c_stream__<'py>(
        &'py self,
        py: Python<'py>,
        requested_schema: Option<Bound<'py, PyCapsule>>,
    ) -> PyArrowResult<Bound<'py, PyCapsule>> {
        let reader = self
            .0
            .lock()
            .unwrap()
            .take()
            .ok_or(PyIOError::new_err("Cannot read from closed stream"))?;
        Self::to_stream_pycapsule(py, reader, requested_schema)
    }

    // Return self
    // https://stackoverflow.com/a/52056290
    fn __iter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }

    fn __next__(&self) -> PyArrowResult<Arro3RecordBatch> {
        self.read_next_batch()
    }

    fn __repr__(&self) -> String {
        self.to_string()
    }

    #[classmethod]
    fn from_arrow(_cls: &Bound<PyType>, input: AnyRecordBatch) -> PyArrowResult<Self> {
        let reader = input.into_reader()?;
        Ok(Self::new(reader))
    }

    #[classmethod]
    #[pyo3(name = "from_arrow_pycapsule")]
    fn from_arrow_pycapsule_py(_cls: &Bound<PyType>, capsule: &Bound<PyCapsule>) -> PyResult<Self> {
        Self::from_arrow_pycapsule(capsule)
    }

    #[classmethod]
    fn from_batches(_cls: &Bound<PyType>, schema: PySchema, batches: Vec<PyRecordBatch>) -> Self {
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
    fn from_stream(_cls: &Bound<PyType>, data: &Bound<PyAny>) -> PyResult<Self> {
        data.extract()
    }

    #[getter]
    fn closed(&self) -> bool {
        self.0.lock().unwrap().is_none()
    }

    fn read_all(&self) -> PyArrowResult<Arro3Table> {
        let stream = self
            .0
            .lock()
            .unwrap()
            .take()
            .ok_or(PyIOError::new_err("Cannot read from closed stream."))?;
        let schema = stream.schema();
        let mut batches = vec![];
        for batch in stream {
            batches.push(batch?);
        }
        Ok(PyTable::try_new(batches, schema)?.into())
    }

    fn read_next_batch(&self) -> PyArrowResult<Arro3RecordBatch> {
        let mut inner = self.0.lock().unwrap();
        let stream = inner
            .as_mut()
            .ok_or(PyIOError::new_err("Cannot read from closed stream."))?;

        if let Some(next_batch) = stream.next() {
            Ok(next_batch?.into())
        } else {
            Err(PyStopIteration::new_err("").into())
        }
    }

    #[getter]
    fn schema(&self) -> PyResult<Arro3Schema> {
        Ok(PySchema::new(self.schema_ref()?.clone()).into())
    }
}
