use std::fmt::Display;
use std::sync::Arc;

use arrow::ffi::FFI_ArrowSchema;
use arrow::ffi_stream::FFI_ArrowArrayStream;
use arrow_array::{ArrayRef, RecordBatchIterator, RecordBatchReader, StructArray};
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef};
use pyo3::exceptions::{PyIOError, PyStopIteration, PyValueError};
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyTuple, PyType};

use crate::error::PyArrowResult;
use crate::ffi::from_python::utils::import_stream_pycapsule;
use crate::ffi::to_python::chunked::ArrayIterator;
use crate::ffi::to_python::nanoarrow::to_nanoarrow_array_stream;
use crate::ffi::to_python::to_stream_pycapsule;
use crate::ffi::to_schema_pycapsule;
use crate::input::AnyRecordBatch;
use crate::schema::display_schema;
use crate::{PyRecordBatch, PySchema, PyTable};

fn get_stream_schema(stream_ptr: *mut FFI_ArrowArrayStream) -> Result<SchemaRef, ArrowError> {
    dbg!("get_stream_schema");
    let mut schema = FFI_ArrowSchema::empty();

    let ret_code = unsafe { (*stream_ptr).get_schema.unwrap()(stream_ptr, &mut schema) };

    if ret_code == 0 {
        let schema = schema_try_from(&schema)?;
        dbg!("after try_from");
        dbg!(&schema);
        Ok(Arc::new(schema))
    } else {
        Err(ArrowError::CDataInterface(format!(
            "Cannot get schema from input stream. Error code: {ret_code:?}"
        )))
    }
}

fn schema_try_from(c_schema: &FFI_ArrowSchema) -> Result<Schema, ArrowError> {
    // interpret it as a struct type then extract its fields
    let dtype = DataType::try_from(c_schema)?;
    dbg!("hi");
    dbg!(c_schema.metadata().unwrap());
    if let DataType::Struct(fields) = dtype {
        Ok(Schema::new(fields).with_metadata(c_schema.metadata()?))
    } else {
        Err(ArrowError::CDataInterface(
            "Unable to interpret C data struct as a Schema".to_string(),
        ))
    }
}

fn check_c_schema_meta() {}

/// A Python-facing Arrow record batch reader.
///
/// This is a wrapper around a [RecordBatchReader].
#[pyclass(module = "arro3.core._core", name = "RecordBatchReader", subclass)]
pub struct PyRecordBatchReader(pub(crate) Option<Box<dyn RecordBatchReader + Send>>);

impl PyRecordBatchReader {
    /// Construct a new PyRecordBatchReader from an existing [RecordBatchReader].
    pub fn new(reader: Box<dyn RecordBatchReader + Send>) -> Self {
        dbg!(reader.schema());
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
        Ok(PyTable::try_new(batches, schema)?)
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
        dbg!("to_arro3");
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
    fn __arrow_c_schema__<'py>(&'py self, py: Python<'py>) -> PyArrowResult<Bound<'py, PyCapsule>> {
        to_schema_pycapsule(py, self.schema_ref()?.as_ref())
    }

    #[allow(unused_variables)]
    fn __arrow_c_stream__<'py>(
        &'py mut self,
        py: Python<'py>,
        requested_schema: Option<Bound<'py, PyCapsule>>,
    ) -> PyArrowResult<Bound<'py, PyCapsule>> {
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
            Field::new_struct("", schema.fields().clone(), false)
                .with_metadata(schema.metadata.clone())
                .into(),
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

    fn __repr__(&self) -> String {
        self.to_string()
    }

    #[classmethod]
    fn from_arrow(_cls: &Bound<PyType>, input: AnyRecordBatch) -> PyArrowResult<Self> {
        let reader = input.into_reader()?;
        Ok(Self::new(reader))
    }

    #[classmethod]
    pub(crate) fn from_arrow_pycapsule(
        _cls: &Bound<PyType>,
        capsule: &Bound<PyCapsule>,
    ) -> PyResult<Self> {
        dbg!("from_arrow_pycapsule");
        let mut stream = import_stream_pycapsule(capsule)?;
        dbg!("&stream");
        dbg!(&stream);

        let schema = get_stream_schema(&mut stream).unwrap();

        let stream_reader = arrow::ffi_stream::ArrowArrayStreamReader::try_new(stream)
            .map_err(|err| PyValueError::new_err(err.to_string()))?;
        dbg!("stream reader schema");
        dbg!(stream_reader.schema());

        Ok(Self(Some(Box::new(stream_reader))))
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
        self.0.is_none()
    }

    fn read_all(&mut self, py: Python) -> PyArrowResult<PyObject> {
        let stream = self
            .0
            .take()
            .ok_or(PyIOError::new_err("Cannot read from closed stream."))?;
        let schema = stream.schema();
        let mut batches = vec![];
        for batch in stream {
            batches.push(batch?);
        }
        Ok(PyTable::try_new(batches, schema)?.to_arro3(py)?)
    }

    fn read_next_batch(&mut self, py: Python) -> PyArrowResult<PyObject> {
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

    #[getter]
    fn schema(&self, py: Python) -> PyResult<PyObject> {
        PySchema::new(self.schema_ref()?.clone()).to_arro3(py)
    }
}
