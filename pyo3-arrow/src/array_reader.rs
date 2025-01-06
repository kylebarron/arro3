use std::fmt::Display;
use std::sync::Mutex;

use arrow_schema::FieldRef;
use pyo3::exceptions::{PyIOError, PyStopIteration, PyValueError};
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyTuple, PyType};

use crate::error::PyArrowResult;
use crate::export::{Arro3Array, Arro3ChunkedArray, Arro3Field};
use crate::ffi::from_python::ffi_stream::ArrowArrayStreamReader;
use crate::ffi::from_python::utils::import_stream_pycapsule;
use crate::ffi::to_python::nanoarrow::to_nanoarrow_array_stream;
use crate::ffi::to_python::to_stream_pycapsule;
use crate::ffi::{to_schema_pycapsule, ArrayIterator, ArrayReader};
use crate::input::AnyArray;
use crate::{PyArray, PyChunkedArray, PyField};

/// A Python-facing Arrow array reader.
///
/// This is a wrapper around a [ArrayReader].
#[pyclass(module = "arro3.core._core", name = "ArrayReader", subclass, frozen)]
pub struct PyArrayReader(pub(crate) Mutex<Option<Box<dyn ArrayReader + Send>>>);

impl PyArrayReader {
    /// Construct a new [PyArrayReader] from an existing [ArrayReader].
    pub fn new(reader: Box<dyn ArrayReader + Send>) -> Self {
        Self(Mutex::new(Some(reader)))
    }

    /// Import from a raw Arrow C Stream capsule
    pub fn from_arrow_pycapsule(capsule: &Bound<PyCapsule>) -> PyResult<Self> {
        let stream = import_stream_pycapsule(capsule)?;
        let stream_reader = ArrowArrayStreamReader::try_new(stream)
            .map_err(|err| PyValueError::new_err(err.to_string()))?;
        Ok(Self::new(Box::new(stream_reader)))
    }

    /// Consume this reader and convert into a [ArrayReader].
    ///
    /// The reader can only be consumed once. Calling `into_reader`
    pub fn into_reader(self) -> PyResult<Box<dyn ArrayReader + Send>> {
        let stream = self
            .0
            .lock()
            .unwrap()
            .take()
            .ok_or(PyIOError::new_err("Cannot write from closed stream."))?;
        Ok(stream)
    }

    /// Consume this reader and create a [PyChunkedArray] object
    pub fn into_chunked_array(self) -> PyArrowResult<PyChunkedArray> {
        let stream = self
            .0
            .lock()
            .unwrap()
            .take()
            .ok_or(PyIOError::new_err("Cannot write from closed stream."))?;
        let field = stream.field();
        let mut arrays = vec![];
        for array in stream {
            arrays.push(array?);
        }
        Ok(PyChunkedArray::try_new(arrays, field)?)
    }

    /// Access the [FieldRef] of this ArrayReader.
    ///
    /// If the stream has already been consumed, this method will error.
    pub fn field_ref(&self) -> PyResult<FieldRef> {
        let inner = self.0.lock().unwrap();
        let stream = inner
            .as_ref()
            .ok_or(PyIOError::new_err("Stream already closed."))?;
        Ok(stream.field())
    }

    /// Export this to a Python `arro3.core.ArrayReader`.
    #[allow(clippy::wrong_self_convention)]
    pub fn to_arro3<'py>(&'py self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let arro3_mod = py.import(intern!(py, "arro3.core"))?;
        arro3_mod.getattr(intern!(py, "ArrayReader"))?.call_method1(
            intern!(py, "from_arrow_pycapsule"),
            PyTuple::new(py, vec![self.__arrow_c_stream__(py, None)?])?,
        )
    }

    /// Export this to a Python `nanoarrow.ArrayStream`.
    #[allow(clippy::wrong_self_convention)]
    pub fn to_nanoarrow<'py>(&'py self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        to_nanoarrow_array_stream(py, &self.__arrow_c_stream__(py, None)?)
    }
}

impl From<Box<dyn ArrayReader + Send>> for PyArrayReader {
    fn from(value: Box<dyn ArrayReader + Send>) -> Self {
        Self::new(value)
    }
}

impl Display for PyArrayReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "arro3.core.ArrayReader")?;
        writeln!(f, "-----------------------")?;
        if let Ok(field) = self.field_ref() {
            field.data_type().fmt(f)
        } else {
            writeln!(f, "Closed stream")
        }
    }
}

#[pymethods]
impl PyArrayReader {
    fn __arrow_c_schema__<'py>(&'py self, py: Python<'py>) -> PyArrowResult<Bound<'py, PyCapsule>> {
        to_schema_pycapsule(py, self.field_ref()?.as_ref())
    }

    #[pyo3(signature = (requested_schema=None))]
    fn __arrow_c_stream__<'py>(
        &'py self,
        py: Python<'py>,
        requested_schema: Option<Bound<'py, PyCapsule>>,
    ) -> PyArrowResult<Bound<'py, PyCapsule>> {
        let array_reader = self
            .0
            .lock()
            .unwrap()
            .take()
            .ok_or(PyIOError::new_err("Cannot read from closed stream"))?;
        to_stream_pycapsule(py, array_reader, requested_schema)
    }

    // Return self
    // https://stackoverflow.com/a/52056290
    fn __iter__<'py>(&'py self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.to_arro3(py)
    }

    fn __next__(&self) -> PyArrowResult<Arro3Array> {
        self.read_next_array()
    }

    fn __repr__(&self) -> String {
        self.to_string()
    }

    #[getter]
    fn closed(&self) -> bool {
        self.0.lock().unwrap().is_none()
    }

    #[classmethod]
    fn from_arrow(_cls: &Bound<PyType>, input: AnyArray) -> PyArrowResult<Self> {
        let reader = input.into_reader()?;
        Ok(Self::new(reader))
    }

    #[classmethod]
    #[pyo3(name = "from_arrow_pycapsule")]
    fn from_arrow_pycapsule_py(_cls: &Bound<PyType>, capsule: &Bound<PyCapsule>) -> PyResult<Self> {
        Self::from_arrow_pycapsule(capsule)
    }

    #[classmethod]
    fn from_arrays(_cls: &Bound<PyType>, field: PyField, arrays: Vec<PyArray>) -> Self {
        let arrays = arrays
            .into_iter()
            .map(|array| {
                let (arr, _field) = array.into_inner();
                arr
            })
            .collect::<Vec<_>>();
        Self::new(Box::new(ArrayIterator::new(
            arrays.into_iter().map(Ok),
            field.into_inner(),
        )))
    }

    #[classmethod]
    fn from_stream(_cls: &Bound<PyType>, data: &Bound<PyAny>) -> PyResult<Self> {
        data.extract()
    }

    #[getter]
    fn field(&self) -> PyResult<Arro3Field> {
        Ok(PyField::new(self.field_ref()?).into())
    }

    fn read_all(&self) -> PyArrowResult<Arro3ChunkedArray> {
        let stream = self
            .0
            .lock()
            .unwrap()
            .take()
            .ok_or(PyIOError::new_err("Cannot read from closed stream."))?;
        let field = stream.field();
        let mut arrays = vec![];
        for array in stream {
            arrays.push(array?);
        }
        Ok(PyChunkedArray::try_new(arrays, field)?.into())
    }

    fn read_next_array(&self) -> PyArrowResult<Arro3Array> {
        let mut inner = self.0.lock().unwrap();
        let stream = inner
            .as_mut()
            .ok_or(PyIOError::new_err("Cannot read from closed stream."))?;

        if let Some(next_batch) = stream.next() {
            Ok(PyArray::new(next_batch?, stream.field()).into())
        } else {
            Err(PyStopIteration::new_err("").into())
        }
    }
}
