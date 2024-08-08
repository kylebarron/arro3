use std::fmt::Display;

use arrow_schema::FieldRef;
use pyo3::exceptions::{PyIOError, PyStopIteration, PyValueError};
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyTuple, PyType};

use crate::error::PyArrowResult;
use crate::ffi::from_python::ffi_stream::ArrowArrayStreamReader;
use crate::ffi::from_python::utils::import_stream_pycapsule;
use crate::ffi::to_python::nanoarrow::to_nanoarrow_array_stream;
use crate::ffi::to_python::to_stream_pycapsule;
use crate::ffi::{ArrayIterator, ArrayReader};
use crate::input::AnyArray;
use crate::{PyArray, PyChunkedArray, PyField};

#[pyclass(module = "arro3.core._core", name = "ArrayReader", subclass)]
pub struct PyArrayReader(pub(crate) Option<Box<dyn ArrayReader + Send>>);

impl PyArrayReader {
    pub fn new(reader: Box<dyn ArrayReader + Send>) -> Self {
        Self(Some(reader))
    }

    /// Consume this reader and convert into a [ArrayReader].
    ///
    /// The reader can only be consumed once. Calling `into_reader`
    pub fn into_reader(mut self) -> PyResult<Box<dyn ArrayReader + Send>> {
        let stream = self
            .0
            .take()
            .ok_or(PyIOError::new_err("Cannot write from closed stream."))?;
        Ok(stream)
    }

    /// Consume this reader and create a [PyChunkedArray] object
    pub fn into_chunked_array(mut self) -> PyArrowResult<PyChunkedArray> {
        let stream = self
            .0
            .take()
            .ok_or(PyIOError::new_err("Cannot write from closed stream."))?;
        let field = stream.field();
        let mut arrays = vec![];
        for array in stream {
            arrays.push(array?);
        }
        Ok(PyChunkedArray::new(arrays, field))
    }

    /// Access the [FieldRef] of this ArrayReader.
    ///
    /// If the stream has already been consumed, this method will error.
    pub fn field_ref(&self) -> PyResult<FieldRef> {
        let stream = self
            .0
            .as_ref()
            .ok_or(PyIOError::new_err("Stream already closed."))?;
        Ok(stream.field())
    }

    /// Export this to a Python `arro3.core.ArrayReader`.
    #[allow(clippy::wrong_self_convention)]
    pub fn to_arro3(&mut self, py: Python) -> PyResult<PyObject> {
        let arro3_mod = py.import_bound(intern!(py, "arro3.core"))?;
        let core_obj = arro3_mod
            .getattr(intern!(py, "ArrayReader"))?
            .call_method1(
                intern!(py, "from_arrow_pycapsule"),
                PyTuple::new_bound(py, vec![self.__arrow_c_stream__(py, None)?]),
            )?;
        Ok(core_obj.to_object(py))
    }

    /// Export this to a Python `nanoarrow.ArrayStream`.
    #[allow(clippy::wrong_self_convention)]
    pub fn to_nanoarrow(&mut self, py: Python) -> PyResult<PyObject> {
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
    #[allow(unused_variables)]
    pub fn __arrow_c_stream__<'py>(
        &'py mut self,
        py: Python<'py>,
        requested_schema: Option<Bound<PyCapsule>>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let array_reader = self
            .0
            .take()
            .ok_or(PyIOError::new_err("Cannot read from closed stream"))?;
        to_stream_pycapsule(py, array_reader, requested_schema)
    }

    // Return self
    // https://stackoverflow.com/a/52056290
    fn __iter__(&mut self, py: Python) -> PyResult<PyObject> {
        self.to_arro3(py)
    }

    fn __next__(&mut self, py: Python) -> PyArrowResult<PyObject> {
        self.read_next_array(py)
    }

    pub fn __repr__(&self) -> String {
        self.to_string()
    }

    #[getter]
    pub fn closed(&self) -> bool {
        self.0.is_none()
    }

    #[classmethod]
    pub fn from_arrow(_cls: &Bound<PyType>, input: AnyArray) -> PyArrowResult<Self> {
        let reader = input.into_reader()?;
        Ok(Self::new(reader))
    }

    #[classmethod]
    pub fn from_arrow_pycapsule(
        _cls: &Bound<PyType>,
        capsule: &Bound<PyCapsule>,
    ) -> PyResult<Self> {
        let stream = import_stream_pycapsule(capsule)?;
        let stream_reader = ArrowArrayStreamReader::try_new(stream)
            .map_err(|err| PyValueError::new_err(err.to_string()))?;
        Ok(Self(Some(Box::new(stream_reader))))
    }

    #[classmethod]
    pub fn from_arrays(_cls: &Bound<PyType>, field: PyField, arrays: Vec<PyArray>) -> Self {
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
    pub fn from_stream(_cls: &Bound<PyType>, data: &Bound<PyAny>) -> PyResult<Self> {
        data.extract()
    }

    #[getter]
    pub fn field(&self, py: Python) -> PyResult<PyObject> {
        PyField::new(self.field_ref()?).to_arro3(py)
    }

    pub fn read_all(&mut self, py: Python) -> PyArrowResult<PyObject> {
        let stream = self
            .0
            .take()
            .ok_or(PyIOError::new_err("Cannot read from closed stream."))?;
        let field = stream.field();
        let mut arrays = vec![];
        for array in stream {
            arrays.push(array?);
        }
        Ok(PyChunkedArray::new(arrays, field).to_arro3(py)?)
    }

    pub fn read_next_array(&mut self, py: Python) -> PyArrowResult<PyObject> {
        let stream = self
            .0
            .as_mut()
            .ok_or(PyIOError::new_err("Cannot read from closed stream."))?;

        if let Some(next_batch) = stream.next() {
            Ok(PyArray::new(next_batch?, stream.field()).to_arro3(py)?)
        } else {
            Err(PyStopIteration::new_err("").into())
        }
    }
}
