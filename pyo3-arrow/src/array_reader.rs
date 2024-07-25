use std::fmt::Display;

use arrow_schema::FieldRef;
use pyo3::exceptions::{PyIOError, PyValueError};
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyTuple, PyType};

use crate::error::PyArrowResult;
use crate::ffi::from_python::ffi_stream::ArrowArrayStreamReader;
use crate::ffi::from_python::utils::import_stream_pycapsule;
use crate::ffi::to_python::nanoarrow::to_nanoarrow_array_stream;
use crate::ffi::to_python::to_stream_pycapsule;
use crate::ffi::ArrayReader;
use crate::{PyChunkedArray, PyField};

/// A Python-facing Arrow array reader.
///
/// This is a wrapper around a [ArrayReader].
#[pyclass(module = "arro3.core._rust", name = "ArrayReader", subclass)]
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
        let array_reader = self
            .0
            .take()
            .ok_or(PyIOError::new_err("Cannot read from closed stream"))?;
        to_stream_pycapsule(py, array_reader, requested_schema)
    }

    pub fn __repr__(&self) -> String {
        self.to_string()
    }

    /// Returns `true` if this reader has already been consumed.
    pub fn closed(&self) -> bool {
        self.0.is_none()
    }

    /// Construct this from an existing Arrow object.
    ///
    /// It can be called on anything that exports the Arrow stream interface
    /// (`__arrow_c_stream__`), such as a `Table` or `ArrayReader`.
    #[classmethod]
    pub fn from_arrow(_cls: &Bound<PyType>, input: &Bound<PyAny>) -> PyResult<Self> {
        input.extract()
    }

    /// Construct this object from a bare Arrow PyCapsule.
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

    /// Access the field of this reader
    #[getter]
    pub fn field(&self, py: Python) -> PyResult<PyObject> {
        PyField::new(self.field_ref()?).to_arro3(py)
    }
}
