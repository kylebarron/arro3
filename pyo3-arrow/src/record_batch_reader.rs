use std::ffi::CString;

use arrow::ffi_stream::FFI_ArrowArrayStream;
use arrow_array::RecordBatchReader;
use pyo3::exceptions::{PyIOError, PyValueError};
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyTuple, PyType};

use crate::error::PyArrowResult;
use crate::ffi::from_python::utils::import_stream_pycapsule;
use crate::PyTable;

/// A Python-facing Arrow record batch reader.
///
/// This is a wrapper around a [RecordBatchReader].
#[pyclass(module = "arro3.core._rust", name = "RecordBatchReader", subclass)]
pub struct PyRecordBatchReader(pub(crate) Option<Box<dyn RecordBatchReader + Send>>);

impl PyRecordBatchReader {
    /// Returns `true` if this reader has already been consumed.
    pub fn closed(&self) -> bool {
        self.0.is_none()
    }

    /// Consume this reader and convert into a [RecordBatchReader].
    ///
    /// The reader can only be consumed once. Calling `into_reader`
    pub fn into_reader(mut self) -> PyArrowResult<Box<dyn RecordBatchReader + Send>> {
        let stream = self
            .0
            .take()
            .ok_or(PyIOError::new_err("Cannot write from closed stream."))?;
        Ok(stream)
    }

    /// Consume this reader and create a [PyTable] object
    pub fn into_table(mut self) -> PyArrowResult<PyTable> {
        let stream = self
            .0
            .take()
            .ok_or(PyIOError::new_err("Cannot write from closed stream."))?;
        let schema = stream.schema();
        let mut batches = vec![];
        for batch in stream {
            batches.push(batch?);
        }
        Ok(PyTable::new(schema, batches))
    }

    /// Export this to a Python `arro3.core.RecordBatchReader`.
    pub fn to_python(&mut self, py: Python) -> PyArrowResult<PyObject> {
        let arro3_mod = py.import_bound(intern!(py, "arro3.core"))?;
        let core_obj = arro3_mod
            .getattr(intern!(py, "RecordBatchReader"))?
            .call_method1(
                intern!(py, "from_arrow_pycapsule"),
                PyTuple::new_bound(py, vec![self.__arrow_c_stream__(py, None)?]),
            )?;
        Ok(core_obj.to_object(py))
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
    fn __arrow_c_stream__<'py>(
        &'py mut self,
        py: Python<'py>,
        requested_schema: Option<PyObject>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let reader = self
            .0
            .take()
            .ok_or(PyIOError::new_err("Cannot read from closed stream"))?;

        let ffi_stream = FFI_ArrowArrayStream::new(reader);
        let stream_capsule_name = CString::new("arrow_array_stream").unwrap();
        PyCapsule::new_bound(py, ffi_stream, Some(stream_capsule_name))
    }

    /// Construct this from an existing Arrow object.
    ///
    /// It can be called on anything that exports the Arrow stream interface
    /// (`__arrow_c_stream__`), such as a `Table` or `RecordBatchReader`.
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
        let stream_reader = arrow::ffi_stream::ArrowArrayStreamReader::try_new(stream)
            .map_err(|err| PyValueError::new_err(err.to_string()))?;

        Ok(Self(Some(Box::new(stream_reader))))
    }
}
