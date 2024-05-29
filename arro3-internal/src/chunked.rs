use std::ffi::CString;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef};
use arrow_schema::FieldRef;
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyType};

use crate::ffi::from_python::ffi_stream::ArrowArrayStreamReader;
use crate::ffi::from_python::utils::import_stream_pycapsule;
use crate::ffi::to_python::chunked::ArrayIterator;
use crate::ffi::to_python::ffi_stream::new_stream;
use crate::interop::numpy::to_numpy::chunked_to_numpy;

// Note: we include the field so that we can round-trip extension types, which would otherwise lose
// their metadata.
#[pyclass(module = "arro3.core._rust", name = "ChunkedArray", subclass)]
pub struct PyChunkedArray {
    chunks: Vec<Arc<dyn Array>>,
    field: FieldRef,
}

impl PyChunkedArray {
    pub fn new(chunks: Vec<Arc<dyn Array>>, field: FieldRef) -> Self {
        Self { chunks, field }
    }

    pub fn into_inner(self) -> (Vec<ArrayRef>, FieldRef) {
        (self.chunks, self.field)
    }
}

#[pymethods]
impl PyChunkedArray {
    /// An implementation of the Array interface, for interoperability with numpy and other
    /// array libraries.
    pub fn __array__(&self, py: Python) -> PyResult<PyObject> {
        let chunk_refs = self
            .chunks
            .iter()
            .map(|arr| arr.as_ref())
            .collect::<Vec<_>>();
        chunked_to_numpy(py, chunk_refs.as_slice())
    }

    /// An implementation of the [Arrow PyCapsule
    /// Interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html).
    /// This dunder method should not be called directly, but enables zero-copy
    /// data transfer to other Python libraries that understand Arrow memory.
    ///
    /// For example (as of the upcoming pyarrow v16), you can call
    /// [`pyarrow.chunked_array()`][pyarrow.chunked_array] to convert this array into a
    /// pyarrow array, without copying memory.
    #[allow(unused_variables)]
    fn __arrow_c_stream__(&self, requested_schema: Option<PyObject>) -> PyResult<PyObject> {
        let field = self.field.clone();
        let chunks = self.chunks.clone();

        let array_reader = Box::new(ArrayIterator::new(chunks.into_iter().map(Ok), field));
        let ffi_stream = new_stream(array_reader);
        let stream_capsule_name = CString::new("arrow_array_stream").unwrap();

        Python::with_gil(|py| {
            let stream_capsule = PyCapsule::new(py, ffi_stream, Some(stream_capsule_name))?;
            Ok(stream_capsule.to_object(py))
        })
    }

    pub fn __eq__(&self, other: &PyChunkedArray) -> bool {
        self.field == other.field && self.chunks == other.chunks
    }

    pub fn __len__(&self) -> usize {
        self.chunks.iter().fold(0, |acc, x| acc + x.len())
    }

    /// Copy this array to a `numpy` NDArray
    pub fn to_numpy(&self, py: Python) -> PyResult<PyObject> {
        self.__array__(py)
    }

    #[classmethod]
    pub fn from_arrow(_cls: &PyType, input: &PyAny) -> PyResult<Self> {
        input.extract()
    }

    /// Construct this object from a bare Arrow PyCapsule
    #[classmethod]
    pub fn from_arrow_pycapsule(_cls: &PyType, capsule: &PyCapsule) -> PyResult<Self> {
        let stream = import_stream_pycapsule(capsule)?;

        let stream_reader = ArrowArrayStreamReader::try_new(stream)
            .map_err(|err| PyValueError::new_err(err.to_string()))?;

        let field = stream_reader.field();

        let mut chunks = vec![];
        for array in stream_reader {
            let array = array.map_err(|err| PyTypeError::new_err(err.to_string()))?;
            chunks.push(array);
        }

        Ok(PyChunkedArray::new(chunks, field))
    }
}
