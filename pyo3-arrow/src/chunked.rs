use std::ffi::CString;
use std::sync::Arc;

use arrow_array::{make_array, Array, ArrayRef};
use arrow_schema::{ArrowError, Field, FieldRef};
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyTuple, PyType};

use crate::error::{PyArrowError, PyArrowResult};
use crate::ffi::from_python::ffi_stream::ArrowArrayStreamReader;
use crate::ffi::from_python::utils::import_stream_pycapsule;
use crate::ffi::to_python::chunked::ArrayIterator;
use crate::ffi::to_python::ffi_stream::new_stream;
use crate::ffi::to_python::nanoarrow::to_nanoarrow_array_stream;
use crate::interop::numpy::to_numpy::chunked_to_numpy;

/// A Python-facing Arrow chunked array.
///
/// This is a wrapper around a [FieldRef] and a `Vec` of [ArrayRef].
#[pyclass(module = "arro3.core._rust", name = "ChunkedArray", subclass)]
pub struct PyChunkedArray {
    chunks: Vec<ArrayRef>,
    field: FieldRef,
}

impl PyChunkedArray {
    pub fn new(chunks: Vec<ArrayRef>, field: FieldRef) -> Self {
        Self { chunks, field }
    }

    pub fn from_arrays<A: Array>(chunks: &[A]) -> PyArrowResult<Self> {
        let arrays = chunks
            .iter()
            .map(|chunk| make_array(chunk.to_data()))
            .collect::<Vec<_>>();
        Self::from_array_refs(arrays)
    }

    /// Create a new PyChunkedArray from a vec of [ArrayRef]s, inferring their data type
    /// automatically.
    pub fn from_array_refs(chunks: Vec<ArrayRef>) -> PyArrowResult<Self> {
        if chunks.is_empty() {
            return Err(ArrowError::SchemaError(
                "Cannot infer data type from empty Vec<ArrayRef>".to_string(),
            )
            .into());
        }

        if !chunks
            .windows(2)
            .all(|w| w[0].data_type() == w[1].data_type())
        {
            return Err(ArrowError::SchemaError("Mismatched data types".to_string()).into());
        }

        let field = Field::new("", chunks.first().unwrap().data_type().clone(), true);
        Ok(Self::new(chunks, Arc::new(field)))
    }

    pub fn chunks(&self) -> &[ArrayRef] {
        &self.chunks
    }

    pub fn field(&self) -> &FieldRef {
        &self.field
    }

    pub fn into_inner(self) -> (Vec<ArrayRef>, FieldRef) {
        (self.chunks, self.field)
    }

    /// Export this to a Python `arro3.core.ChunkedArray`.
    pub fn to_arro3(&self, py: Python) -> PyResult<PyObject> {
        let arro3_mod = py.import_bound(intern!(py, "arro3.core"))?;
        let core_obj = arro3_mod
            .getattr(intern!(py, "ChunkedArray"))?
            .call_method1(
                intern!(py, "from_arrow_pycapsule"),
                PyTuple::new_bound(py, vec![self.__arrow_c_stream__(py, None)?]),
            )?;
        Ok(core_obj.to_object(py))
    }

    /// Export this to a Python `nanoarrow.ArrayStream`.
    pub fn to_nanoarrow(&self, py: Python) -> PyResult<PyObject> {
        to_nanoarrow_array_stream(py, &self.__arrow_c_stream__(py, None)?)
    }

    /// Export to a pyarrow.ChunkedArray
    ///
    /// Requires pyarrow >=14
    pub fn to_pyarrow(self, py: Python) -> PyResult<PyObject> {
        let pyarrow_mod = py.import_bound(intern!(py, "pyarrow"))?;
        let pyarrow_obj = pyarrow_mod
            .getattr(intern!(py, "chunked_array"))?
            .call1(PyTuple::new_bound(py, vec![self.into_py(py)]))?;
        Ok(pyarrow_obj.to_object(py))
    }
}

impl TryFrom<Vec<ArrayRef>> for PyChunkedArray {
    type Error = PyArrowError;

    fn try_from(value: Vec<ArrayRef>) -> Result<Self, Self::Error> {
        Self::from_array_refs(value)
    }
}

impl AsRef<[ArrayRef]> for PyChunkedArray {
    fn as_ref(&self) -> &[ArrayRef] {
        &self.chunks
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
    fn __arrow_c_stream__<'py>(
        &'py self,
        py: Python<'py>,
        requested_schema: Option<PyObject>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let field = self.field.clone();
        let chunks = self.chunks.clone();

        let array_reader = Box::new(ArrayIterator::new(chunks.into_iter().map(Ok), field));
        let ffi_stream = new_stream(array_reader);
        let stream_capsule_name = CString::new("arrow_array_stream").unwrap();
        PyCapsule::new_bound(py, ffi_stream, Some(stream_capsule_name))
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

    /// Construct this from an existing Arrow object.
    ///
    /// It can be called on anything that exports the Arrow stream interface
    /// (`__arrow_c_stream__`). All batches will be materialized in memory.
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
