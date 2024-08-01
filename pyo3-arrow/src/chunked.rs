use std::fmt::Display;
use std::sync::Arc;

use arrow::compute::concat;
use arrow_array::{make_array, Array, ArrayRef};
use arrow_schema::{ArrowError, DataType, Field, FieldRef};
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyTuple, PyType};

use crate::error::{PyArrowError, PyArrowResult};
use crate::ffi::from_python::ffi_stream::ArrowArrayStreamReader;
use crate::ffi::from_python::utils::import_stream_pycapsule;
use crate::ffi::to_python::chunked::ArrayIterator;
use crate::ffi::to_python::nanoarrow::to_nanoarrow_array_stream;
use crate::ffi::to_python::to_stream_pycapsule;
use crate::input::AnyArray;
use crate::interop::numpy::to_numpy::chunked_to_numpy;
use crate::{PyArray, PyDataType, PyField};

/// A Python-facing Arrow chunked array.
///
/// This is a wrapper around a [FieldRef] and a `Vec` of [ArrayRef].
#[pyclass(module = "arro3.core._core", name = "ChunkedArray", subclass)]
pub struct PyChunkedArray {
    chunks: Vec<ArrayRef>,
    field: FieldRef,
}

impl PyChunkedArray {
    pub fn new(chunks: Vec<ArrayRef>, field: FieldRef) -> Self {
        assert!(
            chunks
                .iter()
                .all(|chunk| chunk.data_type().equals_datatype(field.data_type())),
            "All chunks must have same data type"
        );
        Self { chunks, field }
    }

    pub fn data_type(&self) -> &DataType {
        self.field.data_type()
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

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn len(&self) -> usize {
        self.chunks.iter().fold(0, |acc, arr| acc + arr.len())
    }

    pub fn rechunk(&self, chunk_lengths: Vec<usize>) -> PyArrowResult<Self> {
        let total_chunk_length = chunk_lengths.iter().sum::<usize>();
        if total_chunk_length != self.length() {
            return Err(PyValueError::new_err(
                "Chunk lengths do not add up to chunked array length",
            )
            .into());
        }

        // If the desired rechunking is the existing chunking, return early
        let matches_existing_chunking = chunk_lengths
            .iter()
            .zip(self.chunks())
            .all(|(length, arr)| *length == arr.len());
        if matches_existing_chunking {
            return Ok(Self::new(self.chunks.clone(), self.field.clone()));
        }

        let mut offset = 0;
        let chunks = chunk_lengths
            .iter()
            .map(|chunk_length| {
                let sliced_chunks = self.slice(offset, *chunk_length)?;
                let arr_refs = sliced_chunks.iter().map(|a| a.as_ref()).collect::<Vec<_>>();
                let sliced_concatted = concat(&arr_refs)?;
                offset += chunk_length;
                Ok(sliced_concatted)
            })
            .collect::<PyArrowResult<Vec<_>>>()?;

        Ok(PyChunkedArray::new(chunks, self.field.clone()))
    }

    pub fn slice(&self, mut offset: usize, mut length: usize) -> PyArrowResult<Vec<ArrayRef>> {
        if offset + length > self.length() {
            return Err(
                PyValueError::new_err("offset + length may not exceed length of array").into(),
            );
        }

        let mut sliced_chunks: Vec<ArrayRef> = vec![];
        for chunk in self.chunks() {
            if chunk.is_empty() {
                continue;
            }

            // If the offset is greater than the len of this chunk, don't include any rows from
            // this chunk
            if offset >= chunk.len() {
                offset -= chunk.len();
                continue;
            }

            let take_count = length.min(chunk.len());
            let sliced_chunk = chunk.slice(offset, take_count);
            sliced_chunks.push(sliced_chunk);

            length -= take_count;

            // If we've selected all rows, exit
            if length == 0 {
                break;
            } else {
                offset = 0;
            }
        }

        Ok(sliced_chunks)
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

impl Display for PyChunkedArray {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "arro3.core.ChunkedArray<")?;
        self.field.data_type().fmt(f)?;
        writeln!(f, ">")?;
        Ok(())
    }
}

#[pymethods]
impl PyChunkedArray {
    #[new]
    pub fn init(arrays: Vec<PyArray>, r#type: Option<PyField>) -> PyResult<Self> {
        let (chunks, fields): (Vec<_>, Vec<_>) =
            arrays.into_iter().map(|arr| arr.into_inner()).unzip();
        if !fields
            .windows(2)
            .all(|w| w[0].data_type().equals_datatype(w[1].data_type()))
        {
            return Err(PyTypeError::new_err(
                "Cannot create a ChunkedArray with differing data types.",
            ));
        }

        let field = r#type
            .map(|py_data_type| py_data_type.into_inner())
            .unwrap_or_else(|| fields[0].clone());

        Ok(PyChunkedArray::new(
            chunks,
            Field::new("", field.data_type().clone(), true)
                .with_metadata(field.metadata().clone())
                .into(),
        ))
    }

    /// An implementation of the Array interface, for interoperability with numpy and other
    /// array libraries.
    #[pyo3(signature = (dtype=None, copy=None))]
    #[allow(unused_variables)]
    pub fn __array__(
        &self,
        py: Python,
        dtype: Option<PyObject>,
        copy: Option<PyObject>,
    ) -> PyResult<PyObject> {
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
    pub fn __arrow_c_stream__<'py>(
        &'py self,
        py: Python<'py>,
        requested_schema: Option<Bound<PyCapsule>>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let array_reader = Box::new(ArrayIterator::new(
            self.chunks.clone().into_iter().map(Ok),
            self.field.clone(),
        ));
        to_stream_pycapsule(py, array_reader, requested_schema)
    }

    fn __eq__(&self, other: &PyChunkedArray) -> bool {
        self.field == other.field && self.chunks == other.chunks
    }

    fn __len__(&self) -> usize {
        self.chunks.iter().fold(0, |acc, x| acc + x.len())
    }

    pub fn __repr__(&self) -> String {
        self.to_string()
    }

    /// Construct this from an existing Arrow object.
    ///
    /// It can be called on anything that exports the Arrow stream interface
    /// (`__arrow_c_stream__`). All batches will be materialized in memory.
    #[classmethod]
    pub fn from_arrow(_cls: &Bound<PyType>, input: AnyArray) -> PyArrowResult<Self> {
        input.into_chunked_array()
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

    fn cast(&self, py: Python, target_type: PyDataType) -> PyArrowResult<PyObject> {
        let target_type = target_type.into_inner();
        let new_chunks = self
            .chunks
            .iter()
            .map(|chunk| arrow::compute::cast(&chunk, &target_type))
            .collect::<Result<Vec<_>, ArrowError>>()?;
        let new_field = self.field.as_ref().clone().with_data_type(target_type);
        Ok(PyChunkedArray::new(new_chunks, new_field.into()).to_arro3(py)?)
    }

    pub fn chunk(&self, py: Python, i: usize) -> PyResult<PyObject> {
        let field = self.field().clone();
        let array = self
            .chunks
            .get(i)
            .ok_or(PyValueError::new_err("out of index"))?
            .clone();
        PyArray::new(array, field).to_arro3(py)
    }

    #[getter]
    #[pyo3(name = "chunks")]
    pub fn chunks_py(&self, py: Python) -> PyResult<Vec<PyObject>> {
        let field = self.field().clone();
        self.chunks
            .iter()
            .map(|array| PyArray::new(array.clone(), field.clone()).to_arro3(py))
            .collect()
    }

    pub fn combine_chunks(&self, py: Python) -> PyArrowResult<PyObject> {
        let field = self.field().clone();
        let arrays: Vec<&dyn Array> = self.chunks.iter().map(|arr| arr.as_ref()).collect();
        Ok(PyArray::new(concat(&arrays)?, field).to_arro3(py)?)
    }

    pub fn equals(&self, other: PyChunkedArray) -> bool {
        self.field == other.field && self.chunks == other.chunks
    }

    fn length(&self) -> usize {
        self.len()
    }

    #[getter]
    fn nbytes(&self) -> usize {
        self.chunks
            .iter()
            .fold(0, |acc, batch| acc + batch.get_array_memory_size())
    }

    #[getter]
    pub fn null_count(&self) -> usize {
        self.chunks
            .iter()
            .fold(0, |acc, arr| acc + arr.null_count())
    }

    #[getter]
    pub fn num_chunks(&self) -> usize {
        self.chunks.len()
    }

    #[pyo3(signature = (offset=0, length=None))]
    #[pyo3(name = "slice")]
    pub fn slice_py(
        &self,
        py: Python,
        offset: usize,
        length: Option<usize>,
    ) -> PyArrowResult<PyObject> {
        let length = length.unwrap_or_else(|| self.len() - offset);
        let sliced_chunks = self.slice(offset, length)?;
        Ok(PyChunkedArray::new(sliced_chunks, self.field.clone()).to_arro3(py)?)
    }

    /// Copy this array to a `numpy` NDArray
    pub fn to_numpy(&self, py: Python) -> PyResult<PyObject> {
        self.__array__(py, None, None)
    }

    #[getter]
    pub fn r#type(&self, py: Python) -> PyResult<PyObject> {
        PyDataType::new(self.field.data_type().clone()).to_arro3(py)
    }
}
