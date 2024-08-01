use std::fmt::Display;
use std::sync::Arc;

use arrow::compute::concat;
use arrow::datatypes::{
    Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type, UInt32Type,
    UInt64Type, UInt8Type,
};
use arrow_array::{
    make_array, Array, ArrayRef, BinaryArray, BinaryViewArray, BooleanArray, LargeBinaryArray,
    LargeStringArray, PrimitiveArray, StringArray, StringViewArray,
};
use arrow_schema::{ArrowError, DataType, Field, FieldRef};
use numpy::PyUntypedArray;
use pyo3::exceptions::PyNotImplementedError;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyTuple, PyType};

use crate::error::PyArrowResult;
use crate::ffi::from_python::utils::import_array_pycapsules;
use crate::ffi::to_array_pycapsules;
use crate::ffi::to_python::nanoarrow::to_nanoarrow_array;
use crate::input::AnyArray;
use crate::interop::numpy::from_numpy::from_numpy;
use crate::interop::numpy::to_numpy::to_numpy;
use crate::{PyDataType, PyField};

/// A Python-facing Arrow array.
///
/// This is a wrapper around an [ArrayRef] and a [FieldRef].
#[pyclass(module = "arro3.core._core", name = "Array", subclass)]
pub struct PyArray {
    array: ArrayRef,
    field: FieldRef,
}

impl PyArray {
    /// Create a new Python Array from an [ArrayRef] and a [FieldRef].
    pub fn new(array: ArrayRef, field: FieldRef) -> Self {
        assert_eq!(array.data_type(), field.data_type());
        Self { array, field }
    }

    /// Create a new Python Array from an [ArrayRef] and a [FieldRef].
    pub fn try_new(array: ArrayRef, field: FieldRef) -> Result<Self, ArrowError> {
        // Note: if the array and field data types don't match, you'll get an obscure FFI
        // exception, because you might be describing a different array than you're actually
        // providing.
        if array.data_type() != field.data_type() {
            return Err(ArrowError::SchemaError(
                "Array DataType must match Field DataType".to_string(),
            ));
        }
        Ok(Self { array, field })
    }

    pub fn from_array<A: Array>(array: A) -> Self {
        let array = make_array(array.into_data());
        Self::from_array_ref(array)
    }

    /// Create a new PyArray from an [ArrayRef], inferring its data type automatically.
    pub fn from_array_ref(array: ArrayRef) -> Self {
        let field = Field::new("", array.data_type().clone(), true);
        Self::new(array, Arc::new(field))
    }

    /// Access the underlying [ArrayRef].
    pub fn array(&self) -> &ArrayRef {
        &self.array
    }

    /// Access the underlying [FieldRef].
    pub fn field(&self) -> &FieldRef {
        &self.field
    }

    /// Consume self to access the underlying [ArrayRef] and [FieldRef].
    pub fn into_inner(self) -> (ArrayRef, FieldRef) {
        (self.array, self.field)
    }

    /// Export to an arro3.core.Array.
    ///
    /// This requires that you depend on arro3-core from your Python package.
    pub fn to_arro3(&self, py: Python) -> PyResult<PyObject> {
        let arro3_mod = py.import_bound(intern!(py, "arro3.core"))?;
        let core_obj = arro3_mod.getattr(intern!(py, "Array"))?.call_method1(
            intern!(py, "from_arrow_pycapsule"),
            self.__arrow_c_array__(py, None)?,
        )?;
        Ok(core_obj.to_object(py))
    }

    /// Export this to a Python `nanoarrow.Array`.
    pub fn to_nanoarrow(&self, py: Python) -> PyResult<PyObject> {
        to_nanoarrow_array(py, &self.__arrow_c_array__(py, None)?)
    }

    /// Export to a pyarrow.Array
    ///
    /// Requires pyarrow >=14
    pub fn to_pyarrow(self, py: Python) -> PyResult<PyObject> {
        let pyarrow_mod = py.import_bound(intern!(py, "pyarrow"))?;
        let pyarrow_obj = pyarrow_mod
            .getattr(intern!(py, "array"))?
            .call1(PyTuple::new_bound(py, vec![self.into_py(py)]))?;
        Ok(pyarrow_obj.to_object(py))
    }
}

impl From<ArrayRef> for PyArray {
    fn from(value: ArrayRef) -> Self {
        Self::from_array_ref(value)
    }
}

impl AsRef<ArrayRef> for PyArray {
    fn as_ref(&self) -> &ArrayRef {
        &self.array
    }
}

impl Display for PyArray {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "arro3.core.Array<")?;
        self.array.data_type().fmt(f)?;
        writeln!(f, ">")?;
        Ok(())
    }
}

#[pymethods]
impl PyArray {
    #[new]
    #[pyo3(signature = (obj, /, r#type, *))]
    pub fn init(py: Python, obj: PyObject, r#type: PyDataType) -> PyResult<Self> {
        macro_rules! impl_primitive {
            ($rust_type:ty, $arrow_type:ty) => {{
                let values: Vec<$rust_type> = obj.extract(py)?;
                Arc::new(PrimitiveArray::<$arrow_type>::from(values))
            }};
        }

        let data_type = r#type.into_inner();
        let array: ArrayRef = match data_type {
            DataType::Float32 => impl_primitive!(f32, Float32Type),
            DataType::Float64 => impl_primitive!(f64, Float64Type),
            DataType::UInt8 => impl_primitive!(u8, UInt8Type),
            DataType::UInt16 => impl_primitive!(u16, UInt16Type),
            DataType::UInt32 => impl_primitive!(u32, UInt32Type),
            DataType::UInt64 => impl_primitive!(u64, UInt64Type),
            DataType::Int8 => impl_primitive!(i8, Int8Type),
            DataType::Int16 => impl_primitive!(i16, Int16Type),
            DataType::Int32 => impl_primitive!(i32, Int32Type),
            DataType::Int64 => impl_primitive!(i64, Int64Type),
            DataType::Boolean => {
                let values: Vec<bool> = obj.extract(py)?;
                Arc::new(BooleanArray::from(values))
            }
            DataType::Binary => {
                let values: Vec<Vec<u8>> = obj.extract(py)?;
                let slices = values.iter().map(|x| x.as_slice()).collect::<Vec<_>>();
                Arc::new(BinaryArray::from(slices))
            }
            DataType::LargeBinary => {
                let values: Vec<Vec<u8>> = obj.extract(py)?;
                let slices = values.iter().map(|x| x.as_slice()).collect::<Vec<_>>();
                Arc::new(LargeBinaryArray::from(slices))
            }
            DataType::BinaryView => {
                let values: Vec<Vec<u8>> = obj.extract(py)?;
                let slices = values.iter().map(|x| x.as_slice()).collect::<Vec<_>>();
                Arc::new(BinaryViewArray::from(slices))
            }
            DataType::Utf8 => {
                let values: Vec<String> = obj.extract(py)?;
                Arc::new(StringArray::from(values))
            }
            DataType::LargeUtf8 => {
                let values: Vec<String> = obj.extract(py)?;
                Arc::new(LargeStringArray::from(values))
            }
            DataType::Utf8View => {
                let values: Vec<String> = obj.extract(py)?;
                Arc::new(StringViewArray::from(values))
            }
            dt => {
                return Err(PyNotImplementedError::new_err(format!(
                    "Array constructor for {dt} not yet implemented."
                )))
            }
        };
        Ok(Self::new(array, Field::new("", data_type, true).into()))
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
        to_numpy(py, &self.array)
    }

    /// An implementation of the [Arrow PyCapsule
    /// Interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html).
    /// This dunder method should not be called directly, but enables zero-copy
    /// data transfer to other Python libraries that understand Arrow memory.
    ///
    /// For example, you can call [`pyarrow.array()`][pyarrow.array] to convert this array
    /// into a pyarrow array, without copying memory.
    #[allow(unused_variables)]
    pub fn __arrow_c_array__<'py>(
        &'py self,
        py: Python<'py>,
        requested_schema: Option<Bound<PyCapsule>>,
    ) -> PyArrowResult<Bound<PyTuple>> {
        to_array_pycapsules(py, self.field.clone(), &self.array, requested_schema)
    }

    pub fn __eq__(&self, other: &PyArray) -> bool {
        self.array.as_ref() == other.array.as_ref() && self.field == other.field
    }

    pub fn __len__(&self) -> usize {
        self.array.len()
    }

    pub fn __repr__(&self) -> String {
        self.to_string()
    }

    #[classmethod]
    pub fn from_arrow(_cls: &Bound<PyType>, input: AnyArray) -> PyArrowResult<Self> {
        match input {
            AnyArray::Array(array) => Ok(array),
            AnyArray::Stream(stream) => {
                let chunked_array = stream.into_chunked_array()?;
                let (chunks, field) = chunked_array.into_inner();
                let chunk_refs = chunks.iter().map(|arr| arr.as_ref()).collect::<Vec<_>>();
                let concatted = concat(chunk_refs.as_slice())?;
                Ok(Self::new(concatted, field))
            }
        }
    }

    #[classmethod]
    pub fn from_arrow_pycapsule(
        _cls: &Bound<PyType>,
        schema_capsule: &Bound<PyCapsule>,
        array_capsule: &Bound<PyCapsule>,
    ) -> PyResult<Self> {
        let (array, field) = import_array_pycapsules(schema_capsule, array_capsule)?;
        Ok(Self::new(array, Arc::new(field)))
    }

    #[classmethod]
    pub fn from_numpy(
        _cls: &Bound<PyType>,
        py: Python,
        array: Bound<'_, PyAny>,
    ) -> PyArrowResult<Self> {
        let mut numpy_array = array;
        if numpy_array.hasattr("__array__")? {
            numpy_array = numpy_array.call_method0("__array__")?;
        };
        let numpy_array: &PyUntypedArray = FromPyObject::extract_bound(&numpy_array)?;
        let arrow_array = from_numpy(py, numpy_array)?;
        Ok(Self::from_array_ref(arrow_array))
    }

    fn cast(&self, py: Python, target_type: PyDataType) -> PyArrowResult<PyObject> {
        let target_type = target_type.into_inner();
        let new_array = arrow::compute::cast(self.as_ref(), &target_type)?;
        let new_field = self.field.as_ref().clone().with_data_type(target_type);
        Ok(PyArray::new(new_array, new_field.into()).to_arro3(py)?)
    }

    #[getter]
    #[pyo3(name = "field")]
    fn py_field(&self, py: Python) -> PyResult<PyObject> {
        PyField::new(self.field.clone()).to_arro3(py)
    }

    #[getter]
    fn nbytes(&self) -> usize {
        self.array.get_array_memory_size()
    }

    #[pyo3(signature = (offset=0, length=None))]
    pub fn slice(&self, py: Python, offset: usize, length: Option<usize>) -> PyResult<PyObject> {
        let length = length.unwrap_or_else(|| self.array.len() - offset);
        let new_array = self.array.slice(offset, length);
        PyArray::new(new_array, self.field().clone()).to_arro3(py)
    }

    fn take(&self, py: Python, indices: PyArray) -> PyArrowResult<PyObject> {
        let new_array = arrow::compute::take(self.as_ref(), indices.as_ref(), None)?;
        Ok(PyArray::new(new_array, self.field.clone()).to_arro3(py)?)
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
