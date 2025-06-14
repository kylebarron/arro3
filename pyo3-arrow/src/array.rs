use std::fmt::Display;
use std::sync::Arc;

use arrow_array::types::{
    Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type, UInt32Type,
    UInt64Type, UInt8Type,
};
use arrow_array::{
    Array, ArrayRef, BinaryArray, BinaryViewArray, BooleanArray, Datum, FixedSizeBinaryArray,
    LargeBinaryArray, LargeStringArray, PrimitiveArray, StringArray, StringViewArray,
};
use arrow_cast::cast;
use arrow_cast::display::ArrayFormatter;
use arrow_schema::{ArrowError, DataType, Field, FieldRef};
use arrow_select::concat::concat;
use arrow_select::take::take;
use numpy::PyUntypedArray;
use pyo3::exceptions::{PyIndexError, PyNotImplementedError, PyValueError};
use pyo3::prelude::*;
use pyo3::pybacked::{PyBackedBytes, PyBackedStr};
use pyo3::types::{PyCapsule, PyTuple, PyType};
use pyo3::{intern, IntoPyObjectExt};

#[cfg(feature = "buffer_protocol")]
use crate::buffer::AnyBufferProtocol;
use crate::error::PyArrowResult;
use crate::export::{Arro3Array, Arro3DataType, Arro3Field};
use crate::ffi::from_python::utils::import_array_pycapsules;
use crate::ffi::to_python::nanoarrow::to_nanoarrow_array;
use crate::ffi::{to_array_pycapsules, to_schema_pycapsule};
use crate::input::AnyArray;
use crate::interop::numpy::from_numpy::from_numpy;
use crate::interop::numpy::to_numpy::to_numpy;
use crate::scalar::PyScalar;
use crate::utils::default_repr_options;
use crate::{PyDataType, PyField};

/// A Python-facing Arrow array.
///
/// This is a wrapper around an [ArrayRef] and a [FieldRef].
///
/// It's important for this to wrap both an array _and_ a field so that it can faithfully store all
/// data transmitted via the `__arrow_c_array__` Python method, which [exports both an Array and a
/// Field](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html#arrow_c_array__).
/// In particular, storing a [FieldRef] is required to persist Arrow extension metadata through the
/// C Data Interface.
#[derive(Debug)]
#[pyclass(module = "arro3.core._core", name = "Array", subclass, frozen)]
pub struct PyArray {
    array: ArrayRef,
    field: FieldRef,
}

impl PyArray {
    /// Create a new Python Array from an [ArrayRef] and a [FieldRef].
    ///
    /// This will panic if the array's data type does not match the field's data type.
    pub fn new(array: ArrayRef, field: FieldRef) -> Self {
        Self::try_new(array, field).unwrap()
    }

    /// Create a new Python Array from an [ArrayRef] and a [FieldRef].
    pub fn try_new(array: ArrayRef, field: FieldRef) -> Result<Self, ArrowError> {
        // Note: if the array and field data types don't match, you'll get an obscure FFI
        // exception, because you might be describing a different array than you're actually
        // providing.
        if array.data_type() != field.data_type() {
            return Err(ArrowError::SchemaError(
                format!("Array DataType must match Field DataType. Array DataType is {}; field DataType is {}", array.data_type(), field.data_type())
            ));
        }
        Ok(Self { array, field })
    }

    /// Create a new PyArray from an [ArrayRef], inferring its data type automatically.
    pub fn from_array_ref(array: ArrayRef) -> Self {
        let field = Field::new("", array.data_type().clone(), true);
        Self::new(array, Arc::new(field))
    }

    /// Import from raw Arrow capsules
    pub fn from_arrow_pycapsule(
        schema_capsule: &Bound<PyCapsule>,
        array_capsule: &Bound<PyCapsule>,
    ) -> PyResult<Self> {
        let (array, field) = import_array_pycapsules(schema_capsule, array_capsule)?;
        Ok(Self::new(array, Arc::new(field)))
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
    pub fn to_arro3<'py>(&'py self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let arro3_mod = py.import(intern!(py, "arro3.core"))?;
        arro3_mod.getattr(intern!(py, "Array"))?.call_method1(
            intern!(py, "from_arrow_pycapsule"),
            self.__arrow_c_array__(py, None)?,
        )
    }

    /// Export to an arro3.core.Array.
    ///
    /// This requires that you depend on arro3-core from your Python package.
    pub fn into_arro3(self, py: Python) -> PyResult<Bound<PyAny>> {
        let arro3_mod = py.import(intern!(py, "arro3.core"))?;
        let array_capsules = to_array_pycapsules(py, self.field.clone(), &self.array, None)?;
        arro3_mod
            .getattr(intern!(py, "Array"))?
            .call_method1(intern!(py, "from_arrow_pycapsule"), array_capsules)
    }

    /// Export this to a Python `nanoarrow.Array`.
    pub fn to_nanoarrow<'py>(&'py self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        to_nanoarrow_array(py, self.__arrow_c_array__(py, None)?)
    }

    /// Export to a pyarrow.Array
    ///
    /// Requires pyarrow >=14
    pub fn to_pyarrow<'py>(&'py self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let pyarrow_mod = py.import(intern!(py, "pyarrow"))?;
        let cloned = Self::new(self.array.clone(), self.field.clone());
        pyarrow_mod
            .getattr(intern!(py, "array"))?
            .call1(PyTuple::new(py, vec![cloned.into_pyobject(py)?])?)
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

        let options = default_repr_options();
        let formatter =
            ArrayFormatter::try_new(self.array.as_ref(), &options).map_err(|_| std::fmt::Error)?;

        writeln!(f, "[")?;
        for i in 0..self.array.len().min(10) {
            let row = formatter.value(i);
            writeln!(f, "  {},", row.to_string())?;
        }
        writeln!(f, "]")?;

        Ok(())
    }
}

impl Datum for PyArray {
    fn get(&self) -> (&dyn Array, bool) {
        (self.array.as_ref(), false)
    }
}

#[pymethods]
impl PyArray {
    #[new]
    #[pyo3(signature = (obj, /, r#type = None, *))]
    pub(crate) fn init(obj: &Bound<PyAny>, r#type: Option<PyField>) -> PyArrowResult<Self> {
        if let Ok(data) = obj.extract::<PyArray>() {
            return Ok(data);
        }

        macro_rules! impl_primitive {
            ($rust_type:ty, $arrow_type:ty) => {{
                let values: Vec<Option<$rust_type>> = obj.extract()?;
                Arc::new(PrimitiveArray::<$arrow_type>::from(values))
            }};
        }

        let field = r#type
            .ok_or(PyValueError::new_err(
                "type must be passed for non-Arrow input",
            ))?
            .into_inner();
        let array: ArrayRef = match field.data_type() {
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
                let values: Vec<Option<bool>> = obj.extract()?;
                Arc::new(BooleanArray::from(values))
            }
            DataType::Binary => {
                let values: Vec<Option<PyBackedBytes>> = obj.extract()?;
                let slices = values
                    .iter()
                    .map(|maybe_vec| maybe_vec.as_ref().map(|vec| vec.as_ref()))
                    .collect::<Vec<_>>();
                Arc::new(BinaryArray::from(slices))
            }
            DataType::LargeBinary => {
                let values: Vec<Option<PyBackedBytes>> = obj.extract()?;
                let slices = values
                    .iter()
                    .map(|maybe_vec| maybe_vec.as_ref().map(|vec| vec.as_ref()))
                    .collect::<Vec<_>>();
                Arc::new(LargeBinaryArray::from(slices))
            }
            DataType::BinaryView => {
                let values: Vec<Option<PyBackedBytes>> = obj.extract()?;
                let slices = values
                    .iter()
                    .map(|maybe_vec| maybe_vec.as_ref().map(|vec| vec.as_ref()))
                    .collect::<Vec<_>>();
                Arc::new(BinaryViewArray::from(slices))
            }
            DataType::FixedSizeBinary(size) => {
                let values: Vec<Option<PyBackedBytes>> = obj.extract()?;
                Arc::new(FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                    values
                        .iter()
                        .map(|maybe_vec| maybe_vec.as_ref().map(|vec| vec.as_ref())),
                    *size,
                )?)
            }
            DataType::Utf8 => {
                let values: Vec<Option<PyBackedStr>> = obj.extract()?;
                let slices = values
                    .iter()
                    .map(|maybe_str| maybe_str.as_ref().map(|s| s.as_ref()))
                    .collect::<Vec<_>>();
                Arc::new(StringArray::from(slices))
            }
            DataType::LargeUtf8 => {
                let values: Vec<Option<PyBackedStr>> = obj.extract()?;
                let slices = values
                    .iter()
                    .map(|maybe_str| maybe_str.as_ref().map(|s| s.as_ref()))
                    .collect::<Vec<_>>();
                Arc::new(LargeStringArray::from(slices))
            }
            DataType::Utf8View => {
                let values: Vec<Option<PyBackedStr>> = obj.extract()?;
                let slices = values
                    .iter()
                    .map(|maybe_str| maybe_str.as_ref().map(|s| s.as_ref()))
                    .collect::<Vec<_>>();
                Arc::new(StringViewArray::from(slices))
            }
            dt => {
                return Err(PyNotImplementedError::new_err(format!(
                    "Array constructor for {dt} not yet implemented."
                ))
                .into())
            }
        };
        Ok(Self::new(array, field))
    }

    #[cfg(feature = "buffer_protocol")]
    fn buffer(&self) -> crate::buffer::PyArrowBuffer {
        use arrow_array::cast::AsArray;

        match self.array.data_type() {
            DataType::Int64 => {
                let arr = self.array.as_primitive::<Int64Type>();
                let values = arr.values();
                let buffer = values.inner().clone();
                crate::buffer::PyArrowBuffer::new(buffer)
            }
            _ => todo!(),
        }
    }

    #[pyo3(signature = (dtype=None, copy=None))]
    #[allow(unused_variables)]
    fn __array__<'py>(
        &'py self,
        py: Python<'py>,
        dtype: Option<Bound<'py, PyAny>>,
        copy: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        to_numpy(py, &self.array)
    }

    #[pyo3(signature = (requested_schema=None))]
    fn __arrow_c_array__<'py>(
        &'py self,
        py: Python<'py>,
        requested_schema: Option<Bound<'py, PyCapsule>>,
    ) -> PyArrowResult<Bound<'py, PyTuple>> {
        to_array_pycapsules(py, self.field.clone(), &self.array, requested_schema)
    }

    fn __arrow_c_schema__<'py>(&'py self, py: Python<'py>) -> PyArrowResult<Bound<'py, PyCapsule>> {
        to_schema_pycapsule(py, self.field.as_ref())
    }

    fn __eq__(&self, other: &PyArray) -> bool {
        self.array.as_ref() == other.array.as_ref() && self.field == other.field
    }

    fn __getitem__(&self, i: isize) -> PyArrowResult<PyScalar> {
        // Handle negative indexes from the end
        let i = if i < 0 {
            let i = self.array.len() as isize + i;
            if i < 0 {
                return Err(PyIndexError::new_err("Index out of range").into());
            }
            i as usize
        } else {
            i as usize
        };
        if i >= self.array.len() {
            return Err(PyIndexError::new_err("Index out of range").into());
        }
        PyScalar::try_new(self.array.slice(i, 1), self.field.clone())
    }

    fn __len__(&self) -> usize {
        self.array.len()
    }

    fn __repr__(&self) -> String {
        self.to_string()
    }

    #[classmethod]
    fn from_arrow(_cls: &Bound<PyType>, input: AnyArray) -> PyArrowResult<Self> {
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
    #[pyo3(name = "from_arrow_pycapsule")]
    fn from_arrow_pycapsule_py(
        _cls: &Bound<PyType>,
        schema_capsule: &Bound<PyCapsule>,
        array_capsule: &Bound<PyCapsule>,
    ) -> PyResult<Self> {
        Self::from_arrow_pycapsule(schema_capsule, array_capsule)
    }

    /// Import via buffer protocol
    #[cfg(feature = "buffer_protocol")]
    #[classmethod]
    fn from_buffer(_cls: &Bound<PyType>, buffer: AnyBufferProtocol) -> PyArrowResult<Self> {
        buffer.try_into()
    }

    #[classmethod]
    fn from_numpy(
        _cls: &Bound<PyType>,
        py: Python,
        array: Bound<'_, PyAny>,
    ) -> PyArrowResult<Self> {
        let mut numpy_array = array;
        if numpy_array.hasattr("__array__")? {
            numpy_array = numpy_array.call_method0("__array__")?;
        };

        // Prefer zero-copy route via buffer protocol, if possible
        #[cfg(feature = "buffer_protocol")]
        if let Ok(buf) = numpy_array.extract::<AnyBufferProtocol>() {
            return buf.try_into();
        }

        let numpy_array: Bound<PyUntypedArray> = FromPyObject::extract_bound(&numpy_array)?;
        let arrow_array = from_numpy(py, &numpy_array)?;
        Ok(Self::from_array_ref(arrow_array))
    }

    fn cast(&self, target_type: PyField) -> PyArrowResult<Arro3Array> {
        let new_field = target_type.into_inner();
        let new_array = cast(self.as_ref(), new_field.data_type())?;
        Ok(PyArray::new(new_array, new_field).into())
    }

    #[getter]
    #[pyo3(name = "field")]
    fn py_field(&self) -> Arro3Field {
        PyField::new(self.field.clone()).into()
    }

    #[getter]
    fn nbytes(&self) -> usize {
        self.array.get_array_memory_size()
    }

    #[getter]
    fn null_count(&self) -> usize {
        self.array.null_count()
    }

    #[pyo3(signature = (offset=0, length=None))]
    fn slice(&self, offset: usize, length: Option<usize>) -> Arro3Array {
        let length = length.unwrap_or_else(|| self.array.len() - offset);
        let new_array = self.array.slice(offset, length);
        PyArray::new(new_array, self.field().clone()).into()
    }

    fn take(&self, indices: PyArray) -> PyArrowResult<Arro3Array> {
        let new_array = take(self.as_ref(), indices.as_ref(), None)?;
        Ok(PyArray::new(new_array, self.field.clone()).into())
    }

    fn to_numpy<'py>(&'py self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.__array__(py, None, None)
    }

    fn to_pylist(&self, py: Python) -> PyResult<PyObject> {
        let mut scalars = Vec::with_capacity(self.array.len());
        for i in 0..self.array.len() {
            let scalar =
                unsafe { PyScalar::new_unchecked(self.array.slice(i, 1), self.field.clone()) };
            scalars.push(scalar.as_py(py)?);
        }
        scalars.into_py_any(py)
    }

    #[getter]
    fn r#type(&self) -> Arro3DataType {
        PyDataType::new(self.field.data_type().clone()).into()
    }
}
