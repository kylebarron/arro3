//! Support for Python buffer protocol

use std::ffi::CStr;
use std::os::raw;
use std::os::raw::c_int;
use std::ptr::NonNull;
use std::sync::Arc;

use arrow_array::builder::BooleanBuilder;
use arrow_array::{
    ArrayRef, FixedSizeListArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
    Int8Array, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow_buffer::{Buffer, ScalarBuffer};
use arrow_schema::Field;
use pyo3::buffer::{Element, ElementType, PyBuffer};
use pyo3::exceptions::PyValueError;
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use crate::error::{PyArrowError, PyArrowResult};
use crate::PyArray;

/// A wrapper around an Arrow [Buffer].
///
/// This implements both import and export via the Python buffer protocol.
///
/// ### Buffer import
///
/// This can be very useful as a general way to support ingest of a Python buffer protocol object.
/// The underlying Arrow [Buffer] manages the external memory, automatically calling the Python
/// buffer's release callback when the Arrow [Buffer] reference count reaches 0.
///
/// This does not need to be used with Arrow at all! This can be used with any API where you want
/// to handle both Python-provided and Rust-provided buffers. [`PyArrowBuffer`] implements
/// `AsRef<[u8]>`.
///
/// ### Buffer export
///
/// The Python buffer protocol is implemented on this buffer to enable zero-copy data transfer of
/// the core buffer into Python. This allows for zero-copy data sharing with numpy via
/// `numpy.frombuffer`.
#[pyclass(module = "arro3.core._core", name = "Buffer", subclass, frozen)]
pub struct PyArrowBuffer(Buffer);

impl AsRef<Buffer> for PyArrowBuffer {
    fn as_ref(&self) -> &Buffer {
        &self.0
    }
}

impl AsRef<[u8]> for PyArrowBuffer {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl PyArrowBuffer {
    /// Construct a new [PyArrowBuffer]
    pub fn new(buffer: Buffer) -> Self {
        Self(buffer)
    }

    /// Consume and return the [Buffer]
    pub fn into_inner(self) -> Buffer {
        self.0
    }
}

#[pymethods]
impl PyArrowBuffer {
    /// new
    #[new]
    fn py_new(buf: PyArrowBuffer) -> Self {
        buf
    }

    fn to_bytes<'py>(&'py self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.0)
    }

    fn __len__(&self) -> usize {
        self.0.len()
    }

    /// This is taken from opendal:
    /// https://github.com/apache/opendal/blob/d001321b0f9834bc1e2e7d463bcfdc3683e968c9/bindings/python/src/utils.rs#L51-L72
    unsafe fn __getbuffer__(
        slf: PyRef<Self>,
        view: *mut ffi::Py_buffer,
        flags: c_int,
    ) -> PyResult<()> {
        let bytes = slf.0.as_slice();
        let ret = ffi::PyBuffer_FillInfo(
            view,
            slf.as_ptr() as *mut _,
            bytes.as_ptr() as *mut _,
            bytes.len().try_into().unwrap(),
            1, // read only
            flags,
        );
        if ret == -1 {
            return Err(PyErr::fetch(slf.py()));
        }
        Ok(())
    }

    unsafe fn __releasebuffer__(&self, _view: *mut ffi::Py_buffer) {}
}

impl<'py> FromPyObject<'py> for PyArrowBuffer {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let buffer = ob.extract::<AnyBufferProtocol>()?;
        if !matches!(buffer, AnyBufferProtocol::UInt8(_)) {
            return Err(PyValueError::new_err("Expected u8 buffer protocol object"));
        }

        Ok(Self(buffer.into_arrow_buffer()?))
    }
}

/// A wrapper around a PyBuffer that applies a custom destructor that checks if the Python
/// interpreter is still initialized before freeing the buffer memory.
#[derive(Debug)]
pub struct PyBufferWrapper<T: Element>(Option<PyBuffer<T>>);

impl<T: Element> PyBufferWrapper<T> {
    fn inner(&self) -> PyResult<&PyBuffer<T>> {
        self.0
            .as_ref()
            .ok_or(PyValueError::new_err("Buffer already disposed"))
    }
}

impl<T: Element> Drop for PyBufferWrapper<T> {
    fn drop(&mut self) {
        // Only call the underlying Drop of PyBuffer if the Python interpreter is still
        // initialized. Sometimes the Drop can attempt to happen after the Python interpreter was
        // already finalized.
        // https://github.com/kylebarron/arro3/issues/230
        let is_initialized = unsafe { ffi::Py_IsInitialized() };
        if let Some(val) = self.0.take() {
            if is_initialized == 0 {
                std::mem::forget(val);
            } else {
                std::mem::drop(val);
            }
        }
    }
}

/// An enum over buffer protocol input types.
#[allow(missing_docs)]
#[derive(Debug)]
pub enum AnyBufferProtocol {
    UInt8(PyBufferWrapper<u8>),
    UInt16(PyBufferWrapper<u16>),
    UInt32(PyBufferWrapper<u32>),
    UInt64(PyBufferWrapper<u64>),
    Int8(PyBufferWrapper<i8>),
    Int16(PyBufferWrapper<i16>),
    Int32(PyBufferWrapper<i32>),
    Int64(PyBufferWrapper<i64>),
    Float32(PyBufferWrapper<f32>),
    Float64(PyBufferWrapper<f64>),
}

impl<'py> FromPyObject<'py> for AnyBufferProtocol {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        if let Ok(buf) = ob.extract::<PyBuffer<u8>>() {
            Ok(Self::UInt8(PyBufferWrapper(Some(buf))))
        } else if let Ok(buf) = ob.extract::<PyBuffer<u16>>() {
            Ok(Self::UInt16(PyBufferWrapper(Some(buf))))
        } else if let Ok(buf) = ob.extract::<PyBuffer<u32>>() {
            Ok(Self::UInt32(PyBufferWrapper(Some(buf))))
        } else if let Ok(buf) = ob.extract::<PyBuffer<u64>>() {
            Ok(Self::UInt64(PyBufferWrapper(Some(buf))))
        } else if let Ok(buf) = ob.extract::<PyBuffer<i8>>() {
            Ok(Self::Int8(PyBufferWrapper(Some(buf))))
        } else if let Ok(buf) = ob.extract::<PyBuffer<i16>>() {
            Ok(Self::Int16(PyBufferWrapper(Some(buf))))
        } else if let Ok(buf) = ob.extract::<PyBuffer<i32>>() {
            Ok(Self::Int32(PyBufferWrapper(Some(buf))))
        } else if let Ok(buf) = ob.extract::<PyBuffer<i64>>() {
            Ok(Self::Int64(PyBufferWrapper(Some(buf))))
        } else if let Ok(buf) = ob.extract::<PyBuffer<f32>>() {
            Ok(Self::Float32(PyBufferWrapper(Some(buf))))
        } else if let Ok(buf) = ob.extract::<PyBuffer<f64>>() {
            Ok(Self::Float64(PyBufferWrapper(Some(buf))))
        } else {
            Err(PyValueError::new_err("Not a buffer protocol object"))
        }
    }
}

impl AnyBufferProtocol {
    fn buf_ptr(&self) -> PyResult<*mut raw::c_void> {
        let out = match self {
            Self::UInt8(buf) => buf.inner()?.buf_ptr(),
            Self::UInt16(buf) => buf.inner()?.buf_ptr(),
            Self::UInt32(buf) => buf.inner()?.buf_ptr(),
            Self::UInt64(buf) => buf.inner()?.buf_ptr(),
            Self::Int8(buf) => buf.inner()?.buf_ptr(),
            Self::Int16(buf) => buf.inner()?.buf_ptr(),
            Self::Int32(buf) => buf.inner()?.buf_ptr(),
            Self::Int64(buf) => buf.inner()?.buf_ptr(),
            Self::Float32(buf) => buf.inner()?.buf_ptr(),
            Self::Float64(buf) => buf.inner()?.buf_ptr(),
        };
        Ok(out)
    }

    #[allow(dead_code)]
    fn dimensions(&self) -> PyResult<usize> {
        let out = match self {
            Self::UInt8(buf) => buf.inner()?.dimensions(),
            Self::UInt16(buf) => buf.inner()?.dimensions(),
            Self::UInt32(buf) => buf.inner()?.dimensions(),
            Self::UInt64(buf) => buf.inner()?.dimensions(),
            Self::Int8(buf) => buf.inner()?.dimensions(),
            Self::Int16(buf) => buf.inner()?.dimensions(),
            Self::Int32(buf) => buf.inner()?.dimensions(),
            Self::Int64(buf) => buf.inner()?.dimensions(),
            Self::Float32(buf) => buf.inner()?.dimensions(),
            Self::Float64(buf) => buf.inner()?.dimensions(),
        };
        Ok(out)
    }

    fn format(&self) -> PyResult<&CStr> {
        let out = match self {
            Self::UInt8(buf) => buf.inner()?.format(),
            Self::UInt16(buf) => buf.inner()?.format(),
            Self::UInt32(buf) => buf.inner()?.format(),
            Self::UInt64(buf) => buf.inner()?.format(),
            Self::Int8(buf) => buf.inner()?.format(),
            Self::Int16(buf) => buf.inner()?.format(),
            Self::Int32(buf) => buf.inner()?.format(),
            Self::Int64(buf) => buf.inner()?.format(),
            Self::Float32(buf) => buf.inner()?.format(),
            Self::Float64(buf) => buf.inner()?.format(),
        };
        Ok(out)
    }

    /// Consume this and convert to an Arrow [`ArrayRef`].
    ///
    /// For almost all buffer protocol objects this is zero-copy. Only boolean-typed buffers need
    /// to be copied, because boolean Python buffers are one _byte_ per element, while Arrow
    /// buffers are one _bit_ per element. All numeric buffers are zero-copy compatible.
    ///
    /// This uses [`Buffer::from_custom_allocation`][], which creates Arrow buffers from existing
    /// memory regions. The [`Buffer`] tracks ownership of the [`PyBuffer`] memory via reference
    /// counting. The [`PyBuffer`]'s release callback will be called when the Arrow [`Buffer`] sees
    /// that the `PyBuffer`'s reference count
    /// reaches zero.
    ///
    /// ## Safety
    ///
    /// - This assumes that the Python buffer is immutable. Immutability is not guaranteed by the
    ///   Python buffer protocol, so the end user must uphold this. Mutating a Python buffer could
    ///   lead to undefined behavior.
    // Note: in the future, maybe you should check item alignment as well?
    // https://github.com/PyO3/pyo3/blob/ce18f79d71f4d3eac54f55f7633cf08d2f57b64e/src/buffer.rs#L217-L221
    pub fn into_arrow_array(self) -> PyArrowResult<ArrayRef> {
        self.validate_buffer()?;

        let shape = self.shape()?.to_vec();

        // Handle multi dimensional arrays by wrapping in FixedSizeLists
        if shape.len() == 1 {
            self.into_arrow_values()
        } else {
            assert!(shape.len() > 1, "shape cannot be 0");

            let mut values = self.into_arrow_values()?;

            for size in shape[1..].iter().rev() {
                let field = Arc::new(Field::new("item", values.data_type().clone(), false));
                let x = FixedSizeListArray::new(field, (*size).try_into().unwrap(), values, None);
                values = Arc::new(x);
            }

            Ok(values)
        }
    }

    /// Convert the raw buffer to an [ArrayRef].
    ///
    /// In `into_arrow_array` the values will be wrapped in FixedSizeLists if needed for multi
    /// dimensional input.
    fn into_arrow_values(self) -> PyArrowResult<ArrayRef> {
        let len = self.item_count()?;
        let len_bytes = self.len_bytes()?;
        let ptr = NonNull::new(self.buf_ptr()? as _)
            .ok_or(PyValueError::new_err("Expected buffer ptr to be non null"))?;
        let element_type = ElementType::from_format(self.format()?);

        // TODO: couldn't get this macro to work with error
        // cannot find value `buf` in this scope
        //
        // macro_rules! impl_array {
        //     ($array_type:ty) => {
        //         let owner = Arc::new(buf);
        //         let buffer = unsafe { Buffer::from_custom_allocation(ptr, len_bytes, owner) };
        //         Ok(Arc::new(PrimitiveArray::<$array_type>::new(
        //             ScalarBuffer::new(buffer, 0, len),
        //             None,
        //         )))
        //     };
        // }

        match self {
            Self::UInt8(buf) => match element_type {
                ElementType::Bool => {
                    let slice = NonNull::slice_from_raw_parts(ptr, len);
                    let slice = unsafe { slice.as_ref() };
                    let mut builder = BooleanBuilder::with_capacity(len);
                    for val in slice {
                        builder.append_value(*val > 0);
                    }
                    Ok(Arc::new(builder.finish()))
                }
                ElementType::UnsignedInteger { bytes } => {
                    if bytes != 1 {
                        return Err(PyValueError::new_err(format!(
                            "Expected 1 byte element type, got {}",
                            bytes
                        ))
                        .into());
                    }

                    let owner = Arc::new(buf);
                    let buffer = unsafe { Buffer::from_custom_allocation(ptr, len_bytes, owner) };
                    Ok(Arc::new(UInt8Array::new(
                        ScalarBuffer::new(buffer, 0, len),
                        None,
                    )))
                }
                _ => Err(PyValueError::new_err(format!(
                    "Unexpected element type {:?}",
                    element_type
                ))
                .into()),
            },
            Self::UInt16(buf) => {
                let owner = Arc::new(buf);
                let buffer = unsafe { Buffer::from_custom_allocation(ptr, len_bytes, owner) };
                Ok(Arc::new(UInt16Array::new(
                    ScalarBuffer::new(buffer, 0, len),
                    None,
                )))
            }
            Self::UInt32(buf) => {
                let owner = Arc::new(buf);
                let buffer = unsafe { Buffer::from_custom_allocation(ptr, len_bytes, owner) };
                Ok(Arc::new(UInt32Array::new(
                    ScalarBuffer::new(buffer, 0, len),
                    None,
                )))
            }
            Self::UInt64(buf) => {
                let owner = Arc::new(buf);
                let buffer = unsafe { Buffer::from_custom_allocation(ptr, len_bytes, owner) };
                Ok(Arc::new(UInt64Array::new(
                    ScalarBuffer::new(buffer, 0, len),
                    None,
                )))
            }

            Self::Int8(buf) => {
                let owner = Arc::new(buf);
                let buffer = unsafe { Buffer::from_custom_allocation(ptr, len_bytes, owner) };
                Ok(Arc::new(Int8Array::new(
                    ScalarBuffer::new(buffer, 0, len),
                    None,
                )))
            }
            Self::Int16(buf) => {
                let owner = Arc::new(buf);
                let buffer = unsafe { Buffer::from_custom_allocation(ptr, len_bytes, owner) };
                Ok(Arc::new(Int16Array::new(
                    ScalarBuffer::new(buffer, 0, len),
                    None,
                )))
            }
            Self::Int32(buf) => {
                let owner = Arc::new(buf);
                let buffer = unsafe { Buffer::from_custom_allocation(ptr, len_bytes, owner) };
                Ok(Arc::new(Int32Array::new(
                    ScalarBuffer::new(buffer, 0, len),
                    None,
                )))
            }
            Self::Int64(buf) => {
                let owner = Arc::new(buf);
                let buffer = unsafe { Buffer::from_custom_allocation(ptr, len_bytes, owner) };
                Ok(Arc::new(Int64Array::new(
                    ScalarBuffer::new(buffer, 0, len),
                    None,
                )))
            }
            Self::Float32(buf) => {
                let owner = Arc::new(buf);
                let buffer = unsafe { Buffer::from_custom_allocation(ptr, len_bytes, owner) };
                Ok(Arc::new(Float32Array::new(
                    ScalarBuffer::new(buffer, 0, len),
                    None,
                )))
            }
            Self::Float64(buf) => {
                let owner = Arc::new(buf);
                let buffer = unsafe { Buffer::from_custom_allocation(ptr, len_bytes, owner) };
                Ok(Arc::new(Float64Array::new(
                    ScalarBuffer::new(buffer, 0, len),
                    None,
                )))
            }
        }
    }

    /// Consume this buffer protocol object and convert to an Arrow [Buffer].
    pub fn into_arrow_buffer(self) -> PyArrowResult<Buffer> {
        let len_bytes = self.len_bytes()?;
        let ptr = NonNull::new(self.buf_ptr()? as _)
            .ok_or(PyValueError::new_err("Expected buffer ptr to be non null"))?;

        let buffer = match self {
            Self::UInt8(buf) => {
                let owner = Arc::new(buf);
                unsafe { Buffer::from_custom_allocation(ptr, len_bytes, owner) }
            }
            Self::UInt16(buf) => {
                let owner = Arc::new(buf);
                unsafe { Buffer::from_custom_allocation(ptr, len_bytes, owner) }
            }
            Self::UInt32(buf) => {
                let owner = Arc::new(buf);
                unsafe { Buffer::from_custom_allocation(ptr, len_bytes, owner) }
            }
            Self::UInt64(buf) => {
                let owner = Arc::new(buf);
                unsafe { Buffer::from_custom_allocation(ptr, len_bytes, owner) }
            }
            Self::Int8(buf) => {
                let owner = Arc::new(buf);
                unsafe { Buffer::from_custom_allocation(ptr, len_bytes, owner) }
            }
            Self::Int16(buf) => {
                let owner = Arc::new(buf);
                unsafe { Buffer::from_custom_allocation(ptr, len_bytes, owner) }
            }
            Self::Int32(buf) => {
                let owner = Arc::new(buf);
                unsafe { Buffer::from_custom_allocation(ptr, len_bytes, owner) }
            }
            Self::Int64(buf) => {
                let owner = Arc::new(buf);
                unsafe { Buffer::from_custom_allocation(ptr, len_bytes, owner) }
            }
            Self::Float32(buf) => {
                let owner = Arc::new(buf);
                unsafe { Buffer::from_custom_allocation(ptr, len_bytes, owner) }
            }
            Self::Float64(buf) => {
                let owner = Arc::new(buf);
                unsafe { Buffer::from_custom_allocation(ptr, len_bytes, owner) }
            }
        };
        Ok(buffer)
    }

    fn item_count(&self) -> PyResult<usize> {
        let out = match self {
            Self::UInt8(buf) => buf.inner()?.item_count(),
            Self::UInt16(buf) => buf.inner()?.item_count(),
            Self::UInt32(buf) => buf.inner()?.item_count(),
            Self::UInt64(buf) => buf.inner()?.item_count(),
            Self::Int8(buf) => buf.inner()?.item_count(),
            Self::Int16(buf) => buf.inner()?.item_count(),
            Self::Int32(buf) => buf.inner()?.item_count(),
            Self::Int64(buf) => buf.inner()?.item_count(),
            Self::Float32(buf) => buf.inner()?.item_count(),
            Self::Float64(buf) => buf.inner()?.item_count(),
        };
        Ok(out)
    }

    fn item_size(&self) -> PyResult<usize> {
        let out = match self {
            Self::UInt8(buf) => buf.inner()?.item_size(),
            Self::UInt16(buf) => buf.inner()?.item_size(),
            Self::UInt32(buf) => buf.inner()?.item_size(),
            Self::UInt64(buf) => buf.inner()?.item_size(),
            Self::Int8(buf) => buf.inner()?.item_size(),
            Self::Int16(buf) => buf.inner()?.item_size(),
            Self::Int32(buf) => buf.inner()?.item_size(),
            Self::Int64(buf) => buf.inner()?.item_size(),
            Self::Float32(buf) => buf.inner()?.item_size(),
            Self::Float64(buf) => buf.inner()?.item_size(),
        };
        Ok(out)
    }

    fn is_c_contiguous(&self) -> PyResult<bool> {
        let out = match self {
            Self::UInt8(buf) => buf.inner()?.is_c_contiguous(),
            Self::UInt16(buf) => buf.inner()?.is_c_contiguous(),
            Self::UInt32(buf) => buf.inner()?.is_c_contiguous(),
            Self::UInt64(buf) => buf.inner()?.is_c_contiguous(),
            Self::Int8(buf) => buf.inner()?.is_c_contiguous(),
            Self::Int16(buf) => buf.inner()?.is_c_contiguous(),
            Self::Int32(buf) => buf.inner()?.is_c_contiguous(),
            Self::Int64(buf) => buf.inner()?.is_c_contiguous(),
            Self::Float32(buf) => buf.inner()?.is_c_contiguous(),
            Self::Float64(buf) => buf.inner()?.is_c_contiguous(),
        };
        Ok(out)
    }

    fn len_bytes(&self) -> PyResult<usize> {
        let out = match self {
            Self::UInt8(buf) => buf.inner()?.len_bytes(),
            Self::UInt16(buf) => buf.inner()?.len_bytes(),
            Self::UInt32(buf) => buf.inner()?.len_bytes(),
            Self::UInt64(buf) => buf.inner()?.len_bytes(),
            Self::Int8(buf) => buf.inner()?.len_bytes(),
            Self::Int16(buf) => buf.inner()?.len_bytes(),
            Self::Int32(buf) => buf.inner()?.len_bytes(),
            Self::Int64(buf) => buf.inner()?.len_bytes(),
            Self::Float32(buf) => buf.inner()?.len_bytes(),
            Self::Float64(buf) => buf.inner()?.len_bytes(),
        };
        Ok(out)
    }

    fn shape(&self) -> PyResult<&[usize]> {
        let out = match self {
            Self::UInt8(buf) => buf.inner()?.shape(),
            Self::UInt16(buf) => buf.inner()?.shape(),
            Self::UInt32(buf) => buf.inner()?.shape(),
            Self::UInt64(buf) => buf.inner()?.shape(),
            Self::Int8(buf) => buf.inner()?.shape(),
            Self::Int16(buf) => buf.inner()?.shape(),
            Self::Int32(buf) => buf.inner()?.shape(),
            Self::Int64(buf) => buf.inner()?.shape(),
            Self::Float32(buf) => buf.inner()?.shape(),
            Self::Float64(buf) => buf.inner()?.shape(),
        };
        Ok(out)
    }

    fn strides(&self) -> PyResult<&[isize]> {
        let out = match self {
            Self::UInt8(buf) => buf.inner()?.strides(),
            Self::UInt16(buf) => buf.inner()?.strides(),
            Self::UInt32(buf) => buf.inner()?.strides(),
            Self::UInt64(buf) => buf.inner()?.strides(),
            Self::Int8(buf) => buf.inner()?.strides(),
            Self::Int16(buf) => buf.inner()?.strides(),
            Self::Int32(buf) => buf.inner()?.strides(),
            Self::Int64(buf) => buf.inner()?.strides(),
            Self::Float32(buf) => buf.inner()?.strides(),
            Self::Float64(buf) => buf.inner()?.strides(),
        };
        Ok(out)
    }

    fn validate_buffer(&self) -> PyArrowResult<()> {
        if !self.is_c_contiguous()? {
            return Err(PyValueError::new_err("Buffer is not C contiguous").into());
        }

        if self.shape()?.contains(&0) {
            return Err(
                PyValueError::new_err("0-length dimension not currently supported.").into(),
            );
        }

        let item_size = self.item_size()? as isize;
        if self.strides()?.iter().any(|s| *s != item_size) {
            return Err(PyValueError::new_err(format!(
                "strides other than the item size ({}) not supported, got: {:?} ",
                self.item_size()?,
                self.strides()?
            ))
            .into());
        }

        Ok(())
    }
}

impl TryFrom<AnyBufferProtocol> for PyArray {
    type Error = PyArrowError;

    fn try_from(value: AnyBufferProtocol) -> Result<Self, Self::Error> {
        let array = value.into_arrow_array()?;
        Ok(Self::from_array_ref(array))
    }
}
