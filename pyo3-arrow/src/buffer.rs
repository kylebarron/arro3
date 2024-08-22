use std::ffi::CStr;
use std::os::raw;
use std::os::raw::c_int;
use std::pin::Pin;
use std::{mem, slice};

use arrow_buffer::Buffer;
use pyo3::buffer::{Element, ReadOnlyCell};
use pyo3::exceptions::{PyBufferError, PyValueError};
use pyo3::ffi;
use pyo3::prelude::*;

/// A wrapper around an Arrow [Buffer].
///
/// The Python buffer protocol is implemented on this buffer to enable zero-copy data transfer of
/// the core buffer into Python. This allows for zero-copy data sharing with numpy via
/// `numpy.frombuffer`.
#[pyclass(module = "arro3.core._core", name = "Buffer", subclass)]
pub struct PyArrowBuffer {
    pub(crate) inner: Option<Buffer>,
}

#[pymethods]

impl PyArrowBuffer {
    /// new
    #[new]
    pub fn new(buf: Vec<u8>) -> Self {
        Self {
            inner: Some(Buffer::from_vec(buf)),
        }
    }

    /// This is taken from opendal:
    /// https://github.com/apache/opendal/blob/d001321b0f9834bc1e2e7d463bcfdc3683e968c9/bindings/python/src/utils.rs#L51-L72
    unsafe fn __getbuffer__(
        slf: PyRefMut<Self>,
        view: *mut ffi::Py_buffer,
        flags: c_int,
    ) -> PyResult<()> {
        if let Some(buf) = &slf.inner {
            let bytes = buf.as_slice();
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
        } else {
            Err(PyValueError::new_err("Buffer has already been disposed"))
        }
    }

    unsafe fn __releasebuffer__(mut slf: PyRefMut<Self>, _view: *mut ffi::Py_buffer) {
        slf.inner.take();
    }
}

pub struct PyAnyBuffer(Pin<Box<ffi::Py_buffer>>);

impl<'py> FromPyObject<'py> for PyAnyBuffer {
    fn extract_bound(obj: &Bound<'_, PyAny>) -> PyResult<PyAnyBuffer> {
        Self::get_bound(obj)
    }
}

#[allow(dead_code)]
impl PyAnyBuffer {
    /// Gets the underlying buffer from the specified python object.
    pub fn get_bound(obj: &Bound<'_, PyAny>) -> PyResult<PyAnyBuffer> {
        // TODO: use nightly API Box::new_uninit() once stable
        let mut buf = Box::new(mem::MaybeUninit::uninit());
        let buf: Box<ffi::Py_buffer> = {
            let ret = unsafe {
                ffi::PyObject_GetBuffer(obj.as_ptr(), buf.as_mut_ptr(), ffi::PyBUF_FULL_RO)
            };
            error_on_minusone(obj.py(), ret)?;
            // Safety: buf is initialized by PyObject_GetBuffer.
            // TODO: use nightly API Box::assume_init() once stable
            unsafe { mem::transmute(buf) }
        };
        // Create PyBuffer immediately so that if validation checks fail, the PyBuffer::drop code
        // will call PyBuffer_Release (thus avoiding any leaks).
        let buf = PyAnyBuffer(Pin::from(buf));

        if buf.0.shape.is_null() {
            Err(PyBufferError::new_err("shape is null"))
        } else if buf.0.strides.is_null() {
            Err(PyBufferError::new_err("strides is null"))
        } else {
            Ok(buf)
        }
    }

    /// Gets the pointer to the start of the buffer memory.
    ///
    /// Warning: the buffer memory might be mutated by other Python functions,
    /// and thus may only be accessed while the GIL is held.
    #[inline]
    pub fn buf_ptr(&self) -> *mut raw::c_void {
        self.0.buf
    }

    /// Gets whether the underlying buffer is read-only.
    #[inline]
    pub fn readonly(&self) -> bool {
        self.0.readonly != 0
    }

    /// Gets the size of a single element, in bytes.
    /// Important exception: when requesting an unformatted buffer, item_size still has the value
    #[inline]
    pub fn item_size(&self) -> usize {
        self.0.itemsize as usize
    }

    /// Gets the total number of items.
    #[inline]
    pub fn item_count(&self) -> usize {
        (self.0.len as usize) / (self.0.itemsize as usize)
    }

    /// `item_size() * item_count()`.
    /// For contiguous arrays, this is the length of the underlying memory block.
    /// For non-contiguous arrays, it is the length that the logical structure would have if it were copied to a contiguous representation.
    #[inline]
    pub fn len_bytes(&self) -> usize {
        self.0.len as usize
    }

    /// Gets the number of dimensions.
    ///
    /// May be 0 to indicate a single scalar value.
    #[inline]
    pub fn dimensions(&self) -> usize {
        self.0.ndim as usize
    }

    /// A NUL terminated string in struct module style syntax describing the contents of a single item.
    #[inline]
    pub fn format(&self) -> &CStr {
        if self.0.format.is_null() {
            CStr::from_bytes_with_nul(b"B\0").unwrap()
        } else {
            unsafe { CStr::from_ptr(self.0.format) }
        }
    }

    /// Gets whether the buffer is contiguous in C-style order (last index varies fastest when visiting items in order of memory address).
    #[inline]
    pub fn is_c_contiguous(&self) -> bool {
        unsafe {
            ffi::PyBuffer_IsContiguous(
                &*self.0 as *const ffi::Py_buffer,
                b'C' as std::os::raw::c_char,
            ) != 0
        }
    }

    /// Gets the buffer memory as a slice.
    ///
    /// This function succeeds if:
    /// * the buffer format is compatible with `T`
    /// * alignment and size of buffer elements is matching the expectations for type `T`
    /// * the buffer is C-style contiguous
    ///
    /// The returned slice uses type `Cell<T>` because it's theoretically possible for any call into the Python runtime
    /// to modify the values in the slice.
    pub fn as_slice<'a, T: Element>(
        &'a self,
        _py: Python<'a>,
    ) -> PyResult<Option<&'a [ReadOnlyCell<T>]>> {
        if mem::size_of::<T>() != self.item_size() || !T::is_compatible_format(self.format()) {
            return Err(PyBufferError::new_err(format!(
                "buffer contents are not compatible with {}",
                std::any::type_name::<T>()
            )));
        }

        if self.0.buf.align_offset(mem::align_of::<T>()) != 0 {
            return Err(PyBufferError::new_err(format!(
                "buffer contents are insufficiently aligned for {}",
                std::any::type_name::<T>()
            )));
        }

        if self.is_c_contiguous() {
            unsafe {
                Ok(Some(slice::from_raw_parts(
                    self.0.buf as *mut ReadOnlyCell<T>,
                    self.item_count(),
                )))
            }
        } else {
            Ok(None)
        }
    }
}

impl Drop for PyAnyBuffer {
    fn drop(&mut self) {
        Python::with_gil(|_| unsafe { ffi::PyBuffer_Release(&mut *self.0) });
    }
}

/// Returns Ok if the error code is not -1.
#[inline]
pub(crate) fn error_on_minusone(py: Python<'_>, result: i32) -> PyResult<()> {
    if result != -1 {
        Ok(())
    } else {
        Err(PyErr::fetch(py))
    }
}
