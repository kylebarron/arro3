use std::os::raw::c_int;

use arrow_buffer::Buffer;
use pyo3::ffi;
use pyo3::prelude::*;

/// A wrapper around an Arrow [Buffer].
///
/// The Python buffer protocol is implemented on this buffer to enable zero-copy data transfer of
/// the core buffer into Python. This allows for zero-copy data sharing with numpy via
/// `numpy.frombuffer`.
#[pyclass(module = "arro3.core._core", name = "Buffer", subclass)]
pub struct PyBuffer {
    pub(crate) inner: Buffer,
}

#[pymethods]

impl PyBuffer {
    /// new
    #[new]
    pub fn new(buf: Vec<u8>) -> Self {
        Self {
            inner: Buffer::from_vec(buf),
        }
    }

    /// This is taken from opendal:
    /// https://github.com/apache/opendal/blob/d001321b0f9834bc1e2e7d463bcfdc3683e968c9/bindings/python/src/utils.rs#L51-L72
    unsafe fn __getbuffer__(
        slf: PyRefMut<Self>,
        view: *mut ffi::Py_buffer,
        flags: c_int,
    ) -> PyResult<()> {
        let bytes = slf.inner.as_slice();
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
