use crate::chunked::PyChunkedArray;
use crate::ffi::from_python::ffi_stream::ArrowArrayStreamReader;
use crate::ffi::from_python::utils::import_arrow_c_stream;
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::{PyAny, PyResult};

impl<'a> FromPyObject<'a> for PyChunkedArray {
    fn extract(ob: &'a PyAny) -> PyResult<Self> {
        let stream = import_arrow_c_stream(ob)?;
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
