use std::sync::Arc;

use bytes::{Buf, Bytes};
use futures::future::{BoxFuture, FutureExt};
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::errors::ParquetError;
use parquet::file::footer::{decode_footer, decode_metadata};
use parquet::file::FOOTER_SIZE;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::{PyList, PyTuple};

/// A wrapper around an Async fsspec filesystem instance
pub struct AsyncFsspec {
    fs: PyObject,
    path: String,
    file_length: usize,
}

impl AsyncFsspec {
    pub fn new(fs: PyObject, path: String, file_length: usize) -> Self {
        Self {
            fs,
            path,
            file_length,
        }
    }
}

impl AsyncFileReader for AsyncFsspec {
    fn get_bytes(
        &mut self,
        range: std::ops::Range<usize>,
    ) -> BoxFuture<'_, parquet::errors::Result<bytes::Bytes>> {
        async move {
            let py_bytes_list = Python::with_gil(|py| -> PyResult<_> {
                let paths = PyList::new_bound(py, vec![self.path.as_str()]);

                let args = PyTuple::new_bound(
                    py,
                    vec![
                        paths.to_object(py),
                        range.start.into_py(py),
                        range.end.into_py(py),
                    ],
                );

                let coroutine =
                    self.fs
                        .call_method_bound(py, intern!(py, "_cat_ranges"), args, None)?;
                pyo3_asyncio_0_21::tokio::into_future(coroutine.bind(py).clone())
            })
            .map_err(|err| ParquetError::External(Box::new(err)))?
            .await
            .map_err(|err| ParquetError::External(Box::new(err)))?;

            let mut buffers = Python::with_gil(|py| py_bytes_list.extract::<Vec<Vec<u8>>>(py))
                .map_err(|err| ParquetError::External(Box::new(err)))?;

            assert_eq!(buffers.len(), 1);
            let buffer = buffers.remove(0);
            Ok(Bytes::from(buffer))
        }
        .boxed()
    }

    fn get_metadata(
        &mut self,
    ) -> BoxFuture<
        '_,
        parquet::errors::Result<std::sync::Arc<parquet::file::metadata::ParquetMetaData>>,
    > {
        async move {
            let mut buf = [0_u8; FOOTER_SIZE];
            let footer_size_start_range = self.file_length - FOOTER_SIZE;

            self.get_bytes(footer_size_start_range..self.file_length)
                .await?
                .copy_to_slice(&mut buf);

            let metadata_len = decode_footer(&buf)?;

            let metadata_start_range = self.file_length - FOOTER_SIZE - metadata_len;
            let metadata_end_range = self.file_length - FOOTER_SIZE;

            let metadata_bytes = self
                .get_bytes(metadata_start_range..metadata_end_range)
                .await?;

            Ok(Arc::new(decode_metadata(&metadata_bytes)?))
        }
        .boxed()
    }
}
