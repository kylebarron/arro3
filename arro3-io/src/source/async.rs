use std::sync::Arc;

use bytes::Bytes;
use futures::future::BoxFuture;
use parquet::arrow::async_reader::ParquetObjectReader;
use parquet::file::metadata::ParquetMetaData;
use pyo3::prelude::*;

struct PyObspecReader(PyObject);

impl Clone for PyObspecReader {
    fn clone(&self) -> Self {
        Self(Python::with_gil(|py| self.0.clone_ref(py)))
    }
}

impl parquet::arrow::async_reader::AsyncFileReader for PyObspecReader {
    fn get_bytes(
        &mut self,
        range: std::ops::Range<usize>,
    ) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        todo!()
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<std::ops::Range<usize>>,
    ) -> BoxFuture<'_, parquet::errors::Result<Vec<Bytes>>> {
        todo!()
    }

    fn get_metadata(&mut self) -> BoxFuture<'_, parquet::errors::Result<Arc<ParquetMetaData>>> {
        todo!()
    }
}

#[derive(Clone)]
pub(crate) enum AsyncReader {
    Python(PyObspecReader),
    ObjectStore(ParquetObjectReader),
}

impl parquet::arrow::async_reader::AsyncFileReader for AsyncReader {
    fn get_bytes(
        &mut self,
        range: std::ops::Range<usize>,
    ) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        match self {
            Self::Python(reader) => reader.get_bytes(range),
            Self::ObjectStore(reader) => reader.get_bytes(range),
        }
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<std::ops::Range<usize>>,
    ) -> BoxFuture<'_, parquet::errors::Result<Vec<Bytes>>> {
        match self {
            Self::Python(reader) => reader.get_byte_ranges(ranges),
            Self::ObjectStore(reader) => reader.get_byte_ranges(ranges),
        }
    }

    fn get_metadata(&mut self) -> BoxFuture<'_, parquet::errors::Result<Arc<ParquetMetaData>>> {
        match self {
            Self::Python(reader) => reader.get_metadata(),
            Self::ObjectStore(reader) => reader.get_metadata(),
        }
    }
}
