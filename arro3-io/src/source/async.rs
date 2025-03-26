use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use futures::future::BoxFuture;
use parquet::arrow::async_reader::ParquetObjectReader;
use parquet::file::metadata::ParquetMetaData;

// TODO: come back to this to implement for arbitrary async Python backends
// struct PyObspecReader(PyObject);

// impl Clone for PyObspecReader {
//     fn clone(&self) -> Self {
//         Self(Python::with_gil(|py| self.0.clone_ref(py)))
//     }
// }

// impl parquet::arrow::async_reader::AsyncFileReader for PyObspecReader {
//     fn get_bytes(&mut self, range: Range<usize>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
//         todo!()
//     }

//     fn get_byte_ranges(
//         &mut self,
//         ranges: Vec<Range<usize>>,
//     ) -> BoxFuture<'_, parquet::errors::Result<Vec<Bytes>>> {
//         todo!()
//     }

//     fn get_metadata(&mut self) -> BoxFuture<'_, parquet::errors::Result<Arc<ParquetMetaData>>> {
//         Box::pin(async move {
//             let file_size = self.meta.size;
//             let metadata = ParquetMetaDataReader::new()
//                 .with_column_indexes(self.preload_column_index)
//                 .with_offset_indexes(self.preload_offset_index)
//                 .with_prefetch_hint(self.metadata_size_hint)
//                 .load_and_finish(self, file_size)
//                 .await?;
//             Ok(Arc::new(metadata))
//         })
//     }
// }

#[derive(Clone)]
pub(crate) enum AsyncReader {
    // Python(PyObspecReader),
    ObjectStore(ParquetObjectReader),
}

impl parquet::arrow::async_reader::AsyncFileReader for AsyncReader {
    fn get_bytes(&mut self, range: Range<usize>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        match self {
            // Self::Python(reader) => reader.get_bytes(range),
            Self::ObjectStore(reader) => reader.get_bytes(range),
        }
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<usize>>,
    ) -> BoxFuture<'_, parquet::errors::Result<Vec<Bytes>>> {
        match self {
            // Self::Python(reader) => reader.get_byte_ranges(ranges),
            Self::ObjectStore(reader) => reader.get_byte_ranges(ranges),
        }
    }

    fn get_metadata(&mut self) -> BoxFuture<'_, parquet::errors::Result<Arc<ParquetMetaData>>> {
        match self {
            // Self::Python(reader) => reader.get_metadata(),
            Self::ObjectStore(reader) => reader.get_metadata(),
        }
    }
}
