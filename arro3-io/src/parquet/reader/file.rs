use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::ArrowError;
use futures::StreamExt;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ParquetRecordBatchReaderBuilder};
use parquet::arrow::async_reader::{AsyncFileReader, ParquetRecordBatchStream};
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use pyo3::prelude::*;
use pyo3_arrow::export::{Arro3RecordBatchReader, Arro3Schema};
use pyo3_arrow::PyRecordBatchReader;

use crate::error::Arro3IoResult;
use crate::parquet::reader::options::PyParquetOptions;
use crate::parquet::reader::stream::PyRecordBatchStream;
use crate::runtime::get_runtime;
use crate::source::{AsyncReader, SyncReader};

enum ParquetSource {
    Sync(SyncReader),
    Async(AsyncReader),
}

/// Reader interface for a single Parquet file.
#[pyclass(module = "arro3.io", frozen)]
pub struct ParquetFile {
    parquet_meta: ArrowReaderMetadata,
    source: ParquetSource,
}

#[pymethods]
impl ParquetFile {
    #[getter]
    fn num_row_groups(&self) -> usize {
        self.parquet_meta.metadata().num_row_groups()
    }

    fn read(&self, kwargs: Option<PyParquetOptions>) -> Arro3IoResult<Arro3RecordBatchReader> {
        let options = kwargs.unwrap_or_default();
        match &self.source {
            ParquetSource::Sync(sync_source) => {
                let reader_builder = ParquetRecordBatchReaderBuilder::new_with_metadata(
                    sync_source.try_clone()?,
                    self.parquet_meta.clone(),
                );
                let record_batch_reader = options
                    .apply_to_reader_builder(reader_builder, &self.parquet_meta)
                    .build()?;
                Ok(PyRecordBatchReader::new(Box::new(record_batch_reader)).into())
            }
            ParquetSource::Async(async_source) => {
                let reader_builder = ParquetRecordBatchStreamBuilder::new_with_metadata(
                    async_source.clone(),
                    self.parquet_meta.clone(),
                );
                let record_batch_stream = options
                    .apply_to_reader_builder(reader_builder, &self.parquet_meta)
                    .build()?;
                let blocking_record_batch_reader = BlockingAsyncParquetReader(record_batch_stream);
                Ok(PyRecordBatchReader::new(Box::new(blocking_record_batch_reader)).into())
            }
        }
    }

    fn read_async(&self, kwargs: Option<PyParquetOptions>) -> Arro3IoResult<PyRecordBatchStream> {
        let options = kwargs.unwrap_or_default();
        match &self.source {
            ParquetSource::Sync(sync_source) => {
                todo!("implement AsyncFileReader for SyncReader");
                // let reader_builder = ParquetRecordBatchReaderBuilder::new_with_metadata(
                //     sync_source.try_clone()?,
                //     self.parquet_meta.clone(),
                // );
                // let record_batch_reader = options
                //     .apply_to_reader_builder(reader_builder, &self.parquet_meta)
                //     .build()?;
                // Ok(PyRecordBatchReader::new(Box::new(record_batch_reader)).into())
            }
            ParquetSource::Async(async_source) => {
                let reader_builder = ParquetRecordBatchStreamBuilder::new_with_metadata(
                    Box::new(async_source.clone()) as _,
                    self.parquet_meta.clone(),
                );
                let record_batch_stream = options
                    .apply_to_reader_builder(reader_builder, &self.parquet_meta)
                    .build()?;
                Ok(PyRecordBatchStream::new(record_batch_stream))
            }
        }
    }

    #[getter]
    fn schema_arrow(&self) -> Arro3Schema {
        self.parquet_meta.schema().clone().into()
    }
}

struct BlockingAsyncParquetReader<T: AsyncFileReader + Unpin + Send + 'static>(
    ParquetRecordBatchStream<T>,
);

impl<T: AsyncFileReader + Unpin + Send + 'static> Iterator for BlockingAsyncParquetReader<T> {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        Python::with_gil(|py| {
            let runtime = get_runtime(py).unwrap();
            runtime
                .block_on(self.0.next())
                .map(|maybe_batch| maybe_batch.map_err(|err| err.into()))
        })
    }
}

impl<T: AsyncFileReader + Unpin + Send + 'static> RecordBatchReader
    for BlockingAsyncParquetReader<T>
{
    fn schema(&self) -> arrow_schema::SchemaRef {
        self.0.schema().clone()
    }
}
