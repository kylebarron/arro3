use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::ArrowError;
use futures::StreamExt;
use parquet::arrow::arrow_reader::{
    ArrowReaderBuilder, ArrowReaderMetadata, ParquetRecordBatchReaderBuilder,
};
use parquet::arrow::async_reader::{AsyncFileReader, ParquetRecordBatchStream};
use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask};
use parquet::schema::types::SchemaDescriptor;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3_arrow::export::{Arro3RecordBatchReader, Arro3Schema};
use pyo3_arrow::PyRecordBatchReader;

use crate::error::Arro3IoResult;
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

#[derive(Debug, FromPyObject, Clone)]
struct PyProjectionMask(Vec<String>);

impl PyProjectionMask {
    fn resolve(&self, schema: &SchemaDescriptor) -> ProjectionMask {
        ProjectionMask::columns(schema, self.0.iter().map(|s| s.as_str()))
    }
}

#[derive(Debug, Default, Clone)]
struct PyParquetOptions {
    batch_size: Option<usize>,
    row_groups: Option<Vec<usize>>,
    columns: Option<PyProjectionMask>,
    // filter: Option<RowFilter>,
    // selection: Option<RowSelection>,
    limit: Option<usize>,
    offset: Option<usize>,
}

impl<'py> FromPyObject<'py> for PyParquetOptions {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let py = ob.py();

        let mut batch_size = None;
        let mut row_groups = None;
        let mut columns = None;
        let mut limit = None;
        let mut offset = None;
        if let Ok(val) = ob.get_item(intern!(py, "batch_size")) {
            batch_size = Some(val.extract()?);
        }
        if let Ok(val) = ob.get_item(intern!(py, "row_groups")) {
            row_groups = Some(val.extract()?);
        }
        if let Ok(val) = ob.get_item(intern!(py, "columns")) {
            columns = Some(val.extract()?);
        }
        if let Ok(val) = ob.get_item(intern!(py, "limit")) {
            limit = Some(val.extract()?);
        }
        if let Ok(val) = ob.get_item(intern!(py, "offset")) {
            offset = Some(val.extract()?);
        }

        Ok(Self {
            batch_size,
            row_groups,
            columns,
            limit,
            offset,
        })
    }
}

impl PyParquetOptions {
    fn apply_to_reader_builder<T>(
        self,
        mut builder: ArrowReaderBuilder<T>,
        metadata: &ArrowReaderMetadata,
    ) -> ArrowReaderBuilder<T> {
        if let Some(batch_size) = self.batch_size {
            builder = builder.with_batch_size(batch_size);
        }
        if let Some(row_groups) = self.row_groups {
            builder = builder.with_row_groups(row_groups);
        }
        if let Some(columns) = self.columns {
            builder = builder.with_projection(columns.resolve(metadata.parquet_schema()));
        }
        if let Some(limit) = self.limit {
            builder = builder.with_limit(limit);
        }
        if let Some(offset) = self.offset {
            builder = builder.with_offset(offset);
        }
        builder
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
