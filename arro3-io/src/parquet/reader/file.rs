use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::ArrowError;
use futures::StreamExt;
use object_store::ObjectStore;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder,
};
use parquet::arrow::async_reader::{
    AsyncFileReader, ParquetObjectReader, ParquetRecordBatchStream,
};
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedStr;
use pyo3::types::PyType;
use pyo3::IntoPyObjectExt;
use pyo3_arrow::export::{Arro3RecordBatchReader, Arro3Schema};
use pyo3_arrow::{PyRecordBatchReader, PySchema};
use pyo3_async_runtimes::tokio::future_into_py;
use pyo3_object_store::AnyObjectStore;

use crate::error::{Arro3IoError, Arro3IoResult};
use crate::parquet::reader::options::PyParquetOptions;
use crate::parquet::reader::stream::PyRecordBatchStream;
use crate::runtime::get_runtime;
use crate::source::{AsyncReader, SyncReader};

enum ParquetSource {
    Sync(SyncReader),
    Async(AsyncReader),
}

impl From<SyncReader> for ParquetSource {
    fn from(value: SyncReader) -> Self {
        Self::Sync(value)
    }
}

impl From<AsyncReader> for ParquetSource {
    fn from(value: AsyncReader) -> Self {
        Self::Async(value)
    }
}

impl SyncReader {
    fn open_parquet(&self, options: ArrowReaderOptions) -> Arro3IoResult<ArrowReaderMetadata> {
        Ok(ArrowReaderMetadata::load(self, options)?)
    }
}

impl AsyncReader {
    async fn open_parquet(
        &mut self,
        options: ArrowReaderOptions,
    ) -> Arro3IoResult<ArrowReaderMetadata> {
        Ok(ArrowReaderMetadata::load_async(self, options).await?)
    }
}

impl ParquetSource {
    async fn open_parquet(
        &mut self,
        options: ArrowReaderOptions,
    ) -> Arro3IoResult<ArrowReaderMetadata> {
        match self {
            ParquetSource::Sync(sync_source) => {
                Ok(ArrowReaderMetadata::load(sync_source, options)?)
            }
            ParquetSource::Async(async_source) => {
                Ok(ArrowReaderMetadata::load_async(async_source, options).await?)
            }
        }
    }
}

/// Reader interface for a single Parquet file.
#[pyclass(module = "arro3.io", frozen)]
pub struct ParquetFile {
    meta: ArrowReaderMetadata,
    source: ParquetSource,
}

#[pymethods]
impl ParquetFile {
    #[classmethod]
    #[pyo3(signature = (file, *, store=None, skip_arrow_metadata=false, schema=None, page_index=false))]
    pub(crate) fn open(
        _cls: &Bound<PyType>,
        py: Python,
        file: Bound<PyAny>,
        store: Option<AnyObjectStore>,
        skip_arrow_metadata: bool,
        schema: Option<PySchema>,
        page_index: bool,
    ) -> Arro3IoResult<Self> {
        let mut options = ArrowReaderOptions::default()
            .with_skip_arrow_metadata(skip_arrow_metadata)
            .with_page_index(page_index);
        if let Some(schema) = schema {
            options = options.with_schema(schema.into_inner());
        }

        let runtime = get_runtime(py)?;

        let mut source = if let Some(store) = store {
            let store = store.into_dyn();
            let path = object_store::path::Path::from(file.extract::<PyBackedStr>()?.as_ref());
            let meta = runtime.block_on(store.head(&path))?;
            let reader = ParquetObjectReader::new(store, meta);
            ParquetSource::Async(AsyncReader::ObjectStore(reader))
        } else {
            ParquetSource::Sync(file.extract()?)
        };

        let meta = runtime.block_on(source.open_parquet(options))?;
        Ok(Self { meta, source })
    }

    #[classmethod]
    #[pyo3(signature = (file, *, store=None, skip_arrow_metadata=false, schema=None, page_index=false))]
    pub(crate) fn open_async<'py>(
        _cls: &Bound<PyType>,
        py: Python<'py>,
        file: Bound<'py, PyAny>,
        store: Option<AnyObjectStore>,
        skip_arrow_metadata: bool,
        schema: Option<PySchema>,
        page_index: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let mut options = ArrowReaderOptions::default()
            .with_skip_arrow_metadata(skip_arrow_metadata)
            .with_page_index(page_index);
        if let Some(schema) = schema {
            options = options.with_schema(schema.into_inner());
        }

        if let Some(store) = store {
            let store = store.into_dyn();
            let path = object_store::path::Path::from(file.extract::<PyBackedStr>()?.as_ref());
            future_into_py(py, async move {
                let meta = store.head(&path).await.map_err(Arro3IoError::from)?;
                let mut reader = AsyncReader::ObjectStore(ParquetObjectReader::new(store, meta));
                let meta = reader.open_parquet(options).await?;
                Ok(Self {
                    meta,
                    source: reader.into(),
                })
            })
        } else {
            let reader = file.extract::<SyncReader>()?;
            let meta = reader.open_parquet(options)?;
            let slf = Self {
                meta,
                source: reader.into(),
            };
            slf.into_bound_py_any(py)
        }
    }

    #[getter]
    fn num_row_groups(&self) -> usize {
        self.meta.metadata().num_row_groups()
    }

    #[pyo3(signature = (**kwargs))]
    fn read(&self, kwargs: Option<PyParquetOptions>) -> Arro3IoResult<Arro3RecordBatchReader> {
        let options = kwargs.unwrap_or_default();
        match &self.source {
            ParquetSource::Sync(sync_source) => {
                let sync_reader_builder = ParquetRecordBatchReaderBuilder::new_with_metadata(
                    sync_source.try_clone()?,
                    self.meta.clone(),
                );
                let record_batch_reader = options
                    .apply_to_reader_builder(sync_reader_builder, &self.meta)
                    .build()?;
                Ok(PyRecordBatchReader::new(Box::new(record_batch_reader)).into())
            }
            ParquetSource::Async(async_source) => {
                let async_reader_builder = ParquetRecordBatchStreamBuilder::new_with_metadata(
                    async_source.clone(),
                    self.meta.clone(),
                );
                let record_batch_stream = options
                    .apply_to_reader_builder(async_reader_builder, &self.meta)
                    .build()?;
                let blocking_record_batch_reader = BlockingAsyncParquetReader(record_batch_stream);
                Ok(PyRecordBatchReader::new(Box::new(blocking_record_batch_reader)).into())
            }
        }
    }

    #[pyo3(signature = (**kwargs))]
    fn read_async(&self, kwargs: Option<PyParquetOptions>) -> Arro3IoResult<PyRecordBatchStream> {
        let options = kwargs.unwrap_or_default();
        match &self.source {
            ParquetSource::Sync(sync_source) => {
                let async_reader_builder = ParquetRecordBatchStreamBuilder::new_with_metadata(
                    Box::new(sync_source.try_clone()?) as _,
                    self.meta.clone(),
                );
                let record_batch_stream = options
                    .apply_to_reader_builder(async_reader_builder, &self.meta)
                    .build()?;
                Ok(PyRecordBatchStream::new(record_batch_stream))
            }
            ParquetSource::Async(async_source) => {
                let async_reader_builder = ParquetRecordBatchStreamBuilder::new_with_metadata(
                    Box::new(async_source.clone()) as _,
                    self.meta.clone(),
                );
                let record_batch_stream = options
                    .apply_to_reader_builder(async_reader_builder, &self.meta)
                    .build()?;
                Ok(PyRecordBatchStream::new(record_batch_stream))
            }
        }
    }

    #[getter]
    fn schema_arrow(&self) -> Arro3Schema {
        self.meta.schema().clone().into()
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
