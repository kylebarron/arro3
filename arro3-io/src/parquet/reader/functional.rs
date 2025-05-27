use std::sync::Arc;

use arrow_array::{RecordBatchIterator, RecordBatchReader};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::async_reader::ParquetObjectReader;
use pyo3::prelude::*;
use pyo3_arrow::error::PyArrowResult;
use pyo3_arrow::export::Arro3RecordBatchReader;
use pyo3_arrow::{PyRecordBatchReader, PyTable};
use pyo3_object_store::PyObjectStore;

use crate::error::Arro3IoResult;
use crate::source::SyncReader;

#[pyfunction]
pub fn read_parquet(file: SyncReader) -> PyArrowResult<Arro3RecordBatchReader> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();

    let metadata = builder.schema().metadata().clone();
    let reader = builder.build().unwrap();

    // Add source schema metadata onto reader's schema. The original schema is not valid
    // with a given column projection, but we want to persist the source's metadata.
    let arrow_schema = Arc::new(reader.schema().as_ref().clone().with_metadata(metadata));

    // Create a new iterator with the arrow schema specifically
    //
    // Passing ParquetRecordBatchReader directly to PyRecordBatchReader::new loses schema
    // metadata
    //
    // https://docs.rs/parquet/latest/parquet/arrow/arrow_reader/struct.ParquetRecordBatchReader.html#method.schema
    // https://github.com/apache/arrow-rs/pull/5135
    let iter = Box::new(RecordBatchIterator::new(reader, arrow_schema));
    Ok(PyRecordBatchReader::new(iter).into())
}

#[pyfunction]
#[pyo3(signature = (path, *, store))]
pub fn read_parquet_async(
    py: Python,
    path: String,
    store: PyObjectStore,
) -> PyArrowResult<PyObject> {
    let fut = pyo3_async_runtimes::tokio::future_into_py(py, async move {
        Ok(read_parquet_async_inner(store.into_inner(), path).await?)
    })?;

    Ok(fut.into())
}

async fn read_parquet_async_inner(
    store: Arc<dyn object_store::ObjectStore>,
    path: String,
) -> Arro3IoResult<PyTable> {
    use futures::TryStreamExt;
    use parquet::arrow::ParquetRecordBatchStreamBuilder;

    let object_reader = ParquetObjectReader::new(store, path.into());
    let builder = ParquetRecordBatchStreamBuilder::new(object_reader).await?;

    let metadata = builder.schema().metadata().clone();
    let reader = builder.build()?;

    let arrow_schema = Arc::new(reader.schema().as_ref().clone().with_metadata(metadata));

    let batches = reader.try_collect::<Vec<_>>().await?;
    Ok(PyTable::try_new(batches, arrow_schema)?)
}
