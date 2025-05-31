use std::sync::Arc;

use parquet::arrow::async_reader::ParquetObjectReader;
use pyo3::prelude::*;
use pyo3_arrow::error::PyArrowResult;
use pyo3_arrow::PyTable;
use pyo3_object_store::PyObjectStore;

use crate::error::Arro3IoResult;

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
