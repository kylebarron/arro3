use std::sync::Arc;

use arrow_schema::SchemaRef;
use futures::StreamExt;
use parquet::arrow::async_reader::{AsyncFileReader, ParquetRecordBatchStream};
use pyo3::exceptions::{PyStopAsyncIteration, PyStopIteration};
use pyo3::prelude::*;
use pyo3_arrow::export::{Arro3RecordBatch, Arro3Table};
use pyo3_arrow::PyTable;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use rayon::ThreadPool;
use tokio::sync::Mutex;

use crate::error::Arro3IoError;
use crate::parquet::reader::thread_pool::get_default_pool;

#[pyclass(name = "RecordBatchStream", frozen)]
pub(crate) struct PyRecordBatchStream {
    stream: Arc<Mutex<ParquetRecordBatchStream<Box<dyn AsyncFileReader + 'static>>>>,
    schema: SchemaRef,
}

impl PyRecordBatchStream {
    pub(crate) fn new(
        stream: ParquetRecordBatchStream<Box<dyn AsyncFileReader + 'static>>,
    ) -> Self {
        let schema = stream.schema().clone();
        Self {
            stream: Arc::new(Mutex::new(stream)),
            schema,
        }
    }
}

#[pymethods]
impl PyRecordBatchStream {
    fn __aiter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __anext__<'py>(&'py self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let stream = self.stream.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, next_stream(stream, false))
    }

    fn collect_async<'py>(&'py self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let pool = get_default_pool(py)?.clone();
        let stream = self.stream.clone();
        pyo3_async_runtimes::tokio::future_into_py(
            py,
            collect_stream(stream, self.schema.clone(), pool),
        )
    }
}

async fn next_stream(
    stream: Arc<Mutex<ParquetRecordBatchStream<Box<dyn AsyncFileReader + 'static>>>>,
    is_sync: bool,
) -> PyResult<Arro3RecordBatch> {
    let mut stream = stream.lock().await;
    match stream.next().await {
        Some(Ok(batch)) => Ok(batch.into()),
        Some(Err(err)) => Err(Arro3IoError::from(err).into()),
        None => {
            // Depending on whether the iteration is sync or not, we raise either a
            // StopIteration or a StopAsyncIteration
            if is_sync {
                Err(PyStopIteration::new_err("stream exhausted"))
            } else {
                Err(PyStopAsyncIteration::new_err("stream exhausted"))
            }
        }
    }
}

async fn collect_stream(
    stream: Arc<Mutex<ParquetRecordBatchStream<Box<dyn AsyncFileReader + 'static>>>>,
    schema: SchemaRef,
    pool: Arc<ThreadPool>,
) -> PyResult<Arro3Table> {
    let mut stream = stream.lock().await;

    let mut readers = vec![];
    while let Some(reader) = stream
        .next_row_group()
        .await
        .map_err(Arro3IoError::ParquetError)?
    {
        readers.push(reader);
    }

    let batches = pool.install(|| {
        let batches = readers
            .into_par_iter()
            .map(|r| r.collect::<Result<Vec<_>, _>>())
            .collect::<Result<Vec<_>, _>>()
            .map_err(Arro3IoError::ArrowError)?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        Ok::<_, PyErr>(batches)
    })?;

    Ok(PyTable::try_new(batches, schema)?.into())
}
