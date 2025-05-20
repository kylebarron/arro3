use std::sync::Arc;

use arrow_schema::SchemaRef;
use futures::StreamExt;
use parquet::arrow::async_reader::{AsyncFileReader, ParquetRecordBatchStream};
use pyo3::exceptions::{PyStopAsyncIteration, PyStopIteration};
use pyo3::prelude::*;
use pyo3_arrow::export::{Arro3RecordBatch, Arro3Table};
use pyo3_arrow::PyTable;
use tokio::sync::Mutex;

use crate::error::Arro3IoError;

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
        let stream = self.stream.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, collect_stream(stream, self.schema.clone()))
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
) -> PyResult<Arro3Table> {
    let mut stream = stream.lock().await;
    let mut batches: Vec<_> = vec![];
    loop {
        match stream.next().await {
            Some(Ok(batch)) => {
                batches.push(batch);
            }
            Some(Err(err)) => return Err(Arro3IoError::ParquetError(err).into()),
            None => return Ok(PyTable::try_new(batches, schema)?.into()),
        };
    }
}
