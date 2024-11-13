use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use arrow_array::{RecordBatchIterator, RecordBatchReader};
use arrow_schema::SchemaRef;
use futures::StreamExt;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::arrow_writer::ArrowWriterOptions;
use parquet::arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStream};
use parquet::arrow::ArrowWriter;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::basic::{Compression, Encoding};
use parquet::file::properties::{WriterProperties, WriterVersion};
use parquet::format::KeyValue;
use parquet::schema::types::ColumnPath;
use pyo3::exceptions::{PyStopAsyncIteration, PyStopIteration, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3_arrow::error::PyArrowResult;
use pyo3_arrow::input::AnyRecordBatch;
use pyo3_arrow::{PyRecordBatch, PyRecordBatchReader, PyTable};
use pyo3_object_store::PyObjectStore;
use tokio::sync::Mutex;

use crate::error::{Arro3IoError, Arro3IoResult};
use crate::utils::{FileReader, FileWriter};

#[pyfunction]
pub fn read_parquet(py: Python, file: FileReader) -> PyArrowResult<PyObject> {
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
    Ok(PyRecordBatchReader::new(iter).to_arro3(py)?)
}

#[pyfunction]
#[pyo3(signature = (path, *, store))]
pub fn read_parquet_async(
    py: Python,
    path: String,
    store: PyObjectStore,
) -> PyResult<Bound<PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        Ok(read_parquet_async_inner(store.into_inner(), path).await?)
    })
}

struct PyRecordBatchWrapper(PyRecordBatch);

impl IntoPy<PyObject> for PyRecordBatchWrapper {
    fn into_py(self, py: Python<'_>) -> PyObject {
        self.0.to_arro3(py).unwrap()
    }
}

struct PyTableWrapper(PyTable);

impl IntoPy<PyObject> for PyTableWrapper {
    fn into_py(self, py: Python<'_>) -> PyObject {
        self.0.to_arro3(py).unwrap()
    }
}

#[pyclass(name = "ParquetRecordBatchStream")]
struct PyParquetRecordBatchStream {
    stream: Arc<Mutex<ParquetRecordBatchStream<ParquetObjectReader>>>,
    schema: SchemaRef,
}

#[pymethods]
impl PyParquetRecordBatchStream {
    fn __aiter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __anext__<'py>(&'py mut self, py: Python<'py>) -> PyResult<Bound<PyAny>> {
        let stream = self.stream.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, next_stream(stream, false))
    }

    fn collect_async<'py>(&'py self, py: Python<'py>) -> PyResult<Bound<PyAny>> {
        let stream = self.stream.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, collect_stream(stream, self.schema.clone()))
    }
}

async fn next_stream(
    stream: Arc<Mutex<ParquetRecordBatchStream<ParquetObjectReader>>>,
    sync: bool,
) -> PyResult<PyRecordBatchWrapper> {
    let mut stream = stream.lock().await;
    match stream.next().await {
        Some(Ok(batch)) => Ok(PyRecordBatchWrapper(PyRecordBatch::new(batch))),
        Some(Err(err)) => Err(Arro3IoError::ParquetError(err).into()),
        None => {
            // Depending on whether the iteration is sync or not, we raise either a
            // StopIteration or a StopAsyncIteration
            if sync {
                Err(PyStopIteration::new_err("stream exhausted"))
            } else {
                Err(PyStopAsyncIteration::new_err("stream exhausted"))
            }
        }
    }
}

async fn collect_stream(
    stream: Arc<Mutex<ParquetRecordBatchStream<ParquetObjectReader>>>,
    schema: SchemaRef,
) -> PyResult<PyTableWrapper> {
    let mut stream = stream.lock().await;
    let mut batches: Vec<_> = vec![];
    loop {
        match stream.next().await {
            Some(Ok(batch)) => {
                batches.push(batch);
            }
            Some(Err(err)) => return Err(Arro3IoError::ParquetError(err).into()),
            None => return Ok(PyTableWrapper(PyTable::try_new(batches, schema)?)),
        };
    }
}

async fn read_parquet_async_inner(
    store: Arc<dyn object_store::ObjectStore>,
    path: String,
) -> Arro3IoResult<PyParquetRecordBatchStream> {
    let meta = store.head(&path.into()).await?;

    let object_reader = ParquetObjectReader::new(store, meta);
    let builder = ParquetRecordBatchStreamBuilder::new(object_reader).await?;

    let metadata = builder.schema().metadata().clone();
    let reader = builder.build()?;

    let arrow_schema = Arc::new(reader.schema().as_ref().clone().with_metadata(metadata));

    Ok(PyParquetRecordBatchStream {
        stream: Arc::new(Mutex::new(reader)),
        schema: arrow_schema,
    })
}

pub(crate) struct PyWriterVersion(WriterVersion);

impl<'py> FromPyObject<'py> for PyWriterVersion {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let s: String = ob.extract()?;
        Ok(Self(
            WriterVersion::from_str(&s).map_err(|err| PyValueError::new_err(err.to_string()))?,
        ))
    }
}

pub(crate) struct PyCompression(Compression);

impl<'py> FromPyObject<'py> for PyCompression {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let s: String = ob.extract()?;
        Ok(Self(
            Compression::from_str(&s).map_err(|err| PyValueError::new_err(err.to_string()))?,
        ))
    }
}

#[derive(Debug)]
pub(crate) struct PyEncoding(Encoding);

impl<'py> FromPyObject<'py> for PyEncoding {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let s: String = ob.extract()?;
        Ok(Self(
            Encoding::from_str(&s).map_err(|err| PyValueError::new_err(err.to_string()))?,
        ))
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
#[allow(dead_code)]
pub(crate) struct PyColumnPath(ColumnPath);

impl<'py> FromPyObject<'py> for PyColumnPath {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        if let Ok(path) = ob.extract::<String>() {
            Ok(Self(path.into()))
        } else if let Ok(path) = ob.extract::<Vec<String>>() {
            Ok(Self(path.into()))
        } else {
            Err(PyTypeError::new_err(
                "Expected string or list of string input for column path.",
            ))
        }
    }
}

#[pyfunction]
#[pyo3(signature=(
    data,
    file,
    *,
    bloom_filter_enabled = None,
    bloom_filter_fpp = None,
    bloom_filter_ndv = None,
    column_compression = None,
    column_dictionary_enabled = None,
    column_encoding = None,
    column_max_statistics_size = None,
    compression = None,
    created_by = None,
    data_page_row_count_limit = None,
    data_page_size_limit = None,
    dictionary_enabled = None,
    dictionary_page_size_limit = None,
    encoding = None,
    key_value_metadata = None,
    max_row_group_size = None,
    max_statistics_size = None,
    skip_arrow_metadata = false,
    write_batch_size = None,
    writer_version = None,
))]
#[allow(clippy::too_many_arguments)]
pub(crate) fn write_parquet(
    data: AnyRecordBatch,
    file: FileWriter,
    bloom_filter_enabled: Option<bool>,
    bloom_filter_fpp: Option<f64>,
    bloom_filter_ndv: Option<u64>,
    column_compression: Option<HashMap<PyColumnPath, PyCompression>>,
    column_dictionary_enabled: Option<HashMap<PyColumnPath, bool>>,
    column_encoding: Option<HashMap<PyColumnPath, PyEncoding>>,
    column_max_statistics_size: Option<HashMap<PyColumnPath, usize>>,
    compression: Option<PyCompression>,
    created_by: Option<String>,
    data_page_row_count_limit: Option<usize>,
    data_page_size_limit: Option<usize>,
    dictionary_enabled: Option<bool>,
    dictionary_page_size_limit: Option<usize>,
    encoding: Option<PyEncoding>,
    key_value_metadata: Option<HashMap<String, String>>,
    max_row_group_size: Option<usize>,
    max_statistics_size: Option<usize>,
    skip_arrow_metadata: bool,
    write_batch_size: Option<usize>,
    writer_version: Option<PyWriterVersion>,
) -> PyArrowResult<()> {
    let mut props = WriterProperties::builder();

    if let Some(writer_version) = writer_version {
        props = props.set_writer_version(writer_version.0);
    }
    if let Some(data_page_size_limit) = data_page_size_limit {
        props = props.set_data_page_size_limit(data_page_size_limit);
    }
    if let Some(data_page_row_count_limit) = data_page_row_count_limit {
        props = props.set_data_page_row_count_limit(data_page_row_count_limit);
    }
    if let Some(dictionary_page_size_limit) = dictionary_page_size_limit {
        props = props.set_dictionary_page_size_limit(dictionary_page_size_limit);
    }
    if let Some(write_batch_size) = write_batch_size {
        props = props.set_write_batch_size(write_batch_size);
    }
    if let Some(max_row_group_size) = max_row_group_size {
        props = props.set_max_row_group_size(max_row_group_size);
    }
    if let Some(created_by) = created_by {
        props = props.set_created_by(created_by);
    }
    if let Some(key_value_metadata) = key_value_metadata {
        props = props.set_key_value_metadata(Some(
            key_value_metadata
                .into_iter()
                .map(|(k, v)| KeyValue::new(k, v))
                .collect(),
        ));
    }
    if let Some(compression) = compression {
        props = props.set_compression(compression.0);
    }
    if let Some(dictionary_enabled) = dictionary_enabled {
        props = props.set_dictionary_enabled(dictionary_enabled);
    }
    if let Some(max_statistics_size) = max_statistics_size {
        props = props.set_max_statistics_size(max_statistics_size);
    }
    if let Some(bloom_filter_enabled) = bloom_filter_enabled {
        props = props.set_bloom_filter_enabled(bloom_filter_enabled);
    }
    if let Some(bloom_filter_fpp) = bloom_filter_fpp {
        props = props.set_bloom_filter_fpp(bloom_filter_fpp);
    }
    if let Some(bloom_filter_ndv) = bloom_filter_ndv {
        props = props.set_bloom_filter_ndv(bloom_filter_ndv);
    }
    if let Some(encoding) = encoding {
        props = props.set_encoding(encoding.0);
    }
    if let Some(column_encoding) = column_encoding {
        for (column_path, encoding) in column_encoding.into_iter() {
            props = props.set_column_encoding(column_path.0, encoding.0);
        }
    }
    if let Some(column_compression) = column_compression {
        for (column_path, compression) in column_compression.into_iter() {
            props = props.set_column_compression(column_path.0, compression.0);
        }
    }
    if let Some(column_dictionary_enabled) = column_dictionary_enabled {
        for (column_path, dictionary_enabled) in column_dictionary_enabled.into_iter() {
            props = props.set_column_dictionary_enabled(column_path.0, dictionary_enabled);
        }
    }
    if let Some(column_max_statistics_size) = column_max_statistics_size {
        for (column_path, max_statistics_size) in column_max_statistics_size.into_iter() {
            props = props.set_column_max_statistics_size(column_path.0, max_statistics_size);
        }
    }

    let reader = data.into_reader()?;

    let writer_options = ArrowWriterOptions::new()
        .with_properties(props.build())
        .with_skip_arrow_metadata(skip_arrow_metadata);
    let mut writer =
        ArrowWriter::try_new_with_options(file, reader.schema(), writer_options).unwrap();
    for batch in reader {
        writer.write(&batch?).unwrap();
    }
    writer.close().unwrap();
    Ok(())
}
