use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use arrow_array::RecordBatchIterator;
use arrow_schema::SchemaRef;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::arrow_writer::ArrowWriterOptions;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, Encoding};
use parquet::file::metadata::ParquetMetaData;
use parquet::file::properties::{WriterProperties, WriterVersion};
use parquet::format::KeyValue;
use parquet::schema::types::ColumnPath;
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3_arrow::error::PyArrowResult;
use pyo3_arrow::input::AnyRecordBatch;
use pyo3_arrow::PyRecordBatchReader;

use crate::utils::{FileReader, FileWriter};

#[pyfunction]
pub fn read_parquet(py: Python, file: FileReader) -> PyArrowResult<PyObject> {
    match file {
        FileReader::File(f) => {
            let builder = ParquetRecordBatchReaderBuilder::try_new(f).unwrap();

            let arrow_schema = update_arrow_schema(builder.schema(), builder.metadata());

            let reader = builder.build().unwrap();
            // Create a new iterator with the new schema
            let iter = Box::new(RecordBatchIterator::new(reader, arrow_schema));
            Ok(PyRecordBatchReader::new(iter).to_arro3(py)?)
        }
        FileReader::FileLike(_) => {
            Err(PyTypeError::new_err("File objects not yet supported for reading parquet").into())
        }
    }
}

/// Update Arrow schema with Parquet key-value metadata
///
/// For (believed) parity with pyarrow, we only copy key-value metadata to the Arrow schema when no
/// Arrow schema is stored in the Parquet metadata (i.e. when the file wasn't written by an Arrow
/// writer).
fn update_arrow_schema(existing_schema: &SchemaRef, parquet_meta: &ParquetMetaData) -> SchemaRef {
    if let Some(kv_meta) = parquet_meta.file_metadata().key_value_metadata() {
        let has_arrow_schema_kv = kv_meta.iter().any(|kv| kv.key.as_str() == "ARROW:schema");
        // If the ARROW:schema key exists already, we do nothing
        if has_arrow_schema_kv {
            existing_schema.clone()
        } else {
            let mut metadata = existing_schema.metadata().clone();

            assert!(metadata.is_empty(), "If an Arrow schema is inferred from a Parquet schema, it should always have empty metadata, right?");
            for kv in kv_meta {
                if let Some(kv_value) = &kv.value {
                    metadata.insert(kv.key.clone(), kv_value.clone());
                }
            }

            Arc::new(existing_schema.as_ref().clone().with_metadata(metadata))
        }
    } else {
        existing_schema.clone()
    }
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

    let writer_options = ArrowWriterOptions::new().with_properties(props.build());
    let mut writer =
        ArrowWriter::try_new_with_options(file, reader.schema(), writer_options).unwrap();
    for batch in reader {
        writer.write(&batch?).unwrap();
    }
    writer.close().unwrap();
    Ok(())
}
