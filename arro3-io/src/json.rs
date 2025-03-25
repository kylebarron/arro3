use std::io::BufReader;

use arrow::json::writer::{JsonArray, LineDelimited};
use arrow::json::{ReaderBuilder, WriterBuilder};
use pyo3::prelude::*;
use pyo3_arrow::error::PyArrowResult;
use pyo3_arrow::export::{Arro3RecordBatchReader, Arro3Schema};
use pyo3_arrow::input::AnyRecordBatch;
use pyo3_arrow::{PyRecordBatchReader, PySchema};

use crate::source::{FileWriter, SyncReader};

/// Infer a JSON file's schema
#[pyfunction]
#[pyo3(signature = (
    file,
    *,
    max_records=None,
))]
pub fn infer_json_schema(
    file: SyncReader,
    max_records: Option<usize>,
) -> PyArrowResult<Arro3Schema> {
    let buf_file = BufReader::new(file);
    let (schema, _records_read) = arrow::json::reader::infer_json_schema(buf_file, max_records)?;
    Ok(schema.into())
}

/// Read a JSON file to an Arrow RecordBatchReader
#[pyfunction]
#[pyo3(signature = (
    file,
    schema,
    *,
    batch_size=None,
))]
pub fn read_json(
    file: SyncReader,
    schema: PySchema,
    batch_size: Option<usize>,
) -> PyArrowResult<Arro3RecordBatchReader> {
    let mut builder = ReaderBuilder::new(schema.into());

    if let Some(batch_size) = batch_size {
        builder = builder.with_batch_size(batch_size);
    }

    let buf_file = BufReader::new(file);
    let reader = builder.build(buf_file)?;
    Ok(PyRecordBatchReader::new(Box::new(reader)).into())
}

/// Write an Arrow Table or stream to a JSON file
#[pyfunction]
#[pyo3(signature = (
    data,
    file,
    *,
    explicit_nulls=None,
))]
#[allow(clippy::too_many_arguments)]
pub fn write_json(
    data: AnyRecordBatch,
    file: FileWriter,
    explicit_nulls: Option<bool>,
) -> PyArrowResult<()> {
    let mut builder = WriterBuilder::new();

    if let Some(explicit_nulls) = explicit_nulls {
        builder = builder.with_explicit_nulls(explicit_nulls);
    }

    let mut writer = builder.build::<_, JsonArray>(file);
    for batch in data.into_reader()? {
        writer.write(&batch?)?;
    }
    Ok(())
}

/// Write an Arrow Table or stream to a newline-delimited JSON file
#[pyfunction]
#[pyo3(signature = (
    data,
    file,
    *,
    explicit_nulls=None,
))]
#[allow(clippy::too_many_arguments)]
pub fn write_ndjson(
    data: AnyRecordBatch,
    file: FileWriter,
    explicit_nulls: Option<bool>,
) -> PyArrowResult<()> {
    let mut builder = WriterBuilder::new();

    if let Some(explicit_nulls) = explicit_nulls {
        builder = builder.with_explicit_nulls(explicit_nulls);
    }

    let mut writer = builder.build::<_, LineDelimited>(file);
    for batch in data.into_reader()? {
        writer.write(&batch?)?;
    }
    writer.finish()?;
    Ok(())
}
