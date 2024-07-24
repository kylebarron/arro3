use std::io::BufReader;

use arrow_csv::reader::Format;
use arrow_csv::{ReaderBuilder, WriterBuilder};
use pyo3::prelude::*;
use pyo3_arrow::error::PyArrowResult;
use pyo3_arrow::{PyRecordBatchReader, PySchema};

use crate::utils::{FileReader, FileWriter};

/// Infer a CSV file's schema
#[pyfunction]
#[pyo3(signature = (
    file,
    *,
    has_header=None,
    max_records=None,
    delimiter=None,
    escape=None,
    quote=None,
    terminator=None,
    comment=None,
))]
#[allow(clippy::too_many_arguments)]
pub fn infer_csv_schema(
    py: Python,
    file: FileReader,
    has_header: Option<bool>,
    max_records: Option<usize>,
    delimiter: Option<char>,
    escape: Option<char>,
    quote: Option<char>,
    terminator: Option<char>,
    comment: Option<char>,
) -> PyArrowResult<PyObject> {
    let mut format = Format::default();

    if let Some(has_header) = has_header {
        format = format.with_header(has_header);
    }
    if let Some(delimiter) = delimiter {
        format = format.with_delimiter(delimiter as u8);
    }
    if let Some(escape) = escape {
        format = format.with_escape(escape as u8);
    }
    if let Some(quote) = quote {
        format = format.with_quote(quote as u8);
    }
    if let Some(terminator) = terminator {
        format = format.with_terminator(terminator as u8);
    }
    if let Some(comment) = comment {
        format = format.with_comment(comment as u8);
    }

    let buf_file = BufReader::new(file);
    let (schema, _records_read) = format.infer_schema(buf_file, max_records)?;
    Ok(PySchema::new(schema.into()).to_arro3(py)?)
}

/// Read a CSV file to an Arrow RecordBatchReader
#[pyfunction]
#[pyo3(signature = (
    file,
    schema,
    *,
    has_header=None,
    batch_size=None,
    delimiter=None,
    escape=None,
    quote=None,
    terminator=None,
    comment=None,
))]
#[allow(clippy::too_many_arguments)]
pub fn read_csv(
    py: Python,
    file: FileReader,
    schema: PySchema,
    has_header: Option<bool>,
    batch_size: Option<usize>,
    delimiter: Option<char>,
    escape: Option<char>,
    quote: Option<char>,
    terminator: Option<char>,
    comment: Option<char>,
) -> PyArrowResult<PyObject> {
    let mut builder = ReaderBuilder::new(schema.into());

    if let Some(has_header) = has_header {
        builder = builder.with_header(has_header);
    }
    if let Some(batch_size) = batch_size {
        builder = builder.with_batch_size(batch_size);
    }
    if let Some(delimiter) = delimiter {
        builder = builder.with_delimiter(delimiter as u8);
    }
    if let Some(escape) = escape {
        builder = builder.with_escape(escape as u8);
    }
    if let Some(quote) = quote {
        builder = builder.with_quote(quote as u8);
    }
    if let Some(terminator) = terminator {
        builder = builder.with_terminator(terminator as u8);
    }
    if let Some(comment) = comment {
        builder = builder.with_comment(comment as u8);
    }

    let reader = builder.build(file)?;
    Ok(PyRecordBatchReader::new(Box::new(reader)).to_arro3(py)?)
}

/// Write an Arrow Table or stream to a CSV file
#[pyfunction]
#[pyo3(signature = (
    data,
    file,
    *,
    header=None,
    delimiter=None,
    escape=None,
    quote=None,
    date_format=None,
    datetime_format=None,
    time_format=None,
    timestamp_format=None,
    timestamp_tz_format=None,
    null=None,
))]
#[allow(clippy::too_many_arguments)]
pub fn write_csv(
    data: PyRecordBatchReader,
    file: FileWriter,
    header: Option<bool>,
    delimiter: Option<char>,
    escape: Option<char>,
    quote: Option<char>,
    date_format: Option<String>,
    datetime_format: Option<String>,
    time_format: Option<String>,
    timestamp_format: Option<String>,
    timestamp_tz_format: Option<String>,
    null: Option<String>,
) -> PyArrowResult<()> {
    let mut builder = WriterBuilder::new();

    if let Some(header) = header {
        builder = builder.with_header(header);
    }
    if let Some(delimiter) = delimiter {
        builder = builder.with_delimiter(delimiter as u8);
    }
    if let Some(escape) = escape {
        builder = builder.with_escape(escape as u8);
    }
    if let Some(quote) = quote {
        builder = builder.with_quote(quote as u8);
    }
    if let Some(date_format) = date_format {
        builder = builder.with_date_format(date_format);
    }
    if let Some(datetime_format) = datetime_format {
        builder = builder.with_datetime_format(datetime_format);
    }
    if let Some(time_format) = time_format {
        builder = builder.with_time_format(time_format);
    }
    if let Some(timestamp_format) = timestamp_format {
        builder = builder.with_timestamp_format(timestamp_format);
    }
    if let Some(timestamp_tz_format) = timestamp_tz_format {
        builder = builder.with_timestamp_tz_format(timestamp_tz_format);
    }
    if let Some(null) = null {
        builder = builder.with_null(null);
    }

    let mut writer = builder.build(file);
    for batch in data.into_reader()? {
        writer.write(&batch?)?;
    }
    Ok(())
}
