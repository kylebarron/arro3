use std::io::{BufReader, BufWriter};

use arrow_ipc::reader::{FileReaderBuilder, StreamReader};
use pyo3::prelude::*;
use pyo3_arrow::error::PyArrowResult;
use pyo3_arrow::input::AnyRecordBatch;
use pyo3_arrow::PyRecordBatchReader;

use crate::utils::{FileReader, FileWriter};

/// Read an Arrow IPC file to an Arrow RecordBatchReader
#[pyfunction]
pub fn read_ipc(py: Python, file: FileReader) -> PyArrowResult<PyObject> {
    let builder = FileReaderBuilder::new();
    let buf_file = BufReader::new(file);
    let reader = builder.build(buf_file)?;
    Ok(PyRecordBatchReader::new(Box::new(reader)).to_arro3(py)?)
}

/// Read an Arrow IPC Stream file to an Arrow RecordBatchReader
#[pyfunction]
pub fn read_ipc_stream(py: Python, file: FileReader) -> PyArrowResult<PyObject> {
    let reader = StreamReader::try_new(file, None)?;
    Ok(PyRecordBatchReader::new(Box::new(reader)).to_arro3(py)?)
}

/// Write an Arrow Table or stream to an IPC File
#[pyfunction]
pub fn write_ipc(data: AnyRecordBatch, file: FileWriter) -> PyArrowResult<()> {
    let buf_writer = BufWriter::new(file);
    let reader = data.into_reader()?;
    let mut writer = arrow_ipc::writer::FileWriter::try_new(buf_writer, &reader.schema())?;
    for batch in reader {
        writer.write(&batch?)?;
    }
    Ok(())
}

/// Write an Arrow Table or stream to an IPC Stream
#[pyfunction]
pub fn write_ipc_stream(data: AnyRecordBatch, file: FileWriter) -> PyArrowResult<()> {
    let buf_writer = BufWriter::new(file);
    let reader = data.into_reader()?;
    let mut writer = arrow_ipc::writer::StreamWriter::try_new(buf_writer, &reader.schema())?;
    for batch in reader {
        writer.write(&batch?)?;
    }
    Ok(())
}
