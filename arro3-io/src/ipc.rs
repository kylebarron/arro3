use std::io::{BufReader, BufWriter};

use arrow_ipc::reader::{FileReaderBuilder, StreamReader};
use arrow_ipc::writer::IpcWriteOptions;
use pyo3::exceptions::PyValueError;
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

#[allow(clippy::upper_case_acronyms)]
pub enum IpcCompression {
    LZ4,
    ZSTD,
}

impl<'py> FromPyObject<'py> for IpcCompression {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let s: String = ob.extract()?;
        match s.to_lowercase().as_str() {
            "lz4" | "lz4_frame" | "lz4frame" => Ok(Self::LZ4),
            "zstd" => Ok(Self::ZSTD),
            _ => Err(PyValueError::new_err(
                "Unexpected compression. Should be one of 'LZ4', 'ZSTD'.",
            )),
        }
    }
}

impl From<IpcCompression> for arrow_ipc::CompressionType {
    fn from(value: IpcCompression) -> Self {
        match value {
            IpcCompression::LZ4 => Self::LZ4_FRAME,
            IpcCompression::ZSTD => Self::ZSTD,
        }
    }
}

/// Write an Arrow Table or stream to an IPC File
#[pyfunction]
#[pyo3(
    signature = (data, file, *, compression = IpcCompression::LZ4),
    text_signature = "(data, file, *, compression = 'LZ4')")
]
pub fn write_ipc(
    data: AnyRecordBatch,
    file: FileWriter,
    compression: Option<IpcCompression>,
) -> PyArrowResult<()> {
    let buf_writer = BufWriter::new(file);
    let reader = data.into_reader()?;
    let options = IpcWriteOptions::default().try_with_compression(compression.map(|x| x.into()))?;
    let mut writer =
        arrow_ipc::writer::FileWriter::try_new_with_options(buf_writer, &reader.schema(), options)?;
    for batch in reader {
        writer.write(&batch?)?;
    }
    writer.finish()?;
    Ok(())
}

/// Write an Arrow Table or stream to an IPC Stream
#[pyfunction]
#[pyo3(
    signature = (data, file, *, compression = IpcCompression::LZ4),
    text_signature = "(data, file, *, compression = 'LZ4')")
]
pub fn write_ipc_stream(
    data: AnyRecordBatch,
    file: FileWriter,
    compression: Option<IpcCompression>,
) -> PyArrowResult<()> {
    let buf_writer = BufWriter::new(file);
    let reader = data.into_reader()?;
    let options = IpcWriteOptions::default().try_with_compression(compression.map(|x| x.into()))?;
    let mut writer = arrow_ipc::writer::StreamWriter::try_new_with_options(
        buf_writer,
        &reader.schema(),
        options,
    )?;
    for batch in reader {
        writer.write(&batch?)?;
    }
    writer.finish()?;
    Ok(())
}
