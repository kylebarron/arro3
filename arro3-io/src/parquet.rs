use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3_arrow::error::PyArrowResult;
use pyo3_arrow::input::AnyRecordBatch;
use pyo3_arrow::PyRecordBatchReader;

use crate::utils::{FileReader, FileWriter};

/// Read a Parquet file to an Arrow RecordBatchReader
#[pyfunction]
pub fn read_parquet(py: Python, file: FileReader) -> PyArrowResult<PyObject> {
    match file {
        FileReader::File(f) => {
            let builder = ParquetRecordBatchReaderBuilder::try_new(f).unwrap();

            let reader = builder.build().unwrap();
            Ok(PyRecordBatchReader::new(Box::new(reader)).to_arro3(py)?)
        }
        FileReader::FileLike(_) => {
            Err(PyTypeError::new_err("File objects not yet supported for reading parquet").into())
        }
    }
}

/// Write an Arrow Table or stream to a Parquet file
#[pyfunction]
pub fn write_parquet(data: AnyRecordBatch, file: FileWriter) -> PyArrowResult<()> {
    let reader = data.into_reader()?;
    let mut writer = ArrowWriter::try_new(file, reader.schema(), None).unwrap();
    for batch in reader {
        writer.write(&batch?).unwrap();
    }
    Ok(())
}
