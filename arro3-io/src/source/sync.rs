use bytes::Bytes;
use futures::future::BoxFuture;
use futures::FutureExt;
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};
use parquet::file::reader::{ChunkReader, Length};
use pyo3::exceptions::PyTypeError;
use pyo3::intern;
use pyo3_file::PyFileLikeObject;

use pyo3::prelude::*;
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::Arc;

/// Represents either a path `File` or a file-like object `FileLike`
#[derive(Debug)]
pub enum SyncReader {
    File(File),
    FileLike(PyFileLikeObject),
}

impl SyncReader {
    pub(crate) fn try_clone(&self) -> std::io::Result<Self> {
        match self {
            Self::File(f) => Ok(Self::File(f.try_clone()?)),
            Self::FileLike(f) => Ok(Self::FileLike(f.clone())),
        }
    }
}

impl<'py> FromPyObject<'py> for SyncReader {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let py = ob.py();
        if let Ok(path) = ob.extract::<PathBuf>() {
            Ok(Self::File(File::open(path)?))
        } else if let Ok(path) = ob.extract::<String>() {
            Ok(Self::File(File::open(path)?))
        } else if ob.hasattr(intern!(py, "read"))? && ob.hasattr(intern!(py, "seek"))? {
            Ok(Self::FileLike(PyFileLikeObject::with_requirements(
                ob.clone().unbind(),
                true,
                false,
                true,
                false,
            )?))
        } else {
            Err(PyTypeError::new_err(
                "Expected a file path or a file-like object",
            ))
        }
    }
}

impl Read for SyncReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Self::File(f) => f.read(buf),
            Self::FileLike(f) => f.read(buf),
        }
    }
}

impl Seek for SyncReader {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        match self {
            Self::File(f) => f.seek(pos),
            Self::FileLike(f) => f.seek(pos),
        }
    }
}

impl Length for SyncReader {
    fn len(&self) -> u64 {
        match self {
            Self::File(f) => f.len(),
            Self::FileLike(f) => {
                let mut file = f.clone();
                // Keep track of current pos
                let pos = file.stream_position().unwrap();

                // Seek to end of file
                file.seek(std::io::SeekFrom::End(0)).unwrap();
                let len = file.stream_position().unwrap();

                // Seek back
                file.seek(std::io::SeekFrom::Start(pos)).unwrap();
                len
            }
        }
    }
}

impl ChunkReader for SyncReader {
    type T = BufReader<SyncReader>;

    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        let mut reader = self.try_clone()?;
        reader.seek(SeekFrom::Start(start))?;
        Ok(BufReader::new(self.try_clone()?))
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<Bytes> {
        let mut buffer = Vec::with_capacity(length);
        let mut reader = self.try_clone()?;
        reader.seek(SeekFrom::Start(start))?;
        let read = reader.take(length as _).read_to_end(&mut buffer)?;

        if read != length {
            return Err(parquet::errors::ParquetError::EOF(format!(
                "Expected to read {} bytes, read only {}",
                length, read,
            )));
        }
        Ok(buffer.into())
    }
}

// This impl allows us to use SyncReader in `ParquetFile::read_async`
impl AsyncFileReader for SyncReader {
    fn get_bytes(
        &mut self,
        range: std::ops::Range<usize>,
    ) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        async move { ChunkReader::get_bytes(self, range.start as u64, range.end - range.start) }
            .boxed()
    }

    fn get_metadata(&mut self) -> BoxFuture<'_, parquet::errors::Result<Arc<ParquetMetaData>>> {
        async move {
            let metadata = ParquetMetaDataReader::new()
                .with_column_indexes(true)
                .with_offset_indexes(true)
                .parse_and_finish(self)?;
            Ok(Arc::new(metadata))
        }
        .boxed()
    }
}

/// Represents either a path `File` or a file-like object `FileLike`
#[derive(Debug)]
pub enum FileWriter {
    File(File),
    FileLike(PyFileLikeObject),
}

impl<'py> FromPyObject<'py> for FileWriter {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        if let Ok(path) = ob.extract::<PathBuf>() {
            Ok(Self::File(File::create(path)?))
        } else if let Ok(path) = ob.extract::<String>() {
            Ok(Self::File(File::create(path)?))
        } else {
            Ok(Self::FileLike(PyFileLikeObject::with_requirements(
                ob.clone().unbind(),
                false,
                true,
                true,
                false,
            )?))
        }
    }
}

impl Write for FileWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Self::File(f) => f.write(buf),
            Self::FileLike(f) => f.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Self::File(f) => f.flush(),
            Self::FileLike(f) => f.flush(),
        }
    }
}

impl Seek for FileWriter {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        match self {
            Self::File(f) => f.seek(pos),
            Self::FileLike(f) => f.seek(pos),
        }
    }
}
