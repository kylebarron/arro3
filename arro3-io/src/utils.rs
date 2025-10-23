use bytes::Bytes;
use parquet::file::reader::{ChunkReader, Length};
use pyo3_file::PyFileLikeObject;

use pyo3::prelude::*;
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

/// Represents either a path `File` or a file-like object `FileLike`
#[derive(Debug)]
pub enum FileReader {
    File(File),
    FileLike(PyFileLikeObject),
}

impl FileReader {
    fn try_clone(&self) -> std::io::Result<Self> {
        match self {
            Self::File(f) => Ok(Self::File(f.try_clone()?)),
            Self::FileLike(f) => Ok(Self::FileLike(f.clone())),
        }
    }
}

impl<'py> FromPyObject<'_, 'py> for FileReader {
    type Error = PyErr;

    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
        if let Ok(path) = obj.extract::<PathBuf>() {
            Ok(Self::File(File::open(path)?))
        } else if let Ok(path) = obj.extract::<String>() {
            Ok(Self::File(File::open(path)?))
        } else {
            Ok(Self::FileLike(PyFileLikeObject::py_with_requirements(
                obj.as_any().clone(),
                true,
                false,
                true,
                false,
            )?))
        }
    }
}

impl Read for FileReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Self::File(f) => f.read(buf),
            Self::FileLike(f) => f.read(buf),
        }
    }
}

impl Seek for FileReader {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        match self {
            Self::File(f) => f.seek(pos),
            Self::FileLike(f) => f.seek(pos),
        }
    }
}

impl Length for FileReader {
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

impl ChunkReader for FileReader {
    type T = BufReader<FileReader>;

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
                "Expected to read {length} bytes, read only {read}"
            )));
        }
        Ok(buffer.into())
    }
}

/// Represents either a path `File` or a file-like object `FileLike`
#[derive(Debug)]
pub enum FileWriter {
    File(File),
    FileLike(PyFileLikeObject),
}

impl<'py> FromPyObject<'_, 'py> for FileWriter {
    type Error = PyErr;

    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
        if let Ok(path) = obj.extract::<PathBuf>() {
            Ok(Self::File(File::create(path)?))
        } else if let Ok(path) = obj.extract::<String>() {
            Ok(Self::File(File::create(path)?))
        } else {
            Ok(Self::FileLike(PyFileLikeObject::py_with_requirements(
                obj.as_any().clone(),
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
