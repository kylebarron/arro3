use bytes::Bytes;
use parquet::file::reader::{ChunkReader, Length};
use pyo3_file::PyFileLikeObject;

use pyo3::prelude::*;
use pyo3_bytes::PyBytes;
use std::fs::File;
use std::io::{BufReader, Cursor, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

/// Represents a path `File`, a file-like object `FileLike`, or an in-memory `Buffer`
#[derive(Debug)]
pub enum FileReader {
    File(File),
    FileLike(PyFileLikeObject),
    /// Any Python object supporting the buffer protocol, referenced without copying.
    Buffer(Cursor<Bytes>),
}

impl FileReader {
    fn try_clone(&self) -> std::io::Result<Self> {
        match self {
            Self::File(f) => Ok(Self::File(f.try_clone()?)),
            Self::FileLike(f) => Ok(Self::FileLike(f.clone())),
            // `Bytes` is reference counted, so this does not copy the data.
            Self::Buffer(c) => Ok(Self::Buffer(Cursor::new(c.get_ref().clone()))),
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
        } else if let Ok(buffer) = obj.extract::<PyBytes>() {
            Ok(Self::Buffer(Cursor::new(buffer.into_inner())))
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
            Self::Buffer(c) => c.read(buf),
        }
    }
}

impl Seek for FileReader {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        match self {
            Self::File(f) => f.seek(pos),
            Self::FileLike(f) => f.seek(pos),
            Self::Buffer(c) => c.seek(pos),
        }
    }
}

impl Length for FileReader {
    fn len(&self) -> u64 {
        match self {
            Self::File(f) => f.len(),
            Self::Buffer(c) => c.get_ref().len() as u64,
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
        Ok(BufReader::new(reader))
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<Bytes> {
        if let Self::Buffer(c) = self {
            // Slice the underlying buffer directly instead of copying through a reader.
            let data = c.get_ref();
            let start = usize::try_from(start)
                .map_err(|_| parquet::errors::ParquetError::EOF("Offset out of range".into()))?;
            let end = start
                .checked_add(length)
                .filter(|end| *end <= data.len())
                .ok_or_else(|| {
                    parquet::errors::ParquetError::EOF(format!(
                        "Expected to read {length} bytes at offset {start}, but buffer length is {}",
                        data.len()
                    ))
                })?;
            return Ok(data.slice(start..end));
        }

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
