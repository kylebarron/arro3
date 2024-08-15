use pyo3_file::PyFileLikeObject;

use pyo3::prelude::*;
use std::fs::File;
use std::io::{Read, Seek, Write};
use std::path::PathBuf;

/// Represents either a path `File` or a file-like object `FileLike`
#[derive(Debug)]
pub enum FileReader {
    File(File),
    FileLike(PyFileLikeObject),
}

impl<'py> FromPyObject<'py> for FileReader {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        if let Ok(path) = ob.extract::<PathBuf>() {
            Ok(Self::File(File::open(path)?))
        } else if let Ok(path) = ob.extract::<String>() {
            Ok(Self::File(File::open(path)?))
        } else {
            Ok(Self::FileLike(PyFileLikeObject::with_requirements(
                ob.as_gil_ref().into(),
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
                ob.as_gil_ref().into(),
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
