use pyo3_file::PyFileLikeObject;

use pyo3::prelude::*;
use std::fs::File;
use std::io::{Read, Seek};
use std::path::PathBuf;

/// Represents either a path `File` or a file-like object `FileLike`
// Note: this is currently duplicated between here and arro3-io
#[derive(Debug)]
pub enum FileReader {
    File(File),
    FileLike(PyFileLikeObject),
}

impl<'py> FromPyObject<'py> for FileReader {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let py = ob.py();
        if let Ok(path) = ob.extract::<PathBuf>() {
            Ok(Self::File(File::open(path)?))
        } else if let Ok(path) = ob.extract::<String>() {
            Ok(Self::File(File::open(path)?))
        } else {
            Ok(Self::FileLike(PyFileLikeObject::with_requirements(
                ob.into_py(py),
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
