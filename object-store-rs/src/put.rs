use std::fs::File;
use std::io::{BufReader, Cursor, Read};
use std::path::PathBuf;
use std::sync::Arc;

use object_store::path::Path;
use object_store::{ObjectStore, WriteMultipart};
use pyo3::exceptions::PyIOError;
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedBytes;
use pyo3_file::PyFileLikeObject;
use pyo3_object_store::error::PyObjectStoreResult;
use pyo3_object_store::PyObjectStore;

use crate::runtime::get_runtime;

/// Input types supported by multipart upload
#[derive(Debug)]
pub(crate) enum MultipartPutInput {
    File(BufReader<File>),
    FileLike(PyFileLikeObject),
    Buffer(Cursor<PyBackedBytes>),
}

impl<'py> FromPyObject<'py> for MultipartPutInput {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let py = ob.py();
        if let Ok(path) = ob.extract::<PathBuf>() {
            Ok(Self::File(BufReader::new(File::open(path)?)))
        } else if let Ok(buffer) = ob.extract::<PyBackedBytes>() {
            Ok(Self::Buffer(Cursor::new(buffer)))
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

impl Read for MultipartPutInput {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Self::File(f) => f.read(buf),
            Self::FileLike(f) => f.read(buf),
            Self::Buffer(f) => f.read(buf),
        }
    }
}

#[pyfunction]
#[pyo3(signature = (store, location, file, *, chunk_size = 5120, max_concurrency = 12))]
pub(crate) fn put_file(
    py: Python,
    store: PyObjectStore,
    location: String,
    file: MultipartPutInput,
    chunk_size: usize,
    max_concurrency: usize,
) -> PyObjectStoreResult<()> {
    let runtime = get_runtime(py)?;
    runtime.block_on(put_multipart_inner(
        store.into_inner(),
        &location.into(),
        file,
        chunk_size,
        max_concurrency,
    ))
}

#[pyfunction]
#[pyo3(signature = (store, location, file, *, chunk_size = 5120, max_concurrency = 12))]
pub(crate) fn put_file_async(
    py: Python,
    store: PyObjectStore,
    location: String,
    file: MultipartPutInput,
    chunk_size: usize,
    max_concurrency: usize,
) -> PyResult<Bound<PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        Ok(put_multipart_inner(
            store.into_inner(),
            &location.into(),
            file,
            chunk_size,
            max_concurrency,
        )
        .await?)
    })
}

async fn put_multipart_inner<R: Read>(
    store: Arc<dyn ObjectStore>,
    location: &Path,
    mut reader: R,
    chunk_size: usize,
    max_concurrency: usize,
) -> PyObjectStoreResult<()> {
    let upload = store.put_multipart(location).await?;
    let mut write = WriteMultipart::new(upload);
    let mut scratch_buffer = vec![0; chunk_size];
    loop {
        let read_size = reader
            .read(&mut scratch_buffer)
            .map_err(|err| PyIOError::new_err(err.to_string()))?;
        if read_size == 0 {
            break;
        } else {
            write.wait_for_capacity(max_concurrency).await?;
            write.write(&scratch_buffer[0..read_size]);
        }
    }
    write.finish().await?;
    Ok(())
}
