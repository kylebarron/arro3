use std::io::Read;
use std::sync::Arc;

use object_store::path::Path;
use object_store::{ObjectStore, WriteMultipart};
use pyo3::prelude::*;
use pyo3_object_store::error::PyObjectStoreResult;
use pyo3_object_store::PyObjectStore;

use crate::file::FileReader;
use crate::runtime::get_runtime;

#[pyfunction]
#[pyo3(signature = (store, location, file, *, max_concurrency = 12))]
pub fn put_file(
    py: Python,
    store: PyObjectStore,
    location: String,
    file: FileReader,
    max_concurrency: usize,
) -> PyObjectStoreResult<()> {
    let runtime = get_runtime(py)?;
    runtime.block_on(put_file_inner(
        store.into_inner(),
        &location.into(),
        file,
        max_concurrency,
    ))
}

#[pyfunction]
#[pyo3(signature = (store, location, file, *, max_concurrency = 12))]
pub fn put_file_async(
    py: Python,
    store: PyObjectStore,
    location: String,
    file: FileReader,
    max_concurrency: usize,
) -> PyResult<Bound<PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        Ok(put_file_inner(store.into_inner(), &location.into(), file, max_concurrency).await?)
    })
}

async fn put_file_inner<R: Read>(
    store: Arc<dyn ObjectStore>,
    location: &Path,
    mut reader: R,
    max_concurrency: usize,
) -> PyObjectStoreResult<()> {
    let upload = store.put_multipart(location).await?;
    let mut write = WriteMultipart::new(upload);
    let mut scratch_buffer = vec![0; 1024];
    loop {
        let read_size = reader.read(&mut scratch_buffer).unwrap();
        // dbg!(read_size);
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
