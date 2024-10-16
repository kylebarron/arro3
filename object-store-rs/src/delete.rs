use pyo3::prelude::*;
use pyo3_object_store::error::{PyObjectStoreError, PyObjectStoreResult};
use pyo3_object_store::PyObjectStore;

use crate::runtime::get_runtime;

#[pyfunction]
pub fn delete(py: Python, store: PyObjectStore, location: String) -> PyObjectStoreResult<()> {
    let runtime = get_runtime(py)?;
    let store = store.into_inner();

    py.allow_threads(|| {
        runtime.block_on(store.delete(&location.into()))?;
        Ok::<_, object_store::Error>(())
    })?;
    Ok(())
}

#[pyfunction]
pub fn delete_async(py: Python, store: PyObjectStore, location: String) -> PyResult<PyObject> {
    let store = store.into_inner().clone();
    let fut = pyo3_async_runtimes::tokio::future_into_py(py, async move {
        store
            .delete(&location.into())
            .await
            .map_err(PyObjectStoreError::ObjectStoreError)?;
        Ok(())
    })?;
    Ok(fut.into())
}
