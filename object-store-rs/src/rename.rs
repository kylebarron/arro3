use object_store::ObjectStore;
use pyo3::prelude::*;
use pyo3_object_store::error::{PyObjectStoreError, PyObjectStoreResult};
use pyo3_object_store::PyObjectStore;

use crate::runtime::get_runtime;

#[pyfunction]
pub(crate) fn rename(
    py: Python,
    store: PyObjectStore,
    from_: String,
    to: String,
) -> PyObjectStoreResult<()> {
    let runtime = get_runtime(py)?;
    py.allow_threads(|| {
        runtime.block_on(store.as_ref().rename(&from_.into(), &to.into()))?;
        Ok::<_, PyObjectStoreError>(())
    })
}

#[pyfunction]
pub(crate) fn rename_async(
    py: Python,
    store: PyObjectStore,
    from_: String,
    to: String,
) -> PyResult<Bound<PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        store
            .as_ref()
            .rename(&from_.into(), &to.into())
            .await
            .map_err(PyObjectStoreError::ObjectStoreError)?;
        Ok(())
    })
}

#[pyfunction]
pub(crate) fn rename_if_not_exists(
    py: Python,
    store: PyObjectStore,
    from_: String,
    to: String,
) -> PyObjectStoreResult<()> {
    let runtime = get_runtime(py)?;
    py.allow_threads(|| {
        runtime.block_on(
            store
                .as_ref()
                .rename_if_not_exists(&from_.into(), &to.into()),
        )?;
        Ok::<_, PyObjectStoreError>(())
    })
}

#[pyfunction]
pub(crate) fn rename_if_not_exists_async(
    py: Python,
    store: PyObjectStore,
    from_: String,
    to: String,
) -> PyResult<Bound<PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        store
            .as_ref()
            .rename_if_not_exists(&from_.into(), &to.into())
            .await
            .map_err(PyObjectStoreError::ObjectStoreError)?;
        Ok(())
    })
}
