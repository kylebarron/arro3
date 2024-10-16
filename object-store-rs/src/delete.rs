use std::sync::Arc;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::sync::GILOnceCell;
use pyo3::types::PyBytes;
use pyo3_object_store::PyObjectStore;
use tokio::runtime::Runtime;

use crate::runtime::get_runtime;

#[pyfunction]
pub fn delete(py: Python, store: PyObjectStore, location: String) -> PyResult<()> {
    let runtime = get_runtime(py)?;
    let store = store.into_inner();

    py.allow_threads(|| runtime.block_on(store.delete(&location.into())).unwrap());

    Ok(())
}

#[pyfunction]
pub fn delete_async(py: Python, store: PyObjectStore, location: String) -> PyResult<PyObject> {
    let store = store.into_inner().clone();
    let fut = pyo3_async_runtimes::tokio::future_into_py(py, async move {
        store.delete(&location.into()).await.unwrap();
        Ok(())
        // sign_url_async_inner(store, method.0, path.into(), expires_in).await
    })?;
    Ok(fut.into())

    // let runtime = get_runtime(py)?;
    // let store = store.into_inner();

    // py.allow_threads(|| runtime.block_on(store.delete(&location.into())).unwrap());

    // Ok(())
}
