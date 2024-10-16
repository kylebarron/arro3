use std::collections::HashMap;
use std::sync::Arc;

use futures::TryStreamExt;
use object_store::path::Path;
use object_store::ObjectStore;
use pyo3::prelude::*;
use pyo3_object_store::error::{PyObjectStoreError, PyObjectStoreResult};
use pyo3_object_store::PyObjectStore;

use crate::runtime::get_runtime;

pub(crate) struct PyObjectMeta(object_store::ObjectMeta);

impl IntoPy<PyObject> for PyObjectMeta {
    fn into_py(self, py: Python<'_>) -> PyObject {
        let mut dict = HashMap::<String, PyObject>::new();
        dict.insert("location".to_string(), self.0.location.as_ref().into_py(py));
        dict.insert(
            "last_modified".to_string(),
            self.0.last_modified.into_py(py),
        );
        dict.insert("size".to_string(), self.0.size.into_py(py));
        dict.insert("e_tag".to_string(), self.0.e_tag.into_py(py));
        dict.insert("version".to_string(), self.0.version.into_py(py));
        dict.into_py(py)
    }
}

#[pyfunction]
#[pyo3(signature = (store, prefix = None))]
pub(crate) fn list(
    py: Python,
    store: PyObjectStore,
    prefix: Option<String>,
) -> PyObjectStoreResult<Vec<PyObjectMeta>> {
    let runtime = get_runtime(py)?;
    let store = store.into_inner();
    let prefix: Option<Path> = prefix.map(|s| s.into());

    let list_result = py.allow_threads(|| {
        let out = runtime.block_on(list_materialize(store, prefix.as_ref()))?;
        Ok::<_, PyObjectStoreError>(out)
    })?;
    Ok(list_result)
}

#[pyfunction]
#[pyo3(signature = (store, prefix = None))]
pub(crate) fn list_async(
    py: Python,
    store: PyObjectStore,
    prefix: Option<String>,
) -> PyResult<PyObject> {
    let store = store.into_inner();
    let prefix: Option<Path> = prefix.map(|s| s.into());

    let fut = pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let out = list_materialize(store, prefix.as_ref()).await?;
        Ok(out)
    })?;
    Ok(fut.into())
}

async fn list_materialize(
    store: Arc<dyn ObjectStore>,
    prefix: Option<&Path>,
) -> PyObjectStoreResult<Vec<PyObjectMeta>> {
    let list_result = store.list(prefix).try_collect::<Vec<_>>().await?;
    let py_list_result = list_result.into_iter().map(PyObjectMeta).collect();
    Ok(py_list_result)
}
