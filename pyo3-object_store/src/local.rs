use std::sync::Arc;

use object_store::local::LocalFileSystem;
use pyo3::prelude::*;

use crate::error::PyObjectStoreResult;

#[pyclass(name = "LocalStore")]
pub struct PyLocalStore(Arc<LocalFileSystem>);

impl AsRef<Arc<LocalFileSystem>> for PyLocalStore {
    fn as_ref(&self) -> &Arc<LocalFileSystem> {
        &self.0
    }
}

impl PyLocalStore {
    pub fn into_inner(self) -> Arc<LocalFileSystem> {
        self.0
    }
}

#[pymethods]
impl PyLocalStore {
    #[new]
    #[pyo3(signature = (prefix = None))]
    fn py_new(prefix: Option<std::path::PathBuf>) -> PyObjectStoreResult<Self> {
        let fs = if let Some(prefix) = prefix {
            LocalFileSystem::new_with_prefix(prefix)?
        } else {
            LocalFileSystem::new()
        };
        Ok(Self(Arc::new(fs)))
    }
}
