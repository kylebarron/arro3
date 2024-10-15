use std::sync::Arc;

use object_store::ObjectStore;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use crate::http::PyHttpStore;
use crate::{PyAzureStore, PyGCSStore, PyLocalStore, PyMemoryStore, PyS3Store};

/// A wrapper around a Rust ObjectStore instance that allows any rust-native implementation of
/// ObjectStore.
// (In the future we'll have a separate AnyObjectStore that allows either an fsspec-based
// implementation or a rust-based implementation.)
pub struct PyObjectStore(Arc<dyn ObjectStore>);

impl<'py> FromPyObject<'py> for PyObjectStore {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        if let Ok(store) = ob.downcast::<PyS3Store>() {
            Ok(Self(store.borrow().as_ref().clone()))
        } else if let Ok(store) = ob.downcast::<PyAzureStore>() {
            Ok(Self(store.borrow().as_ref().clone()))
        } else if let Ok(store) = ob.downcast::<PyGCSStore>() {
            Ok(Self(store.borrow().as_ref().clone()))
        } else if let Ok(store) = ob.downcast::<PyHttpStore>() {
            Ok(Self(store.borrow().as_ref().clone()))
        } else if let Ok(store) = ob.downcast::<PyLocalStore>() {
            Ok(Self(store.borrow().as_ref().clone()))
        } else if let Ok(store) = ob.downcast::<PyMemoryStore>() {
            Ok(Self(store.borrow().as_ref().clone()))
        } else {
            // TODO: Check for fsspec
            Err(PyValueError::new_err(format!(
                "Expected an object store instance, got {}.\nAlso note that the object store instance must be exported by this same exact library. They cannot be used across libraries.",
                ob.repr()?
            )))
        }
    }
}

impl AsRef<Arc<dyn ObjectStore>> for PyObjectStore {
    fn as_ref(&self) -> &Arc<dyn ObjectStore> {
        &self.0
    }
}

impl PyObjectStore {
    pub fn into_inner(self) -> Arc<dyn ObjectStore> {
        self.0
    }
}
