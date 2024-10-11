use std::sync::Arc;

use object_store::ObjectStore;
use pyo3::prelude::*;

use crate::http::PyHttpStore;
use crate::{PyAzureStore, PyGCSStore, PyS3Store};

pub struct AnyObjectStore(Arc<dyn ObjectStore>);

impl<'py> FromPyObject<'py> for AnyObjectStore {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        if let Ok(store) = ob.downcast::<PyS3Store>() {
            Ok(Self(store.borrow().as_ref().clone()))
        } else if let Ok(store) = ob.downcast::<PyAzureStore>() {
            Ok(Self(store.borrow().as_ref().clone()))
        } else if let Ok(store) = ob.downcast::<PyGCSStore>() {
            Ok(Self(store.borrow().as_ref().clone()))
        } else if let Ok(store) = ob.downcast::<PyHttpStore>() {
            Ok(Self(store.borrow().as_ref().clone()))
        } else {
            // Check for fsspec, else raise exception.
            // Also note in this exception that the store instances must have been created by _this
            // library_
            todo!()
        }
    }
}

impl AnyObjectStore {
    pub fn into_inner(self) -> Arc<dyn ObjectStore> {
        self.0
    }

    pub fn inner(&self) -> &Arc<dyn ObjectStore> {
        &self.0
    }
}
