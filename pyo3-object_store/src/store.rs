use std::sync::Arc;

use object_store::ObjectStore;
use pyo3::prelude::*;

use crate::aws::S3Store;

pub struct AnyObjectStore(Arc<dyn ObjectStore>);

impl<'py> FromPyObject<'py> for AnyObjectStore {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        if let Ok(store) = ob.downcast::<S3Store>() {
            Ok(Self(store.borrow().inner().clone()))
        } else {
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
