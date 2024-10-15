use std::sync::Arc;

use object_store::memory::InMemory;
use pyo3::prelude::*;

#[pyclass(name = "MemoryStore")]
pub struct PyMemoryStore(Arc<InMemory>);

impl AsRef<Arc<InMemory>> for PyMemoryStore {
    fn as_ref(&self) -> &Arc<InMemory> {
        &self.0
    }
}

impl PyMemoryStore {
    pub fn into_inner(self) -> Arc<InMemory> {
        self.0
    }
}

#[pymethods]
impl PyMemoryStore {
    #[new]
    fn py_new() -> Self {
        Self(Arc::new(InMemory::new()))
    }
}
