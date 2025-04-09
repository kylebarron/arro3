use std::sync::Arc;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use pyo3::sync::GILOnceCell;
use rayon::{ThreadPool, ThreadPoolBuilder};

static DEFAULT_POOL: GILOnceCell<Arc<ThreadPool>> = GILOnceCell::new();

pub fn get_default_pool(py: Python<'_>) -> PyResult<Arc<ThreadPool>> {
    let runtime = DEFAULT_POOL.get_or_try_init(py, || {
        let pool = ThreadPoolBuilder::new().build().map_err(|err| {
            PyValueError::new_err(format!("Could not create rayon threadpool. {}", err))
        })?;
        Ok::<_, PyErr>(Arc::new(pool))
    })?;
    Ok(runtime.clone())
}
